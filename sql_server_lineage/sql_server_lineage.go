// Package that helps you create the data lineage from sql server stored procedures.
// It enables you to get the lineage and push it into a Data Catalog or
// visualise the lineage in an html file that you can share as static website.
//
// # Getting Started
//
// go get github.com/ivan-ostymchuk/sql-server-lineage
package sql_server_lineage

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"strings"
	"sync"
)

const tempTable = "TemporaryTable"
const normalTable = "Table"

type spLineage struct {
	sink     string
	sinkType string
	spName   string
	sources  []string
}

func lexer(sql io.Reader) ([]string, error) {

	query, err := io.ReadAll(sql)
	if err != nil {
		return []string{}, err
	}
	sqlSeparators := "\n,; ()\r"
	tokens := make([]string, 0)
	var token bytes.Buffer

	for i := 0; i < len(query); i++ {
		switch {
		case strings.Contains(sqlSeparators, string(query[i])) && token.Len() > 0:
			tokens = append(tokens, token.String())
			tokens = append(tokens, string(query[i]))
			token.Reset()
		case strings.Contains(sqlSeparators, string(query[i])):
			tokens = append(tokens, string(query[i]))
		default:
			token.WriteString(string(query[i]))
		}
	}

	return tokens, nil
}

func parser(
	wg *sync.WaitGroup,
	limiterCh chan struct{},
	resultsCh chan spLineage,
	sliceOfTokens []string,
) {

	defer wg.Done()

	replacer := strings.NewReplacer(
		"[", "",
		"]", "",
		"(", "",
		")", "",
		"\n", "",
		"\r", "",
		"\t", "",
		" ", "",
		";", "",
		",", "",
		"'", "",
	)
	sqlStatementsToSkip := []string{"delete"}
	tokensToSkip := []string{"\n", "\r", "\t", " "}
	lineageNormalTables := make(map[string]spLineage, 0)
	lineageTempTables := make(map[string]spLineage, 0)
	var spName string
	var currLineage spLineage

	for i := 0; i < len(sliceOfTokens)-1; i++ {
		token := strings.ToLower(sliceOfTokens[i])

		// skip deletes
		if slices.Contains(sqlStatementsToSkip, token) {
			elementsParsed := skipCodeBlockUntilContains(
				sliceOfTokens, i, []string{"select", "insert", "merge", ";"},
			)
			i += elementsParsed
			continue
		}
		// skip comments
		if strings.Contains(token, "--") ||
			strings.Contains(token, "/*") {
			nTokensToSkip := skipComments(sliceOfTokens, i)
			i += nTokensToSkip
			continue
		}
		// get stored procedure name
		if (strings.Contains(token, "create") ||
			strings.Contains(token, "alter")) &&
			len(spName) == 0 {
			name, err := getSpName(sliceOfTokens, i, tokensToSkip, replacer)
			if err != nil {
				fmt.Println("WARNING: ", err)
			}
			spName = name
			continue
		}
		// get sink tables
		if (strings.Contains(token, "into") ||
			strings.Contains(token, "update") ||
			strings.Contains(token, "merge")) &&
			i != len(sliceOfTokens)-1 {

			var formattedSinkTable string
			switch replacer.Replace(token) {
			case "update":
				formattedSinkTable = getSinkNameFromUpdate(sliceOfTokens, i, tokensToSkip, replacer)
			case "into":
				elementsParsed := skipCodeBlockUntilNotContains(sliceOfTokens, i, tokensToSkip)
				sinkTable := sliceOfTokens[i+elementsParsed]
				formattedSinkTable = replacer.Replace(sinkTable)
			case "merge":
				formattedSinkTable = getSinkNameFromMerge(sliceOfTokens, i, tokensToSkip, replacer)
			default:
				continue
			}

			// initialize new lineage
			var sinkType string
			switch {
			case strings.Contains(formattedSinkTable, "#") ||
				strings.Contains(formattedSinkTable, "@"):
				sinkType = tempTable
			default:
				sinkType = normalTable
			}
			currLineage = spLineage{
				sink:     formattedSinkTable,
				sinkType: sinkType,
				spName:   spName,
				sources:  []string{},
			}
			continue
		}
		// get all CTEs as tables
		if strings.Contains(token, "with") && i != len(sliceOfTokens)-1 {
			isCte, _ := isCte(sliceOfTokens, i, tokensToSkip, replacer)
			if isCte {
				cteLineages, nTokensParsed := getCtes(spName, sliceOfTokens, i, tokensToSkip, replacer)
				for _, lineage := range cteLineages {
					lineageTempTables[lineage.sink] = lineage
				}
				i += nTokensParsed
				continue
			}
		}
		// get sources
		if (strings.Contains(token, "from") ||
			strings.Contains(token, "join") ||
			strings.Contains(token, "using")) &&
			currLineage.sinkType != "" {
			if replacer.Replace(token) == "from" ||
				replacer.Replace(token) == "join" ||
				replacer.Replace(token) == "using" {
				sources, elementsParsed := getSources(replacer, sliceOfTokens, i, tokensToSkip)
				for _, source := range sources {
					if !slices.Contains(currLineage.sources, source) && source != currLineage.sink {
						currLineage.sources = append(currLineage.sources, source)
					}
				}
				if currLineage.sink != "" && currLineage.sinkType == normalTable {
					currLineageMerged := mergeLineage(currLineage, lineageNormalTables)
					lineageNormalTables[currLineage.sink] = currLineageMerged
				} else if currLineage.sink != "" && currLineage.sinkType == tempTable {
					currLineageMerged := mergeLineage(currLineage, lineageTempTables)
					lineageTempTables[currLineage.sink] = currLineageMerged
				}
				currLineage.sink = ""
				i += elementsParsed
				continue
			}
		}
	}

	// unnest temp tables and CTEs
	unnestedTempLineage := unnestTempTables(lineageTempTables)

	// replace temp tables and CTEs with the actual sources
	for _, lineage := range lineageNormalTables {
		unnestedNormalSources := replaceTempSourcesWithTables(unnestedTempLineage, lineage)
		lineage.sources = unnestedNormalSources
		resultsCh <- lineage
	}

	<-limiterCh // count -1 for the goroutine limiter
}

func aggregateLineageResults(resultsCh chan spLineage) map[string]map[string][]string {

	lineageMap := make(map[string]map[string][]string, 100)
	for lineage := range resultsCh {
		existingSink, ok := lineageMap[lineage.sink]
		if !ok {
			lineageMap[lineage.sink] = map[string][]string{
				lineage.spName: lineage.sources,
			}
			continue
		}
		existingSources, ok := existingSink[lineage.spName]
		if !ok {
			lineageMap[lineage.sink][lineage.spName] = lineage.sources
			continue
		} else {
			for _, source := range lineage.sources {
				if !slices.Contains(existingSources, source) && len(source) > 0 && source != lineage.sink {
					lineageMap[lineage.sink][lineage.spName] = append(
						lineageMap[lineage.sink][lineage.spName], source,
					)
				}
			}
		}
	}

	return lineageMap
}

func skipCodeBlockUntilContains(tokens []string, currentIndex int, stopTokens []string) int {
	var elementsParsed int
forLoop1:
	for el := 0; el < len(tokens[currentIndex:]); el++ {
		switch {
		case strings.Contains(tokens[el+currentIndex], "--"):
			nTokensToSkip := skipCodeBlockUntilContains(tokens, el+currentIndex+1, []string{"\n", "\r"})
			el += nTokensToSkip
		case strings.Contains(tokens[el+currentIndex], "/*"):
			nTokensToSkip := skipCodeBlockUntilContains(tokens, el+currentIndex+1, []string{"*/"})
			el += nTokensToSkip
		default:
			for _, stopToken := range stopTokens {
				if strings.Contains(tokens[el+currentIndex], stopToken) {
					elementsParsed = el
					break forLoop1
				}
			}
		}
	}
	return elementsParsed
}

func skipCodeBlockUntilNotContains(tokens []string, currentIndex int, stopTokens []string) int {
	var elementsParsed int
forLoop:
	for el := 1; el < len(tokens[currentIndex:]); el++ {
		token := tokens[el+currentIndex]
		switch {
		case strings.Contains(token, "--") || strings.Contains(token, "/*"):
			nTokensToSkip := skipComments(tokens, el+currentIndex)
			el += nTokensToSkip
		case !slices.Contains(stopTokens, token):
			elementsParsed = el
			break forLoop
		}
	}
	return elementsParsed
}

func mergeLineage(currLineage spLineage, destLineage map[string]spLineage) spLineage {
	existentLineage, ok := destLineage[currLineage.sink]
	if ok {
		for _, source := range existentLineage.sources {
			if !slices.Contains(currLineage.sources, source) && source != existentLineage.sink {
				currLineage.sources = append(currLineage.sources, source)
			}
		}
	}
	return currLineage
}

func replaceTempSourcesWithTables(
	tempLineages map[string]spLineage,
	normalSingleLineage spLineage,
) []string {

	unnestedSources := make([]string, 0)

	for _, source := range normalSingleLineage.sources {
		// check if one of the sources is a CTE or temp table
		tmp, ok := tempLineages[source]
		if ok && len(normalSingleLineage.sources) > 0 {
			// add CTE/temp original sources if not present
			for _, tmpSource := range tmp.sources {
				if !slices.Contains(normalSingleLineage.sources, tmpSource) &&
					tmpSource != normalSingleLineage.sink {
					unnestedSources = append(unnestedSources, tmpSource)
				}
			}
		} else if len(source) > 0 {
			unnestedSources = append(unnestedSources, source)
		}
	}
	return unnestedSources
}

func getSources(
	replacer *strings.Replacer,
	sliceOfTokens []string,
	startIndex int,
	tokensToSkip []string,
) ([]string, int) {

	stopTokens := []string{"into", "update", "delete", "merge"}
	functionNamesToSkip := []string{"openquery", "opendatasource", "openrowset"}
	var elementsProcessed int
	sources := make([]string, 0)

forLoop:
	for iso := startIndex; iso < len(sliceOfTokens)-2; iso++ {
		token := strings.ToLower(sliceOfTokens[iso])
		switch {
		case strings.Contains(token, "--") || strings.Contains(token, "/*"):
			nTokensToSkip := skipComments(sliceOfTokens, iso)
			iso += nTokensToSkip
		case strings.Contains(token, "from") ||
			strings.Contains(token, "join") ||
			strings.Contains(token, "using"):
			cleanToken := replacer.Replace(token)
			if cleanToken != "from" && cleanToken != "join" && cleanToken != "using" {
				continue
			}
			elementsParsed := skipCodeBlockUntilNotContains(sliceOfTokens, iso, tokensToSkip)
			sourceTable := sliceOfTokens[iso+elementsParsed]
			formattedSourceTable := replacer.Replace(sourceTable)
			if !slices.Contains(sources, formattedSourceTable) &&
				!slices.Contains(functionNamesToSkip, formattedSourceTable) {
				sources = append(sources, formattedSourceTable)
			}
			iso += elementsParsed
		case slices.Contains(stopTokens, replacer.Replace(token)) || strings.Contains(token, ";"):
			elementsProcessed = iso - startIndex - 1
			break forLoop
		case strings.Contains(token, "begin") && replacer.Replace(token) == "begin":
			elementsProcessed = iso - startIndex - 1
			break forLoop
		case strings.Contains(token, ")"):
			isCte, _ := isCte(sliceOfTokens, iso-1, tokensToSkip, replacer)
			if isCte {
				elementsProcessed = iso - startIndex - 1
				break forLoop
			}
		}
	}
	return sources, elementsProcessed
}

func getCtes(
	spName string,
	sliceOfTokens []string,
	startIndex int,
	tokensToSkip []string,
	replacer *strings.Replacer,
) (map[string]spLineage, int) {

	lineageCteTables := make(map[string]spLineage, 0)
	var nTokensParsed int

	for iso := startIndex; iso < len(sliceOfTokens)-1; iso++ {
		if strings.Contains(strings.ToLower(sliceOfTokens[iso]), "with") ||
			strings.Contains(sliceOfTokens[iso], ")") {
			var startGetSource int
			if strings.Contains(sliceOfTokens[iso], ")") {
				startGetSource = -1
			}
			isCte, sinkTable := isCte(sliceOfTokens, iso+startGetSource, tokensToSkip, replacer)

			if isCte {
				formattedSinkTable := replacer.Replace(sinkTable)
				sources, elementsProcessed := getSources(replacer, sliceOfTokens, iso+1, tokensToSkip)
				cteLineage := spLineage{
					sink:     formattedSinkTable,
					sinkType: tempTable,
					spName:   spName,
					sources:  sources,
				}
				lineageCteTables[cteLineage.sink] = cteLineage
				iso += elementsProcessed
			}
		}
		if strings.ToLower(sliceOfTokens[iso]) == "into" && iso != len(sliceOfTokens)-1 {
			nTokensParsed = iso - startIndex - 1
			break
		}
	}

	return lineageCteTables, nTokensParsed
}

func isCte(
	sliceOfTokens []string,
	startIndex int,
	tokensToSkip []string,
	replacer *strings.Replacer,
) (bool, string) {
	if strings.Contains(sliceOfTokens[startIndex], "with") &&
		replacer.Replace(sliceOfTokens[startIndex]) != "with" {
		return false, ""
	}
	cteElementsParsed := skipCodeBlockUntilContains(sliceOfTokens, startIndex, []string{"("})
	tokensToCheck := make([]string, 0)
	if startIndex == startIndex+cteElementsParsed {
		return false, ""
	}
	cteSlice := sliceOfTokens[startIndex+1 : startIndex+cteElementsParsed]
	// make sure that besides the elements to skip there is only one token (cte name) and "as"
	for index := 0; index < len(cteSlice); index++ {
		switch {
		case strings.Contains(cteSlice[index], "--") ||
			strings.Contains(cteSlice[index], "/*"):
			nTokensToSkip := skipComments(cteSlice, index)
			index += nTokensToSkip
		case !slices.Contains(tokensToSkip, cteSlice[index]):
			tokensToCheck = append(tokensToCheck, cteSlice[index])
		}
	}

	switch {
	case strings.Contains(sliceOfTokens[startIndex], "with") &&
		len(tokensToCheck) == 2 && slices.Contains(tokensToCheck, "as"):
		return true, tokensToCheck[0]
	case len(tokensToCheck) <= 3:
		return false, ""
	case len(tokensToCheck) == 4 && tokensToCheck[0] == ")" &&
		tokensToCheck[1] == "," && tokensToCheck[3] == "as":
		return true, tokensToCheck[2]
	default:
		return false, ""
	}
}

func skipComments(tokens []string, currentIndex int) int {
	var elementsParsed int
forLoop:
	for el := 0; el < len(tokens[currentIndex:]); el++ {
		switch {
		case strings.Contains(tokens[el+currentIndex], "--"):
			elementsToSkip := skipCodeBlockUntilContains(tokens, el+currentIndex, []string{"\n", "\r"})
			elementsParsed = el + elementsToSkip
			break forLoop
		case strings.Contains(tokens[el+currentIndex], "/*"):
			elementsToSkip := skipCodeBlockUntilContains(tokens, el+currentIndex, []string{"*/"})
			elementsParsed = el + elementsToSkip
			break forLoop
		}
	}
	return elementsParsed
}

func unnestTempTables(tempTablesLineage map[string]spLineage) map[string]spLineage {

	for _, lineage := range tempTablesLineage {
		for index := 0; index < len(lineage.sources); index++ {
			tmp, ok := tempTablesLineage[lineage.sources[index]]
			if ok {
				lineage.sources = slices.Delete(lineage.sources, index, index+1)
				lineage.sources = append(lineage.sources, tmp.sources...)
				if index == 0 {
					index--
				} else {
					index = 0
				}
				continue
			}
		}
		cleanedSources := make([]string, 0)
		for _, source := range lineage.sources {
			if len(source) > 0 && !slices.Contains(cleanedSources, source) {
				cleanedSources = append(cleanedSources, source)
			}
		}
		lineage.sources = cleanedSources
		tempTablesLineage[lineage.sink] = lineage
	}
	return tempTablesLineage
}

func getSinkNameFromMerge(
	sliceOfTokens []string,
	i int,
	tokensToSkip []string,
	replacer *strings.Replacer,
) string {
	tokensToSkipForMerge := []string{"\n", "\r", "\t", " ", "(", ")"}
	var sinkTableFromMerge string
	elementsParsed := skipCodeBlockUntilNotContains(sliceOfTokens, i, tokensToSkip)
	sinkTable := sliceOfTokens[i+elementsParsed]
	offset := i + elementsParsed
	if strings.Contains(sinkTable, "top") {
		// skip TOP function
		elementsParsed1 := skipCodeBlockUntilNotContains(sliceOfTokens, offset, tokensToSkipForMerge)
		// skip TOP function argument
		elementsParsed2 := skipCodeBlockUntilNotContains(sliceOfTokens, offset+elementsParsed1, tokensToSkipForMerge)
		sinkTable := sliceOfTokens[offset+elementsParsed1+elementsParsed2]
		sinkTableFromMerge = replacer.Replace(sinkTable)
	} else {
		sinkTableFromMerge = replacer.Replace(sinkTable)
	}
	return sinkTableFromMerge
}

func getSinkNameFromUpdate(
	sliceOfTokens []string,
	startIndex int,
	tokensToSkip []string,
	replacer *strings.Replacer,
) string {

	var tableToUpdate string
	stopTokens := []string{"where", "join", "into", ";"} // select
	var nestedFrom bool

forLoop:
	for i := startIndex; i < len(sliceOfTokens); i++ {
		token := strings.ToLower(sliceOfTokens[i])
		switch {
		case strings.Contains(token, "--") || strings.Contains(token, "/*"):
			nTokensToSkip := skipComments(sliceOfTokens, i)
			i += nTokensToSkip
		case tableToUpdate == "":
			elementsParsed := skipCodeBlockUntilNotContains(sliceOfTokens, i, tokensToSkip)
			table := sliceOfTokens[i+elementsParsed]
			tableToUpdate = replacer.Replace(table)
		case strings.Contains(token, "from") && replacer.Replace(token) == "from":
			elementsParsed := skipCodeBlockUntilNotContains(sliceOfTokens, i, tokensToSkip)
			sourceTable := sliceOfTokens[i+elementsParsed]
			if strings.Contains(sourceTable, "(") {
				nestedFrom = true
				continue
			}
			sourceTableFormatted := replacer.Replace(sourceTable)
			probableTables := getTableToUpdateWithAlias(sliceOfTokens, i, tokensToSkip, replacer)
			var aliasUpdated bool
			for index, m := range probableTables {
				for alias, t := range m {
					if alias == tableToUpdate {
						tableToUpdate = t
						aliasUpdated = true
					}
					if index+1 == len(probableTables) && !aliasUpdated {
						tableToUpdate = t
					}
				}
			}
			if tableToUpdate == "" || nestedFrom {
				tableToUpdate = sourceTableFormatted
			}
			break forLoop
		case slices.Contains(stopTokens, token):
			break forLoop
		}
	}

	return tableToUpdate
}

func getTableToUpdateWithAlias(
	sliceOfTokens []string,
	startIndex int,
	tokensToSkip []string,
	replacer *strings.Replacer,
) []map[string]string {
	updateElementsParsed := skipCodeBlockUntilContains(
		sliceOfTokens,
		startIndex,
		[]string{"delete", ";", "insert", "update"},
	)
	tokensToCheck := make([]string, 0)
	if updateElementsParsed == 0 {
		updateElementsParsed = len(sliceOfTokens) - startIndex
	}
	updateSlice := sliceOfTokens[startIndex : startIndex+updateElementsParsed]
	for index := 0; index < len(updateSlice); index++ {
		switch {
		case strings.Contains(updateSlice[index], "--") ||
			strings.Contains(updateSlice[index], "/*"):
			nTokensToSkip := skipComments(updateSlice, index)
			index += nTokensToSkip
		case !slices.Contains(tokensToSkip, updateSlice[index]):
			tokensToCheck = append(tokensToCheck, replacer.Replace(updateSlice[index]))
		}
	}

	elements := make([]map[string]string, 0)
	lt := len(tokensToCheck)
	for i, el := range tokensToCheck {
		elLower := strings.ToLower(el)
		if elLower == "from" && lt >= 4 && i+2 < lt && tokensToCheck[i+2] == "as" {
			elements = append(
				elements,
				map[string]string{tokensToCheck[i+3]: tokensToCheck[i+1]},
			)
		} else if elLower == "from" && i+2 < lt && lt >= 3 {
			if len(tokensToCheck[i+1]) > 0 {
				elements = append(
					elements,
					map[string]string{tokensToCheck[i+2]: tokensToCheck[i+1]},
				)
			}
		}
	}

	return elements
}

func getSpName(
	sliceOfTokens []string,
	startIndex int,
	tokensToSkip []string,
	replacer *strings.Replacer,
) (string, error) {
	elementsParsed := skipCodeBlockUntilContains(sliceOfTokens, startIndex, []string{"."})
	tokensToCheck := make([]string, 0)
	if startIndex == startIndex+elementsParsed {
		return "sp_name_not_parsed", fmt.Errorf("unable to parse stored procedure name")
	}
	cteSlice := sliceOfTokens[startIndex : startIndex+elementsParsed+1]
	// make sure that besides the elements to skip there is only 3 tokens: create/alter procedure procedure_name
	for index := 0; index < len(cteSlice); index++ {
		switch {
		case strings.Contains(cteSlice[index], "--") ||
			strings.Contains(cteSlice[index], "/*"):
			nTokensToSkip := skipComments(cteSlice, index)
			index += nTokensToSkip
		case !slices.Contains(tokensToSkip, cteSlice[index]):
			tokensToCheck = append(tokensToCheck, strings.ToLower(cteSlice[index]))
		}
	}

	switch {
	case len(tokensToCheck) <= 2:
		return "sp_name_not_parsed", fmt.Errorf("unable to parse stored procedure name")
	case (tokensToCheck[0] == "create" || tokensToCheck[0] == "alter") &&
		(tokensToCheck[1] == "procedure" || tokensToCheck[1] == "proc"):
		return replacer.Replace(tokensToCheck[2]), nil
	default:
		return "sp_name_not_parsed", fmt.Errorf("unable to parse stored procedure name")
	}
}

// GetLineage generates the data lineage from a list of sql server stored procedures.
// The resulting lineage structure is the following:
//
// sink_table -> stored_procedure (1 or more) -> list_of_sources
//
// The source tables are linked to each specific stored procedure.
// This structure was chosen because most likely you want to see for each table where does the data come from.
// CTEs (deeply nested as well), Temp Tables, Table Variables are all handled gracefully.
// You'll always see the original source tables.
func GetLineage(SpSlice []io.Reader) (map[string]map[string][]string, error) {
	var wg sync.WaitGroup
	limiterCh := make(chan struct{}, 50)
	resultsCh := make(chan spLineage, 100000)

	for _, sp := range SpSlice {
		tokens, err := lexer(sp)
		if err != nil {
			return map[string]map[string][]string{}, err
		}
		limiterCh <- struct{}{}
		wg.Add(1)
		go parser(&wg, limiterCh, resultsCh, tokens)
	}
	go func() {
		wg.Wait()
		close(limiterCh)
		close(resultsCh)
	}()

	lineage := aggregateLineageResults(resultsCh)

	return lineage, nil
}

// GenerateHtmlLineage generates an html file with the visualisation of the lineage provided.
// The resulting lineage structure in the html file is the following:
//
// sink_table -> stored_procedure (1 or more) -> list_of_sources
//
// The sink table for reference is with green/yellow background. You can click on it to hide/show the lineage.
// You will see the sink table also in the lineage graph according to the structure described.
// When you see a sink table that has many stored procedures it does not mean that all of them are regularly used.
// You should interpret it as: These are the stored procedures that can write data to this table.
func GenerateHtmlLineage(
	lineageMap map[string]map[string][]string, fileName string,
) error {
	f, err := os.Create(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	buffer := bufio.NewWriter(f)
	_, err = buffer.WriteString(
		`
		<!DOCTYPE html>
		<html lang="en">
		
		<body style="background-color:rgb(80,80,80);">
		`,
	)
	if err != nil {
		os.Remove(fileName)
		return err
	}
	for table, lineage := range lineageMap {
		tableCounter := 2000
		counterSp := 1000
		counterSources := 1
		_, err = buffer.WriteString(fmt.Sprintf(`
		<button class="mermaid" type="button" style="border:none;outline:none;background:none;display:table;margin: 0 auto;cursor:pointer;"
			onclick="showLineage('%s')">
			%%%%{init: {'theme':'forest'}}%%%%
			flowchart LR

			1([%s])

		</button>
		<pre class="mermaid" id="%s" style="visibility:visible;">
			%%%%{init: {'theme':'dark'}}%%%%
			flowchart LR

		`, table, table, table),
		)
		if err != nil {
			os.Remove(fileName)
			return err
		}
		for spName, sources := range lineage {
			for _, source := range sources {
				line := fmt.Sprintf(
					"\t\t\t\t%v([%v]) --> %v([%v]) \n",
					counterSources, source, counterSp, spName,
				)
				_, err := buffer.WriteString(line)
				if err != nil {
					os.Remove(fileName)
					return err
				}
				counterSources += 1
			}
			lineTable := fmt.Sprintf(
				"\t\t\t\t%v([%v]) --> %v([%v]) \n",
				counterSp, spName, tableCounter, table,
			)
			_, err := buffer.WriteString(lineTable)
			if err != nil {
				os.Remove(fileName)
				return err
			}
			counterSp += 1
		}
		_, err := buffer.WriteString("\t\t</pre>")
		if err != nil {
			os.Remove(fileName)
			return err
		}
		tableCounter += 1
	}
	_, err = buffer.WriteString(
		`
			<script type="module">
			import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
			let config = { startOnLoad: true, flowchart: { useMaxWidth: false, htmlLabels: true } };
			mermaid.initialize(config);
		</script>
		<script>
			function showLineage(el) {
				var x = document.getElementById(el);
				if (x.style.visibility === "hidden") {
					x.style.visibility = "visible";
				} else {
					x.style.visibility = "hidden";
				}
			}
		</script>
		</body>

		</html>
	`)
	if err != nil {
		os.Remove(fileName)
		return err
	}
	buffer.Flush()

	return nil
}
