# sql-server-lineage

I worked many times on sql server data warehouses or or sql server application databases with almost   
no documentation and lots of stored procedures transforming data. At some point I wanted a tool that would   
analyse all the stored procedures and generate a lineage that I would be able to send to a Data Catalog or   
visualise immediately to get an idea of the structure and debug much more easily. So I decided to develop it.

This is a library available in Golang and Python that would enable you to do 2 things:
- Get the lineage of all the stored procedures in a database as a Map.
- Generate an html file with the lineage visualised (That you can share on Google Pages or Github Pages or wherever you like).

CTEs (deeply nested as well), Temp Tables, Table Variables are all handled gracefully. You'll always see the original source tables.

The resulting lineage structure is the following:
    sink_table -> stored_procedure (1 or more) -> list_of_sources

Map Example:
```
{
    'db_name.schema.table_sink': {
        'schema.stored_procedure_1': [
            'schema.table_source_1',
            'schema.table_source_2',
            'schema.table_source_3',
        ],
        'schema.stored_procedure_2': [
            'schema.table_source_a',
            'schema.table_source_b',
            'schema.table_source_c',
        ],
    },
    'db_name.schema.table_sink_2': {
        'schema.stored_procedure_3': [
            'schema.table_source',
        ],
    },
}
```

Html Example:

![Sample Image](https://github.com/ivan-ostymchuk/sql-server-lineage/blob/main/lineage_example.png?raw=true)

Therefore, everything is centered around the sink_table. Because most likely you want to see for each table where does the data come from. If you need it different (for a Data Catalog) you can transform the objects and adapt them to your requirement.
In the html you will see the sink table as reference in a different color (green) and you will see it again in the lineage.

I decided to generate html files instead of starting a local server (like DBT does) because I wanted to keep it simple and make it easy to host somewhere as a static website and therefore sharing it with other Data Engineers, Data Analysts, etc.

This project is named sql-server-lineage because I want to keep it specialised only on Sql Server. To generate the lineage you need to develop a custom implementation of a Sql parser. I did it for Sql Server as I've been using it recently but I do not intend to develop parsers for other sql dialects.

# DISCLAIMER
Since this library analyses stored procedures, if you transform data externally and then write it to the database there is nothing you can do for the lineage. Also if the stored procedures rely heavily on dynamic sql especially with table names passed as parameters then the library would not be able to determine the table names for the lineage.

# Get Started

### Installation
```
go get github.com/ivan-ostymchuk/sql-server-lineage
```

### Example to generate the lineage directly from sql server
In this example we get the stored procedures definitions from sql server
but you can also read the definitions from files.

```
package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"time"

	sqln "github.com/ivan-ostymchuk/sql-server-lineage/sql_server_lineage"

	_ "github.com/microsoft/go-mssqldb"
)

func main() {
	userName := "userName"
	password := "password"
	dbName := "dbName"
	host := "host"
	port := "port"

	conString := createConnectionString(userName, password, dbName, host, port)

	// Context
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	appSignal := make(chan os.Signal, 3)
	signal.Notify(appSignal, os.Interrupt)
	go func() {
		<-appSignal
		stop()
	}()
	db, err := sql.Open("sqlserver", conString)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.SetConnMaxLifetime(0)
	db.SetMaxIdleConns(3)
	db.SetMaxOpenConns(3)
	// Open connection
	openDbConnection(ctx, db)

	// Query data
	query := `
		SELECT OBJECT_DEFINITION(object_id)  as sp_definition
		FROM sys.procedures
		WHERE object_id not in (select major_id from sys.extended_properties);
	`
	data, err := getStoredProcedures(ctx, db, query)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Extracted %v stored procedures \n", len(data))
	fmt.Println("Parsing the stored procedures")

	spList := make([]io.Reader, 0)
	for _, sp := range data {
		spList = append(spList, strings.NewReader(sp))
	}
	l, err := sqln.GetLineage(spList)
	if err != nil {
		panic(err)
	}
	err = sqln.GenerateHtmlLineage(l, "lineage_generated.html")
	if err != nil {
		panic(err)
	}
}

func createConnectionString(
	userName string, password string, dbName string, host string, port string,
) string {
	q := url.Values{}
	q.Add("database", dbName)

	u := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(userName, password),
		Host:     fmt.Sprintf("%s:%s", host, port),
		RawQuery: q.Encode(),
	}
	return u.String()
}

func openDbConnection(ctx context.Context, db *sql.DB) {
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		log.Fatal("Unable to connect to database: ", err)
	}
	fmt.Println("Connected to the Database")
}

func getStoredProcedures(
	ctx context.Context,
	db *sql.DB,
	query string,
) ([]string, error) {

	timeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	rows, err := db.QueryContext(timeout, query)
	if err != nil {
		return []string{}, err
	}
	defer rows.Close()

	// Save the results
	spList := make([]string, 0)
	var sp string
	for rows.Next() {
		err = rows.Scan(&sp)
		if err != nil {
			return []string{}, err
		}
		spList = append(spList, sp)
	}
	err = rows.Err()
	if err != nil {
		return []string{}, err
	}

	return spList, nil
}

```

This project operates under the MIT License.