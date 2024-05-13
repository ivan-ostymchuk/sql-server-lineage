package main

/*
#include <stdlib.h>
typedef struct {
	char* err;
} error;
*/
import "C"
import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"unsafe"

	core "github.com/ivan-ostymchuk/sql-server-lineage/sql_server_lineage"
)

type sinksLineage struct {
	Lineage map[string]map[string][]string `json:"lineage"`
}

func newError(s string, args ...interface{}) C.error {
	if s == "" {
		return C.error{}
	}
	msg := fmt.Sprintf(s, args...)
	return C.error{C.CString(msg)}
}

//export delError
func delError(err C.error) {
	if err.err == nil {
		return
	}
	C.free(unsafe.Pointer(err.err))
}

//export extractLineage
func extractLineage(storedProcedures **C.char, length C.int) (*C.char, C.error) {
	spSlice := make([]io.Reader, 0)
	for _, sp := range unsafe.Slice(storedProcedures, length) {
		spSlice = append(spSlice, strings.NewReader(C.GoString(sp)))
	}
	lineage, err := core.GetLineage(spSlice)
	if err != nil {
		al := C.CString("")
		result := (*C.char)(al)
		return result, newError("failed to get lineage: %v", err)
	}
	sl := sinksLineage{Lineage: lineage}
	jsonLineage, err := json.Marshal(sl)
	if err != nil {
		al := C.CString("")
		result := (*C.char)(al)
		return result, newError("failed to convert to json: %v", err)
	}
	al := C.CString(string(jsonLineage))
	result := (*C.char)(al)

	return result, newError("")
}

//export generateHtmlLineage
func generateHtmlLineage(
	storedProcedures **C.char, length C.int, fileName *C.char,
) C.error {
	spSlice := make([]io.Reader, 0)
	for _, sp := range unsafe.Slice(storedProcedures, length) {
		spSlice = append(spSlice, strings.NewReader(C.GoString(sp)))
	}
	lineage, err := core.GetLineage(spSlice)
	if err != nil {
		newError("failed to get lineage: %v", err)
	}
	f := C.GoString(fileName)
	err = core.GenerateHtmlLineage(lineage, f)
	if err != nil {
		newError("failed to generate html file: %v", err)
	}

	return newError("")
}

func main() {}
