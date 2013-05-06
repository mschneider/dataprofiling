package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func NewLineReader(fileName string) (reader *bufio.Reader) {
	file, err := os.Open(fileName)
	check(err)
	return bufio.NewReader(file)
}

func ReadRow(reader *bufio.Reader) (fields []string) {
	line, err := reader.ReadString('\n')
	if err == io.EOF {
		return
	}
	check(err)
	return strings.Split(line, "\t")
}

func ParseDataDir() (dataDir string) {
	if len(os.Args) != 2 {
		panic("provide a data directory")
	}
	dataDir = os.Args[1]
	if !strings.HasSuffix(dataDir, "/") {
		dataDir += "/"
	}
	return dataDir
}

type Table struct {
	columnNames []string
	fileName string
	name string
}

func ReadTableMapping(dataDir string) (result []Table) {
	mappingFileName := dataDir + "mapping.tsv"
	lineReader := NewLineReader(mappingFileName)
	for {
		fields := ReadRow(lineReader)
		if len(fields) == 0 {
			break
		}
		var table Table
		table.name = fields[0]
		table.fileName = dataDir + fields[1]
		table.columnNames = fields[2:]
		result = append(result, table)
	}
	return result
}

func (this *Table) Analyze() {
	lineReader := NewLineReader(this.fileName)
	for {
		fields := ReadRow(lineReader)
		if len(fields) == 0 {
			break
		}
	}
}

func main() {
	dataDir := ParseDataDir()
	fmt.Println("data is in", dataDir)
	tables := ReadTableMapping(dataDir)
	fmt.Println("found ", len(tables), "table definitions")
	first := tables[0]
	fmt.Println("first", first.fileName)
}
