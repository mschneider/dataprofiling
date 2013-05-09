package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"hash/fnv"
	"github.com/willf/bitset"
	"regexp"
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

type BloomFilter struct {
	m uint
	k uint
	bits *bitset.BitSet
}

func (this *BloomFilter) hash(entry []byte) (results []uint) {
	hash := fnv.New64()
	for i := 0; i < int(this.k); i++ {
		hash.Write(entry)
		digest := uint(hash.Sum64()) % this.m
		results = append(results, digest)
	}
	return results
}

func (this *BloomFilter) Add(entry []byte) {
	for _, index := range(this.hash(entry)) {
		this.bits.Set(index)
	}
}

func NewBloomFilter() *BloomFilter {
	m := uint(1000*1000)
	k := uint(4)
	return &BloomFilter{m, k, bitset.New(m)}
}

type Table struct {
	columns []Column
	path string
	name string
}

type Column struct {
	name string
	filter *BloomFilter
	maximum string
	minimum string
	longest int
	shortest int
	average float32
	datatype string
}

func ReadTableMapping(dataDir string) (result []Table) {
	mappingFileName := dataDir + "mapping.tsv"
	lineReader := NewLineReader(mappingFileName)
	for {
		fields := ReadRow(lineReader)
		if len(fields) == 0 {
			break
		}
		result = append(result, BuildTable(dataDir, fields))
	}
	return result
}

func BuildTable(dataDir string, mapping []string) (table Table) {
	table.name = mapping[0]
	table.path = dataDir + mapping[1]
	table.columns = BuildColumns(mapping[2:])
	return table
}

func BuildColumns(columnNames []string) (result []Column) {
	result = make([]Column, len(columnNames))
	for i, name := range(columnNames) {
		result[i] = Column{name, NewBloomFilter(), "", "", 0,0,0.0,""}
	}
	return result
}


func typeCheck(value string) (result string) {

	int_check, _ := regexp.Compile(`^\s*[+-]?[0-9]+\s*$`)
	float_check, _ := regexp.Compile(`^\s*[+-]?[0-9]*\.+[0-9]+([eE][-+]?[0-9]+)?\s*$`)

	if int_check.MatchString(value) {

		return "int"

	} else if float_check.MatchString(value) {
	
		return "float64"

	} else {

		return "string"

	}
	panic("unreachable")
}


func (this *Table) Analyze() {
	lineReader := NewLineReader(this.path)
	rowCount := 0
	for {
		row := ReadRow(lineReader)
		if len(row) == 0 {
			break
		}
		for i, value := range(row) {
			column := this.columns[i]
			column.AnalyzeString(value)
		}
		/* Wofür wird der Block genutzt ?
		if line > 100000 {
			break
		}
		*/
		rowCount++
	}
	for _, column := range(this.columns) {
		column.FinishAnalysis(rowCount)
	}
}

func (this *Column) AnalyzeString(value string) {
	//Falls bereits ein String in der Spalte vorkommt interessiert mich nicht ob der nächste Wert evtl. nur eine Zahl ist
	if this.datatype != "string" {
		this.datatype = typeCheck(value)
	}
	this.filter.Add([]byte(value))
	if this.minimum > value {
		this.minimum = value
	}
	if this.maximum < value {
		this.maximum = value
	}
	if this.longest < len(value) {
		this.longest = len(value)
	}
	if this.shortest > len(value) {
		this.shortest = len(value)
	}
	this.average += float32(len(value))
}

func (this *Column) FinishAnalysis(rowCount int) {
	this.average = this.average/float32(rowCount)
}

func main() {
	dataDir := ParseDataDir()
	fmt.Println("data is in", dataDir)
	tables := ReadTableMapping(dataDir)
	fmt.Println("found ", len(tables), "table definitions")
	for _, table := range(tables) {
		fmt.Println("analyzing", table.path)
		table.Analyze()
	}
}
