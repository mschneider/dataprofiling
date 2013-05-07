package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"encoding/binary"
	"hash/fnv"
	"github.com/willf/bitset"
	"github.com/willf/bloom"
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

func hash(entry []byte) (results []byte) {
	hash := fnv.New64()
	hash.Write(entry)
	digest := hash.Sum64()
	results = make([]byte, 10)
	binary.PutUvarint(results, digest)
	fmt.Println("bytes", binary.PutUvarint(results, digest))
	return results
}

func (this *BloomFilter) Add(entry []byte) {
	hashes := hash(entry)
	for i := 0; i < int(this.k); i++ {
		this.bits.Set(uint(hashes[i]))
	}
}

func NewBloomFilter() *bloom.BloomFilter {
	m := uint(7*10*1000*1000)
	k := uint(5)
	return bloom.New(m, k)
}

type Table struct {
	columns []Column
	path string
	name string
}

type Column struct {
	name string
	filter *bloom.BloomFilter
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


func typeCheck(value interface{}) (result string) {

	switch value.(type) {
	case int:
		return "int"
	case float64:
		return "float64"
	case string:
		return "string"
	default:
		return "NA"
	}
	panic("unreachable")
}


func (this *Table) Analyze() {
	lineReader := NewLineReader(this.path)
    
    var countrows int

	for {
		row := ReadRow(lineReader)
		if len(row) == 0 {
			break
		}
		for i, value := range(row) {

			column := this.columns[i]
			column.AnalyzeString(value)

			
			

		}
		countrows++


	}
	/*

	Brauche Idee wie ich die Nummer der Spalten der Tabelle bekomme,
	um diese als Abruchbedingung für die for Schleife verwenden zu können

	for i := 0; i < numberrows; i++ {

		this.columns[i].average=this.columns[i].average/countrows

	}*/

}

func (this *Column) AnalyzeString(value string) {
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
	if this.datatype != "string" {
		this.datatype = typeCheck(value)

	}
	/*
	Type Konflikt brauche für Average float und len gibt Int
	this.average = this.average + len(value)
	*/
}



func main() {
	dataDir := ParseDataDir()
	fmt.Println("data is in", dataDir)
	tables := ReadTableMapping(dataDir)
	fmt.Println("found ", len(tables), "table definitions")
	for _, table := range(tables[100:]) {
		fmt.Println("analyzing", table.path)
		table.Analyze()
	}
/*	first := tables[0]
	fmt.Println("first", first.fileName)
	first.Analyze() */

}
