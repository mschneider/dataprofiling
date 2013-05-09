package main

import (
	"bufio"
	"fmt"
	"github.com/willf/bitset"
	"hash/fnv"
	"io"
	"math"
	"os"
	"reflect"
	"strconv"
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
	line = strings.Trim(line, "\n")
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
	columns []*Column
	path    string
	name    string
}

type Column struct {
	name     string
	dataType string
	stats    Statistics
	filter   BloomFilter
}

type Statistics interface {
	Print()
	Add(s string)
	FinishAnalysis(rowCount int)
}

type intStatistics struct {
	average float64
	maximum int64
	minimum int64
}

func (this *intStatistics) Print() {
	fmt.Println("max:", this.maximum, "\t| min:", this.minimum, "\t| avg:", this.average)
}

func (this *intStatistics) Add(s string) {
	value, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return
	}
	if this.minimum > value {
		this.minimum = value
	}
	if this.maximum < value {
		this.maximum = value
	}
	this.average += float64(value)
}

func (this *intStatistics) FinishAnalysis(rowCount int) {
	this.average /= float64(rowCount)
}

type stringStatistics struct {
	averageLength float64
	maximum       string
	minimum       string
	longest       string
	shortest      string
}

func (this *stringStatistics) Print() {
	fmt.Println("max:", this.maximum, "\t| min:", this.minimum, "\t| lon:", this.longest, "\t| sho:", this.shortest, "\t| avg:", this.averageLength)
}

func (this *stringStatistics) Add(value string) {
	if this.minimum == "" || this.minimum > value {
		this.minimum = value
	}
	if this.maximum == "" || this.maximum < value {
		this.maximum = value
	}
	if this.longest == "" || len(this.longest) < len(value) {
		this.longest = value
	}
	if this.shortest == "" || len(this.shortest) > len(value) {
		this.shortest = value
	}
	this.averageLength += float64(len(value))
}

func (this *stringStatistics) FinishAnalysis(rowCount int) {
	this.averageLength /= float64(rowCount)
}

type BloomFilter interface {
	Initialize(m uint)
	Add(s string)
	Print()
}

type bloomFilter struct {
	bits *bitset.BitSet
	m    uint
}

func (this *bloomFilter) Set(index uint) {
	this.bits = this.bits.Set(index)
}

func (this *bloomFilter) Initialize(m uint) {
	this.m = m
	this.bits = bitset.New(m)
}

func (this *bloomFilter) Print() {
	fmt.Println("m:", this.m, "\t| bit-len:", this.bits.Len(), "\t| bit-count:", this.bits.Count())
}

type intBloomFilter struct {
	bloomFilter
}

type stringBloomFilter struct {
	bloomFilter
	k uint
}

func (this *intBloomFilter) Add(s string) {
	number, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		number = math.MaxInt64
	}
	index := uint(number) % this.m
	this.Set(index)
}

func (this *stringBloomFilter) Add(s string) {
	for _, index := range this.Hashes(s) {
		this.Set(index)
	}
}

func (this *stringBloomFilter) Hashes(input string) (results []uint) {
	bytes := []byte(input)
	hash := fnv.New64()
	for i := 0; i < int(this.k); i++ {
		hash.Write(bytes)
		digest := uint(hash.Sum64()) % this.m
		results = append(results, digest)
	}
	return results
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

func BuildColumns(columnNames []string) (result []*Column) {
	result = make([]*Column, len(columnNames))
	for i, name := range columnNames {
		result[i] = &Column{name: name}
	}
	return result
}

func (this Table) BuildStatistics(done chan int) {
	fmt.Println("started analyzing", this.path)
	lineReader := NewLineReader(this.path)
	rowCount := 0
	for {
		row := ReadRow(lineReader)
		if len(row) == 0 {
			break
		}
		for columnIndex, column := range this.columns {
			if rowCount == 0 {
				column.AnalyzeType(row[columnIndex])
				fmt.Println("Type", column, reflect.TypeOf(column.stats), column.stats, reflect.TypeOf(column.filter), column.filter)
			}
			column.stats.Add(row[columnIndex])
			column.filter.Add(row[columnIndex])
		}
		rowCount++
	}
	for _, column := range this.columns {
		column.stats.FinishAnalysis(rowCount)
	}
	fmt.Println("finished analyzing", this.path)
	done <- 1
}

func IsInt(s string) bool {
	_, err := strconv.ParseInt(s, 10, 64)
	return err == nil
}

func IsFloat(s string) bool {
	_, err := strconv.ParseFloat(s, 32)
	return err == nil
}

func IsBool(s string) bool {
	return ("." == s) || ("y" == s) || ("n" == s)
}

func (this *Column) AnalyzeType(value string) {
	if IsInt(value) {
		this.dataType = "int"
		this.stats = &intStatistics{average: 0.0, maximum: math.MinInt64, minimum: math.MaxInt64}
		this.filter = new(intBloomFilter)
		this.filter.Initialize(1000000)
	} else if IsFloat(value) {
		this.dataType = "float"
		this.stats = &stringStatistics{averageLength: 0.0}
		this.filter = &stringBloomFilter{k: 4}
		this.filter.Initialize(1000000)
	} else if IsBool(value) {
		this.dataType = "bool"
		this.stats = &stringStatistics{averageLength: 0.0}
		this.filter = &stringBloomFilter{k: 1}
		this.filter.Initialize(2)
	} else {
		this.dataType = "string"
		this.stats = &stringStatistics{averageLength: 0.0}
		this.filter = &stringBloomFilter{k: 4}
		this.filter.Initialize(1000000)
	}
}

func main() {
	dataDir := ParseDataDir()
	fmt.Println("data is in", dataDir)
	tables := ReadTableMapping(dataDir)
	fmt.Println("found ", len(tables), "table definitions")
	c := make(chan int, len(tables))
	for _, table := range tables {
		go table.BuildStatistics(c)
	}
	for i := 0; i < len(tables); i++ {
		<-c
	}
	for _, table := range tables {
		for _, column := range table.columns {
			fmt.Println("Column:", column)
			column.stats.Print()
			column.filter.Print()
		}
	}
}
