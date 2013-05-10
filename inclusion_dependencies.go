package main

import (
	"bufio"
	"fmt"
	"github.com/willf/bitset"
	"hash/fnv"
	"io"
	"math"
	"os"
	"runtime"
	"sort"
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

type Database []*Table

type Table struct {
	columns []*Column
	path    string
	name    string
}

type Column struct {
	table      *Table
	id         int
	name       string
	dataType   string
	stats      Statistics
	filter     BloomFilter
	candidates map[*Column]bool
}

type Statistics interface {
	Print()
	Add(s string)
	FinishAnalysis(rowCount int)
	SimiliarTo(other Statistics) bool
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

func (this *intStatistics) SimiliarTo(s Statistics) bool {
	other := s.(*intStatistics)
	return this.minimum >= other.minimum && this.maximum <= other.maximum
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

func (this *stringStatistics) SimiliarTo(s Statistics) bool {
	other := s.(*stringStatistics)
	return this.minimum >= other.minimum && this.maximum <= other.maximum && len(this.shortest) >= len(other.shortest) && len(this.longest) <= len(other.longest)
}

type BloomFilter interface {
	Initialize(m uint)
	Add(s string)
	Bits() *bitset.BitSet
	SimiliarTo(other BloomFilter) bool
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

func (this *bloomFilter) Bits() *bitset.BitSet {
	return this.bits
}

func (this *bloomFilter) SimiliarTo(other BloomFilter) bool {
	return this.bits.Difference(other.Bits()).None()
}

type intBloomFilter struct {
	bloomFilter
}

func (this *intBloomFilter) Add(s string) {
	number, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		number = math.MaxInt64
	}
	index := uint(number) % this.m
	this.Set(index)
}

type stringBloomFilter struct {
	bloomFilter
	k uint
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

func ReadTableMapping(dataDir string) (result Database) {
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

func BuildTable(dataDir string, mapping []string) (result *Table) {
	result = &Table{name: mapping[0], path: dataDir + mapping[1]}
	result.BuildColumns(mapping[2:])
	return result
}

func (this *Table) BuildColumns(columnNames []string) {
	this.columns = make([]*Column, len(columnNames))
	for i, name := range columnNames {
		this.columns[i] = &Column{table: this, name: name}
	}
}

func (this *Table) Analyze(done chan int) {
	/*fmt.Println("started analyzing", this.path)*/
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
			}
			column.stats.Add(row[columnIndex])
			column.filter.Add(row[columnIndex])
		}
		rowCount++
		/*if rowCount > 10000 {*/
			/*break*/
		/*}*/
	}
	for _, column := range this.columns {
		column.stats.FinishAnalysis(rowCount)
	}
	/*fmt.Println("finished analyzing", this.path)*/
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
	} else {
		this.dataType = "string"
		this.stats = &stringStatistics{averageLength: 0.0}
		this.filter = &stringBloomFilter{k: 4}
		this.filter.Initialize(1000000)
	}
}

func (db Database) Preprocess() {
	// start table analysis in separate threads
	// the channel c gets messaged each time an analysis is finished
	c := make(chan int, len(db))
	for _, table := range db {
		go table.Analyze(c)
	}

	// wait for one message per table
	// afterwards all analyses are finished
	for i := 0; i < len(db); i++ {
		<-c
	}
}

func (db Database) AllColumns() (result []*Column) {
	for _, table := range db {
		result = append(result, table.columns...)
	}
	return result
}

func (db Database) BuildCandidates() {
	columns := db.AllColumns()
	c := make(chan int)
	for i, column := range columns {
		column.id = i
		go func(column *Column) {
			column.BuildCandidates(columns)
			c <- 1
		}(column)
	}
	for i := 0; i < len(columns); i++ {
		<-c
	}
}

func (this *Column) Bits() int {
	return int(this.filter.Bits().Count())
}

func (this *Column) Name() string {
	return this.table.name + "." + this.name
}

func (this *Column) SimiliarTo(other *Column) bool {
	return this.dataType == other.dataType && this.stats.SimiliarTo(other.stats) && this.filter.SimiliarTo(other.filter)
}

func (this *Column) Values() (result map[string]bool) {
	result = make(map[string]bool)
	index := -1
	for i, column := range this.table.columns {
		if column == this {
			index = i
		}
	}
	lineReader := NewLineReader(this.table.path)
	for {
		row := ReadRow(lineReader)
		if len(row) == 0 {
			break
		}
		result[row[index]] = true
	}
	return result
}

func (this *Column) BuildCandidates(others []*Column) {
	this.candidates = make(map[*Column]bool)
	for _, other := range others {
		this.candidates[other] = (this != other) && this.SimiliarTo(other)
	}
}

type InclusionGraph struct {
	nodes           []*Column
	adjacencyMatrix [][]bool
}

type Candidate struct {
	a *Column
	b *Column
}

func (this *InclusionGraph) Add(candidate *Candidate) {
	fmt.Println("Found Inclusion", candidate.a.Name(), candidate.a.Bits(), len(candidate.a.candidates), "<=", candidate.b.Name(), candidate.b.Bits(), len(candidate.b.candidates))
	a := candidate.a.id
	b := candidate.b.id
	// complete transistive closure
	// A <= B & I <= A -> I <= B
	// I <= B & B <= C -> I <= C
	for _, iConnectedTo := range(this.adjacencyMatrix) {
		if iConnectedTo[a] {
			iConnectedTo[b] = true
			delete(this.columns[i].candidates, this.columns[b])
			for c, bConnectedToC := range(this.adjacencyMatrix[b]) {
				if bConnectedToC {
					iConnectedTo[c] = true
					delete(this.columns[i].candidates, this.columns[c])
				}
			}
		}
	}
	fmt.Println("total:", this.Count())
}

func (this *InclusionGraph) Count() (result int) {
	result = 0
	for i, _ := range(this.adjacencyMatrix) {
		for j, _ := range(this.adjacencyMatrix) {
			if (i != j) && this.adjacencyMatrix[i][j] {
				result += 1
			}
		}
	}
	return result
}

func (db Database) ToInclusionGraph() (result *InclusionGraph) {
	nodes := db.AllColumns()
	adjacencyMatrix := make([][]bool, len(nodes))
	for i := range adjacencyMatrix {
		adjacencyMatrix[i] = make([]bool, len(nodes))
		adjacencyMatrix[i][i] = true
	}
	result = &InclusionGraph{nodes, adjacencyMatrix}
	return result
}

func (db Database) Check(candidate *Candidate) bool {
	otherValues := candidate.b.Values()
	for e := range candidate.a.Values() {
		if !otherValues[e] {
			return false
		}
	}
	return true
}

func (db Database) NextCandidate() (result *Candidate) {
	columns := db.AllColumns()
	sort.Sort(ByMostCandidates(columns))
	for _, column := range columns {
		for _, candidate := range columns {
			if column.candidates[candidate] {
				delete(column.candidates, candidate)
				fmt.Println("NextCandidate", column.Name(), column.Bits(), len(column.candidates), "->", candidate.Name(), candidate.Bits(), len(candidate.candidates))
				return &Candidate{column, candidate}
			}
		}
	}
	return nil
}

type ByMostCandidates []*Column

func (cs ByMostCandidates) Len() int {
	return len(cs)
}
func (cs ByMostCandidates) Swap(i, j int) {
	cs[i], cs[j] = cs[j], cs[i]
}
func (cs ByMostCandidates) Less(i, j int) bool {
	return len(cs[i].candidates) > len(cs[j].candidates)
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	fmt.Println("using", runtime.NumCPU(), "threads")

	dataDir := ParseDataDir()
	fmt.Println("data is in", dataDir)

	db := ReadTableMapping(dataDir)
	fmt.Println("found", len(db), "table definitions")

	db.Preprocess()
	db.BuildCandidates()
	graph := db.ToInclusionGraph()
	for {
		candidate := db.NextCandidate()
		if db.Check(candidate) {
			graph.Add(candidate)
		}
	}

}
