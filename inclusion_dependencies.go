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
	"sync"
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
	id      string
}

type Column struct {
	table      *Table
	id         string
	index      int
	name       string
	dataType   string
	stats      Statistics
	filter     BloomFilter
	values     map[string]bool
	candidates map[*Column]bool
}

type Statistics interface {
	Print()
	Add(s string)
	FinishAnalysis(rowCount int)
	SimiliarTo(other Statistics) bool
	ExampleValues() []string
}

type statistics struct {
	samples     []string
	initialized bool
}

func (this *statistics) Sample(s string) {
	return
	if !this.initialized {
		this.samples = make([]string, 0, 10)
		this.initialized = true
	}
	if len(this.samples) < 10 {
		for _, alreadySampled := range this.samples {
			if s == alreadySampled {
				return
			}
		}
		this.samples = append(this.samples, s)
	}
}

func (this *statistics) ExampleValues() (result []string) {
	return this.samples
}

type intStatistics struct {
	statistics
	average float64
	maximum int64
	minimum int64
}

func (this *intStatistics) Print() {
	fmt.Println("max:", this.maximum, "\t| min:", this.minimum, "\t| avg:", this.average)
}

func (this *intStatistics) Add(s string) {
	this.Sample(s)
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
	statistics
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
	this.Sample(value)
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
	Contains(values []string) bool
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
	index := this.Hash(s)
	this.Set(index)
}

func (this *intBloomFilter) Contains(values []string) bool {
	for _, value := range values {
		index := this.Hash(value)
		if !this.bits.Test(index) {
			return false
		}
	}
	return true
}

func (this *intBloomFilter) Hash(input string) (result uint) {
	number, err := strconv.ParseInt(input, 10, 64)
	if err != nil {
		number = math.MaxInt64
	}
	result = uint(number) % this.m
	return result
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

func (this *stringBloomFilter) Contains(values []string) bool {
	for _, value := range values {
		for _, index := range this.Hashes(value) {
			if !this.bits.Test(index) {
				return false
			}
		}
	}
	return true
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
	result = &Table{name: mapping[0], path: dataDir + mapping[1], id: strings.Split(mapping[1], ".")[0]}
	result.BuildColumns(mapping[2:])
	return result
}

func (this *Table) BuildColumns(columnNames []string) {
	this.columns = make([]*Column, len(columnNames))
	for i, name := range columnNames {
		this.columns[i] = &Column{table: this, name: name, id: fmt.Sprintf("c%03d", i), values: make(map[string]bool)}
	}
}

func (this *Table) Analyze() {
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
			value := row[columnIndex]
			column.stats.Add(value)
			column.filter.Add(value)
			column.values[value] = true
		}
		rowCount++
	}
	for _, column := range this.columns {
		column.stats.FinishAnalysis(rowCount)
	}
	/*fmt.Println("finished analyzing", this.path)*/
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
	var wg sync.WaitGroup
	// start table analysis in separate threads
	for _, table := range db {
		wg.Add(1)
		go func(table *Table) {
			table.Analyze()
			wg.Done()
		}(table)
	}
	// wait for each table to finish
	wg.Wait()
}

func (db Database) AllColumns() (result []*Column) {
	for _, table := range db {
		result = append(result, table.columns...)
	}
	return result
}

func (db Database) BuildCandidates() {
	var wg sync.WaitGroup
	columns := db.AllColumns()
	for i, column := range columns {
		column.index = i
		wg.Add(1)
		go func(column *Column) {
			column.BuildCandidates(columns)
			wg.Done()
		}(column)
	}
	// wait for each column to finish
	wg.Wait()
}

func (this *Column) Bits() int {
	return int(this.filter.Bits().Count())
}

func (this *Column) Name() string {
	return this.table.name + "." + this.name
}

func (this *Column) String() string {
	return fmt.Sprintf("%v[%v]", this.table.id, this.id)
}

func (this *Column) SimiliarTo(other *Column) bool {
	return (this.dataType == other.dataType) &&
		this.stats.SimiliarTo(other.stats) &&
		this.filter.SimiliarTo(other.filter)
}

func (this *Column) ReadValues() (result map[string]bool) {
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
	/*fmt.Println("started building candidates for column", this.String())*/
	this.candidates = make(map[*Column]bool)
	for _, other := range others {
		if (this != other) && this.SimiliarTo(other) {
			this.candidates[other] = true
		}
	}
	/*fmt.Println("finished building candidates for column", this.String())*/
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
	/*fmt.Println("Found Inclusion", candidate.a.Name(), candidate.a.Bits(), len(candidate.a.candidates), "<=", candidate.b.Name(), candidate.b.Bits(), len(candidate.b.candidates))*/
	a := candidate.a.index
	b := candidate.b.index
	// complete transistive closure
	// A <= B & I <= A -> I <= B
	// I <= B & B <= C -> I <= C
	for i, iConnectedTo := range this.adjacencyMatrix {
		if iConnectedTo[a] {
			iConnectedTo[b] = true
			delete(this.nodes[i].candidates, this.nodes[b])
			for c, bConnectedToC := range this.adjacencyMatrix[b] {
				if bConnectedToC {
					iConnectedTo[c] = true
					delete(this.nodes[i].candidates, this.nodes[c])
				}
			}
		}
	}
	/*fmt.Println("total:", this.Count())*/
}

func (this *InclusionGraph) Count() (result int) {
	result = 0
	for i, _ := range this.nodes {
		for j, _ := range this.nodes {
			if (i != j) && this.adjacencyMatrix[i][j] {
				result += 1
			}
		}
	}
	return result
}

func (this *InclusionGraph) Print() {
	for _, column := range this.nodes {
		for _, candidate := range this.nodes {
			if (column != candidate) && this.adjacencyMatrix[column.index][candidate.index] {
				fmt.Printf("%v\t%v\n", column.String(), candidate.String())
			}
		}
	}
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
	for e := range candidate.a.values {
		if !candidate.b.values[e] {
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
				/*fmt.Println("NextCandidate", column.Name(), column.Bits(), len(column.candidates), "->", candidate.Name(), candidate.Bits(), len(candidate.candidates))*/
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
	candidates := 0
	for _, column := range db.AllColumns() {
		candidates += len(column.candidates)
	}
	fmt.Println("found", candidates, "candidates")

	graph := db.ToInclusionGraph()
	for {
		candidate := db.NextCandidate()
		if candidate == nil {
			break
		}
		if db.Check(candidate) {
			graph.Add(candidate)
		}
	}
	fmt.Println("found", graph.Count(), "inclusions")

	graph.Print()
}
