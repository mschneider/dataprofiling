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
		result[i] = Column{name, NewBloomFilter()}
	}
	return result
}

func (this *Table) Analyze() {
	lineReader := NewLineReader(this.path)
	for {
		fields := ReadRow(lineReader)
		if len(fields) == 0 {
			break
		}
		for i, value := range(fields) {
			column := this.columns[i]
			column.filter.Add([]byte(value))
		}
	}
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
}
