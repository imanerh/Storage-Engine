package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sort"
)

type Operation uint8

const (
	OpSet Operation = iota
	OpDel
)

const (
	SSTableHeaderSize = 4 + 4 + 4 + 4 + 2
)

// SSTableHeader represents the header of the SSTable file.
type SSTableHeader struct {
	MagicNumber uint32
	EntryCount  uint32
	SmallestKey []byte
	LargestKey  []byte
	Version     uint16
}

// KeyValuePair represents a key-value pair with an operation flag.
type KeyValuePair struct {
	Operation Operation // Indicates 'set' or 'delete' operation
	Key       []byte
	Value     []byte
}

// SSTable represents an SSTable file.
type SSTable struct {
	Header    SSTableHeader
	KeyValues []KeyValuePair
	Checksum  uint32
}

// Pair represents a structure holding a value ([]byte) and a marker (bool).
// The marker indicates whether the entry should be treated as a deletion (true) or a set (false)
type Pair struct {
	Value  []byte
	Marker bool  
}

// CreateAndWriteSSTable writes a memtable to an SSTable file.
func CreateAndWriteSSTable(filename string, data map[string]Pair) error {
	// Convert map to a slice of KeyValuePair
	var keyValuePairs []KeyValuePair
	for key, value := range data {
		if value.Marker {
			keyValuePairs = append(keyValuePairs, KeyValuePair{Operation: OpDel, Key: []byte(key), Value: nil})
		}
		keyValuePairs = append(keyValuePairs, KeyValuePair{Operation: OpSet, Key: []byte(key), Value: value.Value})
	}

	// Sort the slice based on keys
	sort.Slice(keyValuePairs, func(i, j int) bool {
		return bytes.Compare(keyValuePairs[i].Key, keyValuePairs[j].Key) < 0
	})

	// Set the smallest and largest keys
	smallestKey := keyValuePairs[0].Key
	largestKey := keyValuePairs[len(keyValuePairs)-1].Key

	// Create the SSTable object
	table := &SSTable{
		Header: SSTableHeader{
			MagicNumber: uint32(221003),             // Magic number identifying the SSTable format
			EntryCount:  uint32(len(keyValuePairs)), // Number of entries in the SSTable
			SmallestKey: smallestKey,                // Smallest key in the SSTable
			LargestKey:  largestKey,                 // Largest key in the SSTable
			Version:     1,                          // Version number for the SSTable format
		},
		KeyValues: keyValuePairs,
		Checksum:  uint32(0), // Checksum is initially set to 0
	}

	// Calculate Checksum
	checksum := calculateChecksum(table)
	table.Checksum = checksum

	// Write the SSTable to the file
	return WriteSSTable(filename, table)
}

// WriteSSTable writes the SSTable to a file.
func WriteSSTable(filename string, table *SSTable) error {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	//  Write the header
	if err := writeHeader(file, &table.Header); err != nil {
		return err
	}
	// Write the key-value pairs
	for _, kv := range table.KeyValues {
		if err := writeKeyValuePair(file, &kv); err != nil {
			return err
		}
	}

	// Write the checksum to the file
	cs := make([]byte, 4)
	binary.BigEndian.PutUint32(cs, table.Checksum)
	_, err = file.Write(cs)
	if err != nil {
		return err
	}

	return nil
}

// writeHeader writes SSTable header to a file.
func writeHeader(file *os.File, header *SSTableHeader) error {

	// Prepare the data to be written
	data := make([]byte, SSTableHeaderSize)

	magicNumber := uint32(header.MagicNumber)
	entryCount := uint32(header.EntryCount)
	binary.BigEndian.PutUint32(data[:4], magicNumber)
	binary.BigEndian.PutUint32(data[4:8], entryCount)

	copy(data[8:12], header.SmallestKey)
	copy(data[12:16], header.LargestKey)

	version := uint16(header.Version)
	binary.BigEndian.PutUint16(data[16:18], version)

	_, err := file.Write(data)
	if err != nil {
		return err
	}

	return nil
}

// Function to write KeyValuePair to file
func writeKeyValuePair(file *os.File, kv *KeyValuePair) error {

	// Prepare the data to be written
	data := make([]byte, 9)

	op := uint8(kv.Operation)
	keyLen := uint32(len(kv.Key))
	valueLen := uint32(len(kv.Value))
	data[0] = byte(op)
	binary.BigEndian.PutUint32(data[1:5], keyLen)
	binary.BigEndian.PutUint32(data[5:9], valueLen)

	_, err := file.Write(data)
	if err != nil {
		return err
	}
	_, err = file.Write(kv.Key)
	if err != nil {
		return err
	}
	_, err = file.Write(kv.Value)
	if err != nil {
		return err
	}
	return nil

}

// calculateChecksum calculates a CRC32 checksum for an SSTable.
// It uses CRC (Cyclic Redundancy Check) to generate a checksum by hashing the bytes of all keys and values in the SSTable.
// This helps detect data corruption or errors during read operations.
func calculateChecksum(table *SSTable) uint32 {
	crc := crc32.NewIEEE()

	for _, kv := range table.KeyValues {
		crc.Write(kv.Key)
		crc.Write(kv.Value)
	}

	return crc.Sum32()
}

// ReadSSTable reads the SSTable from a file.
func ReadSSTable(filename string) (*SSTable, error) {

	// Open the file
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Read the header
	header, err := readHeader(file)
	if err != nil {
		return nil, err
	}

	// Read the key-value pairs
	keyValues, err := readKeyValues(file, header.EntryCount)
	if err != nil {
		return nil, err
	}

	// Read checksum and validate
	expectedChecksum := calculateChecksum(&SSTable{Header: *header, KeyValues: keyValues})

	actualChecksumBuffer := make([]byte, 4)
	_, err = io.ReadFull(file, actualChecksumBuffer)
	if err != nil {
		return nil, err
	}
	actualChecksum := binary.BigEndian.Uint32(actualChecksumBuffer[:4])

	if actualChecksum != expectedChecksum {
		return nil, errors.New("Checksum mismatch!")
	}

	return &SSTable{
		Header:    *header,
		KeyValues: keyValues,
		Checksum:  actualChecksum,
	}, nil
}

// Function to read SSTable header from file
func readHeader(file *os.File) (*SSTableHeader, error) {

	data := make([]byte, SSTableHeaderSize)
	_, err := io.ReadFull(file, data)
	if err != nil {
		return nil, err
	}

	magicNumber := binary.BigEndian.Uint32(data[:4])
	entryCount := binary.BigEndian.Uint32(data[4:8])

	smallestKey := data[8:12]
	largestKey := data[12:16]

	version := binary.BigEndian.Uint16(data[16:18])

	return &SSTableHeader{MagicNumber: magicNumber,
		EntryCount:  entryCount,
		SmallestKey: smallestKey,
		LargestKey:  largestKey,
		Version:     version}, nil
}

// Function to read KeyValues from file
func readKeyValues(file *os.File, count uint32) ([]KeyValuePair, error) {
	var keyValues []KeyValuePair
	for i := uint32(0); i < count; i++ {
		kv := KeyValuePair{}

		data := make([]byte, 9)
		_, err := io.ReadFull(file, data)
		if err != nil {
			return nil, err
		}

		op := Operation(data[0])
		keyLen := binary.BigEndian.Uint32(data[1:5])
		valueLen := binary.BigEndian.Uint32(data[5:9])

		key := make([]byte, keyLen)
		_, err = io.ReadFull(file, key)
		if err != nil {
			return nil, err
		}

		val := make([]byte, valueLen)
		_, err = io.ReadFull(file, val)
		if err != nil {
			return nil, err
		}

		kv.Operation = op
		kv.Key = key
		kv.Value = val
		keyValues = append(keyValues, kv)
	}
	return keyValues, nil
}

// MergeSSTables merges multiple SSTable files into a single, larger SSTable file as part of the compaction process
// This function is called in the memdb.go file
func MergeSSTables(sstableIDs []string, outputDir string) (string, error) {
	// Read data from all SSTable files specified by sstableIDs
	var mergedData map[string]Pair

	for _, sstableID := range sstableIDs {
		sst, err := ReadSSTable(sstableID)
		if err != nil {
			return "", err
		}
		
		// Logic to merge contents (keys and values) from sst into mergedData
		// Initialize mergedData if it's nil
		if mergedData == nil {
			mergedData = make(map[string]Pair)
		}

		// Merge data from this SSTable into the mergedData map
		// i.e. simulate the process
		for _, kv := range sst.KeyValues {
			switch kv.Operation {
			case OpSet:
				mergedData[string(kv.Key)] = Pair{Value: kv.Value, Marker: false}
			case OpDel:
				// If there's a delete operation, mark the key as deleted in the mergedData
				mergedData[string(kv.Key)] = Pair{Value: nil, Marker: true}
			}
		}
	}

	// Create a new SSTable with the merged data
	// The name will be compact_sstable_[x.time].sst
	// where x is from the last sst file in sstableIDs
	lastSST := sstableIDs[len(sstableIDs)-1]
	mergedSSTableFilename := outputDir + "/compact_sstable_" + lastSST[len(outputDir)+1+12:]
	err := CreateAndWriteSSTable(mergedSSTableFilename, mergedData)
	if err != nil {
		return "", err
	}

	return mergedSSTableFilename, nil
}
