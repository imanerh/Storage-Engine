package tests

import (
	"StorageEngine/memdb"
	"StorageEngine/sstable"
	"testing"
	"time"
	"os"
)

func TestSSTable(t *testing.T) {
	
	// Create the db
	filePath := "test_sstable_wal.log"
	wal, err := memdb.OpenWAL(filePath)
	if err != nil {
		t.Fatalf("Error opening WAL: %s", err)
	}
	sstablesDirectory := "testSSTableFiles_sstable_test"
	db, err := memdb.NewDB(wal, sstablesDirectory, memdb.Threshold(5))
	if err != nil {
		t.Fatalf("Error creating DB: %s", err)
	}
	defer func() {
		if err := wal.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(filePath); err != nil {
			t.Fatal(err)
		}
		if err := os.RemoveAll(sstablesDirectory); err != nil {	
			t.Fatalf("Error removing test SSTable files directory: %s", err)
		}
	}()

	// Test setting values 
	// This should flush to an SSTable
	for i := 0; i < 5; i++ {
		key := "key" + string(rune(i+'0'))
		value := []byte("value" + string(rune(i+'0')))

		err := db.Set(key, value)
		if err != nil {
			t.Fatalf("Error setting value: %s", err)
		}
	}

	// Pause for a moment to let the SSTable flush
	time.Sleep(2 * time.Second)

	// This should flush to another SSTable
	for i := 5; i < 11; i++ {
		key := "key" + string(rune(i+'0'))
		value := []byte("value" + string(rune(i+'0')))

		err := db.Set(key, value)
		if err != nil {
			t.Fatalf("Error setting value: %s", err)
		}
	}

	// Read the number of SSTables after setting values
	sstables := db.SSTableIDs
 
	if len(sstables) != 2 {
		t.Errorf("Expected 2 SSTables, got %d", len(sstables))
	}

	// Read all sstables of db
	ssts, err := db.ReadSSTables()
	if err != nil {
		t.Fatalf("Error reading SSTables: %s", err)
	}

	// Checking if sstables are valid
	expectedMagicNumber := uint32(221003)
	if ssts[0].Header.MagicNumber != expectedMagicNumber {
		t.Errorf("Expected Magic Number %d, got %d", expectedMagicNumber, ssts[0].Header.MagicNumber)
	}

	expectedEntryCount := uint32(5)
	if ssts[0].Header.EntryCount != expectedEntryCount {
		t.Errorf("Expected Entry Count %d, got %d", expectedEntryCount, ssts[0].Header.EntryCount)
	}

	expectedSmallestKey := "key5"
	if string(ssts[0].Header.SmallestKey) != expectedSmallestKey {
		t.Errorf("Expected Smallest Key %s, got %s", expectedSmallestKey, string(ssts[0].Header.SmallestKey))
	}

	expectedLargestKey := "key9"
	if string(ssts[0].Header.LargestKey) != expectedLargestKey {
		t.Errorf("Expected Largest Key %s, got %s", expectedLargestKey, string(ssts[0].Header.LargestKey))
	}

	expectedVersion := 1
	if ssts[0].Header.Version != uint16(expectedVersion) {
		t.Errorf("Expected Version %d, got %d", expectedVersion, ssts[0].Header.Version)
	}

	// Iterating through KeyValues and comparing Operation, Key, and Value
	expectedKeyValues := []struct {
		Operation sstable.Operation
		Key       string
		Value     string
	}{
		{Operation: sstable.OpSet, Key: "key5", Value: "value5"},
		{Operation: sstable.OpSet, Key: "key6", Value: "value6"},
		{Operation: sstable.OpSet, Key: "key7", Value: "value7"},
		{Operation: sstable.OpSet, Key: "key8", Value: "value8"},
		{Operation: sstable.OpSet, Key: "key9", Value: "value9"},
	}

	for i, kv := range ssts[0].KeyValues {
		expectedKV := expectedKeyValues[i]

		if kv.Operation != expectedKV.Operation {
			t.Errorf("Mismatch in Operation at index %d", i)
		}
		if string(kv.Key) != expectedKV.Key {
			t.Errorf("Mismatch in Key at index %d", i)
		}
		if string(kv.Value) != expectedKV.Value {
			t.Errorf("Mismatch in Value at index %d", i)
		}
	}

	expectedChecksum := uint32(3325148388)
	if ssts[0].Checksum != expectedChecksum {
		t.Errorf("Expected Checksum %d, got %d", expectedChecksum, ssts[0].Checksum)
	}
}
