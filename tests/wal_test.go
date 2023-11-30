package tests

import (
	"StorageEngine/memdb"
	"bytes"
	"os"
	"testing"
)

// TestWriteAndReadEntry tests writing an entry to WAL and reading it back
func TestWALWriteAndReadEntry(t *testing.T) {

	// Create the WAL file for testing
	filePath := "test_wal.log"
	wal, err := memdb.OpenWAL(filePath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := wal.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(filePath); err != nil {
			t.Fatal(err)
		}
	}()

	// Prepare a WAL Set Record for testing
	testSetRecord := memdb.WALRecord{
		Operation: memdb.OpSet,
		Key:       []byte("name"),
		Value:     []byte("imane"),
	}
	
	// Write the testSetRecord to the WAL
	if err := wal.WriteEntry(testSetRecord); err != nil {
		t.Fatal(err)
	}

	// Prepare a WAL Del Record for testing
	testDelRecord := memdb.WALRecord{
		Operation: memdb.OpDel,
		Key:       []byte("testKey"),
		Value:     nil,
	}
	
	// Write the testDelRecord to the WAL
	if err := wal.WriteEntry(testDelRecord); err != nil {
		t.Fatal(err)
	}

	// Read the set record from the WAL
	readSetRecord, err := wal.ReadNextEntry()
	if err != nil {
		t.Fatal(err)
	}
	// Read the del record from the WAL
	readDelRecord, err := wal.ReadNextEntry()
	if err != nil {
		t.Fatal(err)
	}

	// Check if readSetRecord matches the testSetRecord
	if readSetRecord.Operation != testSetRecord.Operation || 
		!bytes.Equal(readSetRecord.Key, testSetRecord.Key) ||
		!bytes.Equal(readSetRecord.Value, testSetRecord.Value) {
		t.Errorf("Read record does not match written record")
	}
	// Check if readDelRecord matches the testDelRecord
	if readDelRecord.Operation != testDelRecord.Operation || 
		!bytes.Equal(readDelRecord.Key, testDelRecord.Key) ||
		!bytes.Equal(readDelRecord.Value, testDelRecord.Value) {
		t.Errorf("Read record does not match written record")
	}
}

// TestCreateWatermark verifies the creation/update of a watermark
// func TestCreateWatermark(t *testing.T) {
// 	filePath := "test_wal.log"

// 	wal, err := memdb.OpenWAL(filePath)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	defer func() {
// 		if err := wal.Close(); err != nil {
// 			t.Fatal(err)
// 		}
// 		if err := os.Remove(filePath); err != nil {
// 			t.Fatal(err)
// 		}
// 	}()

// 	// Create a checkpoint
// 	if err := wal.SetWatermark(); err != nil {
// 		t.Fatal(err)
// 	}
	
// 	if wal.MetaData.Watermark != wal.MetaData.Offset {
// 		t.Errorf("WAL checkpoint is not set correctly")
// 	}
// }
