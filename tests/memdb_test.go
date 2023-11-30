package tests

import (
	"StorageEngine/memdb"
	"bytes"
	"os"
	"reflect"
	"testing"
)

func TestMemdb_SetGetDelete(t *testing.T) {

	// Create the db
	filePath := "test_wal.log"
	wal, err := memdb.OpenWAL(filePath)
	if err != nil {
		t.Fatalf("Error opening WAL: %s", err)
	}
	sstablesDirectory := "testSSTableFiles_memdb_test"
	db, err := memdb.NewDB(wal, sstablesDirectory)
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

	key := "tkey"
	value := []byte("tvalue")

	// Test Set and Get
	err = db.Set(key, value)
	if err != nil {
		t.Fatal(err)
	}

	val, err := db.Get(key)
	if err != nil {
		t.Errorf("Error retrieving value for key: %s", err)
	}
	if !reflect.DeepEqual(val, value) {
		t.Errorf("Expected value: %v, got: %v", value, val)
	}

	// Test Delete
	val, err = db.Delete(key)
	if err != nil {
		t.Errorf("Error deleting key: %s", err)
	}
	if !bytes.Equal(val, value) {
		t.Errorf("Expected deleted value: %v, got: %v", value, val)
	}

	_, err = db.Get(key)
	if err != memdb.ErrKeyNotFound {
		t.Errorf("Expected key not found error, got: %s", err)
	}

	val, err = db.Delete(key)
	if err != memdb.ErrKeyNotFound {
		t.Errorf("Expected key not found error, got: %s", err)
	}
	if !bytes.Equal(val, nil) {
		t.Errorf("Expected deleted value: nil, got: %v", val)
	}

}

func TestMemdb_ListKeys(t *testing.T) {

	// Create the db
	filePath := "test_wal.log"
	wal, err := memdb.OpenWAL(filePath)
	if err != nil {
		t.Fatalf("Error opening WAL: %s", err)
	}
	sstablesDirectory := "testSSTableFiles_memdb_test"
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
	}()

	keys := []string{"c", "a", "b"}

	for _, key := range keys {
		err = db.Set(key, []byte(key))
		if err != nil {
			t.Fatal(err)
		}
	}

	sortedKeys := db.ListKeys()
	expectedKeys := []string{"a", "b", "c"}

	if !reflect.DeepEqual(sortedKeys, expectedKeys) {
		t.Errorf("Expected keys: %v, got: %v", expectedKeys, sortedKeys)
	}
}
