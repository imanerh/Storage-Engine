package tests

import (
	"StorageEngine/memdb"
	"os"
	"testing"
)

func TestRecovery(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := "temp_dir"

	err := os.Mkdir(tempDir, 0755)
	if err != nil {
		t.Fatalf("Error creating temporary directory: %s", err)
	}
	defer os.RemoveAll(tempDir)

	// Initialize a WAL and DB
	wal, err := memdb.OpenWAL(tempDir + "/test_wal.log")
	if err != nil {
		t.Fatalf("Error opening WAL: %s", err)
	}

	db, err := memdb.NewDB(wal, tempDir+"/testSSTableFiles")
	if err != nil {
		t.Fatalf("Error creating DB: %s", err)
	}

	defer func() {
		if err := os.RemoveAll(tempDir + "/testSSTableFiles"); err != nil {
			t.Fatalf("Error removing test SSTable files directory: %s", err)
		}
	}()

	// Perform some operations on the database
	err = db.Set("key1", []byte("value1"))
	if err != nil {
		t.Fatalf("Error setting value: %s", err)
	}

	// Simulate a crash or abrupt shutdown by closing the WAL without flushing
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen the WAL for recovery
	walForRecovery, err := memdb.OpenWAL(tempDir + "/test_wal.log")
	if err != nil {
		t.Fatalf("Error opening WAL for recovery: %s", err)
	}
	defer func() {
		if err := walForRecovery.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	// Attempt to recover the database after the crash
	dbRecovered, err := memdb.NewDB(walForRecovery, tempDir+"/testSSTableFiles")
	if err != nil {
		t.Fatalf("Error recovering DB: %s", err)
	}

	// Check if the recovered database has the previous state
	value, err := dbRecovered.Get("key1")
	if err != nil {
		t.Fatalf("Error getting value: %s", err)
	}

	expectedValue := []byte("value1")
	if string(value) != string(expectedValue) {
		t.Errorf("Expected value %s, got %s", expectedValue, value)
	}
}