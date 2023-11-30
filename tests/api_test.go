package tests

import (
	"StorageEngine/handlers"
	"StorageEngine/memdb"
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

// TestGlobal performs a series of tests for set, get, and delete operations on a storage engine.
func TestGlobal(t *testing.T) {
	// Create a mock WAL
	filePath := "test_wal.log"
	wal, err := memdb.OpenWAL(filePath)
	if err != nil {
		t.Fatalf("Error opening WAL: %v", err)
	}
	sstablesDirectory := "testSSTableFiles"
	db, err := memdb.NewDB(wal, "testSSTableFiles", memdb.Threshold(5))
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

	
	// set k-value pairs, 5 of them will be flushed to an sst file and one will be set in memory
	setTest(t, db, wal, `{"name":"imane", "age":"20", "city":"azilal", "school":"cs", "university":"um6p", "gender":"female"}`)
	// These get requests should be granted
	grantedGetTest(t, db, "name", "imane")
	grantedGetTest(t, db, "age", "20")
	// The del should be granted
	grantedDeleteTest(t, db, wal, "name", "imane")
	// The get should not be granted for 'name' key after deletion
	notGrantedGetTest(t, db, "name")
	// The del should not be granted for 'name' key after deletion
	notGrantedDeleteTest(t, db, wal, "name")

	// set new k-value pairs, this will flush to another sstable
	setTest(t, db, wal, `{"a":"b", "c":"d", "e":"f", "g":"h", "i":"j"}`)
	// Look for a key in the oldest sstable, this will test if the process of searching all sstables is working
	// This get request should be granted
	grantedGetTest(t, db, "university", "um6p")
	// Delete a a key that does not exist in memory, only in sstables
	// This del should be granted
	grantedDeleteTest(t, db, wal, "university", "um6p")
	// This get should not be granted 
	notGrantedGetTest(t, db, "university")
	// This del should not be granted
	notGrantedDeleteTest(t, db, wal, "university")

}

// setTest performs a set operation on the storage engine.
func setTest(t *testing.T, db *memdb.DB, wal *memdb.WAL, arg string) {
	payload := []byte(arg)
	req, err := http.NewRequest("POST", "/set", bytes.NewBuffer(payload))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Create a new HTTP request recorder
	recorder := httptest.NewRecorder()

	// SetHandler
	handlers.SetHandler(db, wal).ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Errorf("SetHandler returned wrong status code: got %v, want %v", recorder.Code, http.StatusOK)
	}
}

// grantedGetTest tests the retrieval of a key and compares the response with the expected value.
func grantedGetTest(t *testing.T, db *memdb.DB, key string, expectedValue string) {
	req, err := http.NewRequest("GET", "/get?key="+key, nil)
	if err != nil {
		t.Fatal(err)
	}

	// ServeHTTP and retrieve response
	recorder := httptest.NewRecorder()
	handlers.GetHandler(db).ServeHTTP(recorder, req)

	// Check the response status code
	if recorder.Code != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, recorder.Code)
	}

	// Check the response body
	expectedValue = "Value: " + expectedValue
	if recorder.Body.String() != expectedValue {
		t.Errorf("Expected: %s, got: %s", expectedValue, recorder.Body.String())
	}
}

// notGrantedGetTest tests the retrieval of a key that doesn't exist in the database.
func notGrantedGetTest(t *testing.T, db *memdb.DB, key string) {
	req, err := http.NewRequest("GET", "/get?key="+key, nil)
	if err != nil {
		t.Fatal(err)
	}

	// ServeHTTP and retrieve response
	recorder := httptest.NewRecorder()
	handlers.GetHandler(db).ServeHTTP(recorder, req)

	// Check the response status code after deletion
	if recorder.Code != http.StatusNotFound {
		t.Errorf("Expected status code %d, got %d", http.StatusNotFound, recorder.Code)
	}
}

// grantedDeleteTest tests the deletion of a key and its expected value from the database.
func grantedDeleteTest(t *testing.T, db *memdb.DB, wal *memdb.WAL, key string, expectedDeletedValue string) {
	req, err := http.NewRequest("DELETE", "/del?key="+key, nil)
	if err != nil {
		t.Fatal(err)
	}

	// ServeHTTP and retrieve response
	recorder := httptest.NewRecorder()
	handlers.DeleteHandler(db, wal).ServeHTTP(recorder, req)

	// Check the response status code
	if recorder.Code != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, recorder.Code)
	}

	// Check the response body
	expectedDeletedValue = "Deleted value: " + expectedDeletedValue
	if recorder.Body.String() != expectedDeletedValue {
		t.Errorf("Expected: %s, got: %s", expectedDeletedValue, recorder.Body.String())
	}
}

// notGrantedDeleteTest tests the deletion of a key that doesn't exist in the database.
func notGrantedDeleteTest(t *testing.T, db *memdb.DB, wal *memdb.WAL, key string) {
	req, err := http.NewRequest("DELETE", "/del?key="+key, nil)
	if err != nil {
		t.Fatal(err)
	}

	// ServeHTTP and retrieve response
	recorder := httptest.NewRecorder()
	handlers.DeleteHandler(db, wal).ServeHTTP(recorder, req)

	// Check the response status code after deletion
	if recorder.Code != http.StatusNotFound {
		t.Errorf("Expected status code %d, got %d", http.StatusNotFound, recorder.Code)
	}
}
