package memdb

import (
	"StorageEngine/sstable"
	"errors"
	"os"
	"sort"
	"sync"
	"time"
)

var ErrKeyNotFound = errors.New("Key not found")

const (
	DefaultThreshold = 100 // The default threshold value for the memtable size which
	// represents the number of key-value pairs
	CompactionThreshold = 2 // The thershold to perform compaction, i.e. if the number of sst files exceeds
	// CompactionThreshold, we perform compaction on these files
)

// DB is an in-memory key/value database using a sorted map.
type DB struct {
	mu         sync.RWMutex
	data       map[string]sstable.Pair
	keys       []string
	wal        *WAL
	threshold  int      // Threshold for the memtable size which represents the number of key-value pairs
	sstableDir string   // Directory to store SSTables
	SSTableIDs []string // Track associated SSTables in an ascending order based on the time of creation
}

// NewDB initializes a new in-memory key/value DB with threshold set to DefaultThreshold if none specified
func NewDB(wal *WAL, sstableDir string, options ...Option) (*DB, error) {
	db := &DB{
		data:       make(map[string]sstable.Pair),
		keys:       make([]string, 0),
		wal:        wal,
		sstableDir: sstableDir,
		SSTableIDs: make([]string, 0),
	}

	// Apply options
	for _, opt := range options {
		opt(db)
	}
	// Set default threshold if none specified
	if db.threshold == 0 {
		db.threshold = DefaultThreshold
	}

	// Updating SSTableIDs to acheive recovery
	// Check if the directory exists
	_, err := os.Stat(sstableDir)
	if err != nil {
		if os.IsNotExist(err) {
			// Recover database state
			err = db.Recover()
			if err != nil {
				return nil, err
			}
			return db, nil // SSTableIDs will be empty
		}
		return nil, err
	}

	// If the directory exists,
	// Initialize SSTableIDs with existing file names in sstableDir
	files, err := os.ReadDir(sstableDir)
	if err != nil {
		return nil, err
	}

	// Slice to store file information (name, creation time)
	var fileInfos []struct {
		name string
		time time.Time
	}
	for _, file := range files {
		if !file.IsDir() {
			fileInfo, err := file.Info()
			if err != nil {
				return nil, err
			}
			fileInfos = append(fileInfos, struct {
				name string
				time time.Time
			}{sstableDir + "/" + file.Name(), fileInfo.ModTime()})
		}
	}
	// Sort fileInfos based on creation time
	sort.Slice(fileInfos, func(i, j int) bool {
		return fileInfos[i].time.Before(fileInfos[j].time)
	})
	// Append sorted file names to SSTableIDs
	for _, fileInfo := range fileInfos {
		db.SSTableIDs = append(db.SSTableIDs, fileInfo.name)
	}

	// If we exceed the CompactionThreshhold, perform compaction
	// err = db.CompactSSTables()
	// if err != nil {
	// 	return nil, err
	// }

	// Recover database state
	err = db.Recover()
	if err != nil {
		return nil, err
	}

	return db, nil
}

// Option is a functional option for DB
type Option func(*DB)

// WithThreshold sets the threshold value for the memtable size
func Threshold(threshold int) Option {
	return func(db *DB) {
		db.threshold = threshold
	}
}

// Set inserts or updates a key-value pair into the database while maintaining sorted order
func (db *DB) Set(key string, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 1 - Set the value in the memtable
	// Binary search the index at which we should insert/update the key in the memtable
	idx := sort.Search(len(db.keys), func(i int) bool {
		return db.keys[i] >= key
	})

	if idx < len(db.keys) && db.keys[idx] == key {
		// Key already exists, update the value
		db.data[key] = sstable.Pair{Value: value, Marker: false}
	} else {
		// Key doesn't exist, insert at idx
		db.keys = append(db.keys, "")
		copy(db.keys[idx+1:], db.keys[idx:])
		db.keys[idx] = key
		db.data[key] = sstable.Pair{Value: value, Marker: false}
	}

	// 2 - Write to WAL
	walRecord := WALRecord{
		Operation: OpSet,
		Key:       []byte(key),
		Value:     value,
	}
	if err := db.wal.WriteEntry(walRecord); err != nil {
		return err
	}

	// 3- Check if memtable size exceeds threshold
	if len(db.keys) >= db.threshold {
		// If so, create and write an SSTable
		err := db.FlushToSSTable()
		if err != nil {
			return err
		}
	}

	return nil
}

// Get gets the value for the given key if the key exists. Otherwise, it returns Key Not Found Error
func (db *DB) Get(key string) ([]byte, error) {
	// db.mu.RLock()
	// defer db.mu.RUnlock()

	// Check in-memory data
	value, ok := db.data[key]
	if ok {
		if !value.Marker { // If the marker is false, i.e. th key is set
			return value.Value, nil
		}
		return nil, ErrKeyNotFound // The key was deleted
	}

	// If not found in memory, search in SST files
	val, err := db.GetValueFromSSTables(key)
	if err != nil {
		// If the key is found in some sst file but with a del operation (i.e. it was deleted)
		// Or if the key was not found in any of the sst files
		// Then, err is KeyNotFound
		return nil, err
	}

	return val, nil
}

// Delete deletes the value for the given key
func (db *DB) Delete(key string) ([]byte, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Check if the key exists in the in-memory database
	val, exists := db.data[key]
	if !exists {
		// If not found in memory, search in SST files
		value, err := db.GetValueFromSSTables(key)
		if err != nil { // If key not found in SST files, return keyn not found error
			return nil, err
		}
		// Set the marker to true to indicate deletion in the in-memory database
		// Binary search the index at which we should insert the key in the memtable
		idx := sort.Search(len(db.keys), func(i int) bool {
			return db.keys[i] >= key
		})
		db.keys = append(db.keys, "")
		copy(db.keys[idx+1:], db.keys[idx:])
		db.keys[idx] = key
		db.data[key] = sstable.Pair{Value: value, Marker: true}

		// Write deletion to WAL
		walRecord := WALRecord{
			Operation: OpDel,
			Key:       []byte(key),
			Value:     nil, // Value doesn't matter for delete operation in WAL
		}
		if err := db.wal.WriteEntry(walRecord); err != nil {
			return nil, err
		}
		return value, nil
	}
	if exists && val.Marker == true { // If it is in memory but was already deleted
		return nil, ErrKeyNotFound
	}
	// If the key exists in memory, set the marker to true to indicate deletion
	db.data[key] = sstable.Pair{Value: nil, Marker: true}

	// Write deletion to WAL
	walRecord := WALRecord{
		Operation: OpDel,
		Key:       []byte(key),
		Value:     nil, // Value doesn't matter for delete operation in WAL
	}
	if err := db.wal.WriteEntry(walRecord); err != nil {
		return nil, err
	}

	// Return the value before deletion
	return val.Value, nil
}

// ListKeys returns a sorted list of keys.
func (db *DB) ListKeys() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	keysCopy := make([]string, len(db.keys))
	copy(keysCopy, db.keys)
	return db.keys
}

func (db *DB) FlushToSSTable() error {
	// Ensure the directory exists or create it if it doesn't
	if err := os.MkdirAll(db.sstableDir, 0755); err != nil {
		return err
	}
	// Create an SSTable and write it to a file of the format sstable_file_YYMMDDHHMMSS.sst
	sstableFilename := db.sstableDir + "/sstable_file_" + time.Now().Format("060102150405") + ".sst"
	err := sstable.CreateAndWriteSSTable(sstableFilename, db.data)
	if err != nil {
		return err
	}

	// Clear memtable after flushing to SSTable
	db.data = make(map[string]sstable.Pair)
	db.keys = make([]string, 0)

	// Track the SSTable filename
	db.SSTableIDs = append(db.SSTableIDs, sstableFilename)
	// If we exceed the CompactionThreshhold, perform compaction
	// err = db.CompactSSTables()
	// if err != nil {
	// 	return err
	// }
	
	// Update the watermark of the wal
	for i := 0; i < db.threshold; i++ {
		db.wal.ReadNextEntry()
	}
	err = db.wal.writeMetadata()
	if err != nil {
		return err
	}

	return nil
}

// ReadSSTables returns a list of all sstables of db
// The list of SSTables is sorted from the most recent sstable (index 0) to the oldest
func (db *DB) ReadSSTables() ([]*sstable.SSTable, error) {
	var sstables []*sstable.SSTable
	for i := len(db.SSTableIDs) - 1; i >= 0; i-- {
		sst, err := sstable.ReadSSTable(db.SSTableIDs[i])
		if err != nil {
			return nil, err
		}
		sstables = append(sstables, sst)
	}
	return sstables, nil
}

// GetValueFromSSTables searches for a key in the SSTables from newest to oldest,
// retrieving its associated value if present and not marked for deletion.
// If the key is found and marked for deletion, it returns ErrKeyNotFound.
// If the key is not found, it returns ErrKeyNotFound.
func (db *DB) GetValueFromSSTables(key string) ([]byte, error) {
	// Search in SSTables from newest to oldest
	sstables, err := db.ReadSSTables()
	if err != nil {
		return nil, err
	}

	for _, sst := range sstables {
		// Skip the SSTable if the key falls outside the range defined by its smallest and largest keys.
		// if key < string(sst.Header.SmallestKey) || key > string(sst.Header.LargestKey) {
		// 	continue
		// }

		// Binary search in SSTable in reverse order
		idx := sort.Search(len(sst.KeyValues), func(i int) bool {
			return string(sst.KeyValues[i].Key) >= key // Reverse search
		})

		if idx >= 0 && idx < len(sst.KeyValues) && string(sst.KeyValues[idx].Key) == key {
			// Check if the operation is a delete
			if sst.KeyValues[idx].Operation == sstable.OpDel {
				return nil, ErrKeyNotFound
			}
			return sst.KeyValues[idx].Value, nil
		}
	}

	return nil, ErrKeyNotFound
}

// Recover replays unflushed operations stored in the Write-Ahead Log (WAL)
// to restore the database state in case of a crash or abrupt shutdown.
// It checks for unflushed operations and replays them, applying 'Set' and 'Delete' operations
// based on the records in the WAL, ensuring consistency after recovery.
func (db *DB) Recover() error {
	// Check if the WAL has unflushed operations
	currentOffset := db.wal.MetaData.Offset
	if db.wal.MetaData.Watermark < currentOffset {

		// Replay unflushed operations
		for {
			// This seeks to the watermark, reads a wal record and updates the watermark
			if db.wal.MetaData.Watermark == currentOffset {
				break
			}
			record, err := db.wal.ReadNextEntry()

			if err != nil {
				return err
			}
			switch record.Operation {
			case OpSet:
				err := db.Set(string(record.Key), record.Value)
				if err != nil {
					return err
				}
			case OpDel:
				_, err := db.Delete(string(record.Key))
				if err != nil {
					return err
				}
			}
		}

	}

	return nil
}

// Perform compaction on SSTables if the total number of sst files exceeds CompactionThreshold
func (db *DB) CompactSSTables() error {
	if len(db.SSTableIDs) < CompactionThreshold {
		return nil // No need for compaction
	}
	for {
		if len(db.SSTableIDs) < CompactionThreshold {
			break
		}
		// Collect smaller SSTables for compaction (e.g., take the first CompactionThreshold (e.g. 5) SSTables for merging)
		sstablesToCompact := db.SSTableIDs[:CompactionThreshold]

		// Merge smaller SSTables into a single larger SSTable
		compactedSSTable, err := sstable.MergeSSTables(sstablesToCompact, db.sstableDir)
		if err != nil {
			return err
		}

		// Update SSTableIDs to reflect the compacted SSTable
		db.SSTableIDs = append([]string{compactedSSTable}, db.SSTableIDs[CompactionThreshold:]...) // Replace compacted SSTables with the new one at their position

		// Delete the smaller SSTables that were merged during compaction
		for _, sstableID := range sstablesToCompact {
			err := os.Remove(sstableID)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
