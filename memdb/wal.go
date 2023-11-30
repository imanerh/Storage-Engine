package memdb

import (
	"encoding/binary"
	"io"
	"os"
	"sync"
)

const (
	// WALFilePermission represents the file permission for the WAL file.
	WALFilePermission = 0744
	// WALRecordHeaderSize represents the size of the WAL record header.
	WALRecordHeaderSize = 1 + 4 + 4 // Operation(1 byte) + KeyLength(4 bytes) + ValueLength(4 bytes)
	// WALMetadataSize represents the size of the metadata in the WAL file.
	WALMetadataSize = 16 // Size of offset then size of watermark (8 bytes each)
)

// WALMetadata represents the metadata to be stored in the WAL file (watermark and offset)
type WALMetadata struct {
	Offset    int64
	Watermark int64 // Watermark is an offset indicating the flushed position
}

// WAL represents the Write-Ahead Log.
type WAL struct {
	MetaData WALMetadata
	file     *os.File
	mu       sync.Mutex
}

// Operation represents the type of operation in the WAL.
type Operation uint8

const (
	OpSet Operation = iota
	OpDel
)

// WALRecord represents an entry in the WAL.
type WALRecord struct {
	Operation Operation
	Key       []byte
	Value     []byte
}

// OpenWAL opens or creates a WAL file.
func OpenWAL(filePath string) (*WAL, error) {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, WALFilePermission)
	if err != nil {
		return nil, err
	}

	wal := &WAL{
		MetaData: WALMetadata{},
		file:     file,
	}

	// Read the metadata if it exists
	err = wal.readMetadata()
	if err != nil {
		return nil, err
	}
	// If the file is created for the first time, we write to the file the metadata: watermark=0 and offset=0
	err = wal.writeMetadata()
	if err != nil {
		return nil, err
	}

	return wal, nil
}

// WriteEntry writes a WAL record to the WAL file.
func (wal *WAL) WriteEntry(record WALRecord) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	// Prepare the record
	header := make([]byte, WALRecordHeaderSize)
	keyLen := uint32(len(record.Key))
	valueLen := uint32(len(record.Value))
	header[0] = byte(record.Operation)
	binary.BigEndian.PutUint32(header[1:5], keyLen)
	binary.BigEndian.PutUint32(header[5:9], valueLen)

	// Calculate the size of the written record
	recordSize := int64(WALRecordHeaderSize + len(record.Key) + len(record.Value))

	// Seek to the correct offset before writing
	_, err := wal.file.Seek(wal.MetaData.Offset, io.SeekStart)
	if err != nil {
		return err
	}

	// Write record header and content
	_, err = wal.file.Write(header)
	if err != nil {
		return err
	}
	_, err = wal.file.Write(record.Key)
	if err != nil {
		return err
	}
	_, err = wal.file.Write(record.Value)
	if err != nil {
		return err
	}

	// Update the offset to where the next record should be written
	wal.MetaData.Offset += recordSize
	err = wal.writeMetadata()
	if err != nil {
		return err
	}

	return nil
}

// ReadNextEntry reads the next WAL record from the WAL file
// It returns a WALRecord containing the operation type, key, and value.
// Finally, it updates the watermark to the current file position for the next read.
func (wal *WAL) ReadNextEntry() (WALRecord, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	_, err := wal.file.Seek(wal.MetaData.Watermark, io.SeekStart)
	if err != nil {
		return WALRecord{}, err
	}

	header := make([]byte, WALRecordHeaderSize)
	_, err = io.ReadFull(wal.file, header)
	if err != nil {
		return WALRecord{}, err
	}

	op := Operation(header[0])
	keyLen := binary.BigEndian.Uint32(header[1:5])
	valueLen := binary.BigEndian.Uint32(header[5:9])

	key := make([]byte, keyLen)
	_, err = io.ReadFull(wal.file, key)
	if err != nil {
		return WALRecord{}, err
	}

	value := make([]byte, valueLen)
	_, err = io.ReadFull(wal.file, value)
	if err != nil {
		return WALRecord{}, err
	}

	// Update the offset for the next read
	wal.MetaData.Watermark, _ = wal.file.Seek(0, io.SeekCurrent)
	err = wal.writeMetadata()
	if err != nil {
		return WALRecord{}, err
	}

	return WALRecord{Operation: op, Key: key, Value: value}, nil
}

// Close closes the WAL file.
func (wal *WAL) Close() error {
	// Write metadata to the WAL file before closing
	err := wal.writeMetadata()
	if err != nil {
		return err
	}
	return wal.file.Close()
}

// writeMetadata writes metadata (offset and watermark) to the WAL file.
func (wal *WAL) writeMetadata() error {
	meta := make([]byte, WALMetadataSize)
	binary.BigEndian.PutUint64(meta[0:8], uint64(wal.MetaData.Offset))
	binary.BigEndian.PutUint64(meta[8:16], uint64(wal.MetaData.Watermark))

	_, err := wal.file.WriteAt(meta, 0)
	if err != nil {
		return err
	}
	return nil
}

// readMetadata reads metadata (offset and watermark) from the WAL file.
func (wal *WAL) readMetadata() error {
	fileInfo, err := wal.file.Stat()
	if err != nil {
		return err
	}

	// If the file size is smaller than the expected metadata size, set defaults
	if fileInfo.Size() < WALMetadataSize {
		wal.MetaData.Offset = int64(WALMetadataSize)
		wal.MetaData.Watermark = int64(WALMetadataSize)
		return nil
	}

	// Otherwise
	meta := make([]byte, WALMetadataSize)
	_, err = wal.file.ReadAt(meta, 0)
	if err != nil {
		return err
	}

	wal.MetaData.Offset = int64(binary.BigEndian.Uint64(meta[0:8]))
	wal.MetaData.Watermark = int64(binary.BigEndian.Uint64(meta[8:16]))

	return nil
}
