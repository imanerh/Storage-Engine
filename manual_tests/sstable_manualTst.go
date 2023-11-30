package manual_tests

import (
	"StorageEngine/memdb"
	"fmt"
	"time"
)

func sstable_manualTest() {
	// Initialize a new memDB with a threshold of 5
	wal, err := memdb.OpenWAL("testwal.txt")
	if err != nil {
		fmt.Println(err)
	}
	defer wal.Close()
	db, err := memdb.NewDB(wal, "testsstablefiles", memdb.Threshold(5))
	if err != nil {
		fmt.Println(err)
	}

	// Set values to the database exceeding the threshold
	for i := 0; i < 5; i++ {
		key := "key" + string(rune(i+'0'))
		value := []byte("value" + string(rune(i+'0')))

		err := db.Set(key, value)
		if err != nil {
		}
	}
	
    time.Sleep(2 * time.Second) 
	
	for i := 5; i < 11; i++ {
		key := "key" + string(rune(i+'0'))
		value := []byte("value" + string(rune(i+'0')))

		err := db.Set(key, value)
		if err != nil {
		}
	}

	// Read the number of sstables after setting values
	sstables := db.SSTableIDs

	fmt.Println(len(sstables))

	ssts, err := db.ReadSSTables()
	if err != nil {
		fmt.Println(err)
	}

	for i := 0; i < len(ssts); i++ {
		fmt.Printf("=====SSTable Number %d=====\n", i+1)
		fmt.Println("> Metadata:")

		fmt.Printf("\tMagicNumber: %d\n", ssts[i].Header.MagicNumber)
		fmt.Printf("\tEntryCount: %d\n", ssts[i].Header.EntryCount)
		fmt.Printf("\tSmallestKey: %s\n", string(ssts[i].Header.SmallestKey))
		fmt.Printf("\tLargestKey: %s\n", string(ssts[i].Header.LargestKey))
		fmt.Printf("\tVersion: %d\n", ssts[i].Header.Version)

		fmt.Println("> Key-Value Pairs:")
		
		for j:= uint32(0); j < ssts[i].Header.EntryCount; j++ {
			fmt.Printf("\t%d: ", j+1)
			if ssts[i].KeyValues[j].Operation == 1 {
				fmt.Print("set ")
			} else {
				fmt.Print("del ")
			}
			fmt.Printf("%s", string(ssts[i].KeyValues[j].Key))
			fmt.Printf(" => %s", string(ssts[i].KeyValues[j].Value))
			fmt.Println("")
		}
	
		fmt.Printf("> Checksum: %d\n", ssts[i].Checksum)
	}
	

}