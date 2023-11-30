package manual_tests

import "fmt"
import "StorageEngine/memdb"

func wal_manualTest() {

	wal, err := memdb.OpenWAL("wal.txt")
	if err != nil {
		fmt.Println(err)
	}
	defer wal.Close()

	// First Entry
	entry := memdb.WALRecord{Operation: memdb.OpSet, Key: []byte("name"), Value: []byte("imane")}
	err = wal.WriteEntry(entry)
	if err != nil {
		fmt.Println(err)
	}

	w, err := wal.ReadNextEntry()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf(("Operation: %d\n"), int(w.Operation))
	fmt.Printf("key: %s\n", w.Key)
	fmt.Printf(("Value: %s\n"), string(w.Value))

	// Second Entry
	entry = memdb.WALRecord{Operation: memdb.OpSet, Key: []byte("age"), Value: []byte("20")}
	err = wal.WriteEntry(entry)
	if err != nil {
		fmt.Println(err)
	}
	w, err = wal.ReadNextEntry()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf(("Operation: %d\n"), int(w.Operation))
	fmt.Printf("key: %s\n", w.Key)
	fmt.Printf(("Value: %s\n"), string(w.Value))

	// Third Entry
	entry = memdb.WALRecord{Operation: memdb.OpDel, Key: []byte("age"), Value: nil}
	err = wal.WriteEntry(entry)
	if err != nil {
		fmt.Println(err)
	}
	w, err = wal.ReadNextEntry()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf(("Operation: %d\n"), int(w.Operation))
	fmt.Printf("key: %s\n", w.Key)
	fmt.Printf(("Value: %s\n"), string(w.Value))


}