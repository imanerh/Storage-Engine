package main

import (
	"StorageEngine/handlers"
	"StorageEngine/memdb"
	"fmt"
	"log"
	"net/http"
)

func main() {

	// Open WAL file
	wal, err := memdb.OpenWAL("wal.log")
	if err != nil {
		log.Fatalf("Error opening WAL: %v", err)
	}
	defer wal.Close()

	db, err := memdb.NewDB(wal, "SSTableFiles", memdb.Threshold(5))
	if err != nil {
		log.Fatalf("Error creating DB: %s", err)
	}

	// Mounting handlers from the external package
	mux := http.NewServeMux()
	handlers.RegisterGetHandler(mux, db)
	handlers.RegisterSetHandler(mux, db, wal)
	handlers.RegisterDeleteHandler(mux, db, wal)

	fmt.Println("Server is running on port 8080...")
	log.Fatal(http.ListenAndServe(":8080", mux))
	
}