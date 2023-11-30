package handlers

import (
    "fmt"
    "net/http"
    "StorageEngine/memdb"
)

func DeleteHandler(db *memdb.DB, wal *memdb.WAL) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        keys, ok := r.URL.Query()["key"]
        if !ok || len(keys[0]) < 1 {
            http.Error(w, "Key not provided", http.StatusBadRequest)
            return
        }

        key := keys[0]

		val, err := db.Delete(key)
        if err != nil {
            if err == memdb.ErrKeyNotFound {
                http.Error(w, "Key not found", http.StatusNotFound)
                return
            }
            http.Error(w, "Internal server error", http.StatusInternalServerError)
            return
        }

        // Return the existing value (if it existed) for the deleted key
        fmt.Fprintf(w, "Deleted value: %s", val)
    }
}

func RegisterDeleteHandler(mux *http.ServeMux, db *memdb.DB, wal *memdb.WAL) {
    mux.HandleFunc("/del", DeleteHandler(db, wal))
}
