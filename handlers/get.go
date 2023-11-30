package handlers

import (
    "fmt"
    "net/http"
    "StorageEngine/memdb"
)

func GetHandler(db *memdb.DB) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        keys, ok := r.URL.Query()["key"]
        if !ok || len(keys[0]) < 1 {
            http.Error(w, "Key not provided", http.StatusBadRequest)
            return
        }

        key := keys[0]
        value, err := db.Get(key)
        if err != nil {
            if err == memdb.ErrKeyNotFound {
                http.Error(w, "Key not found", http.StatusNotFound)
                return
            }
            http.Error(w, "Internal server error", http.StatusInternalServerError)
            return
        }

        // Return the value found for the key
        fmt.Fprintf(w, "Value: %s", value)
    }
}

func RegisterGetHandler(mux *http.ServeMux, db *memdb.DB) {
    mux.HandleFunc("/get", GetHandler(db))
}
