package handlers

import (
	"StorageEngine/memdb"
	"encoding/json"
	"fmt"
	"net/http"
)

func SetHandler(db *memdb.DB, wal *memdb.WAL) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        var data map[string]interface{}

        if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
            http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
            return
        }

        if len(data) == 0 {
            http.Error(w, "No key-value pairs found in the payload", http.StatusBadRequest)
            return
        }

        for key, value := range data {
            // Convert key to string
            keyStr := fmt.Sprintf("%v", key)
            keyBytes := []byte(keyStr)

            // Convert value to byte slice based on its type
            var valueBytes []byte
            switch v := value.(type) {
            case string:
                valueBytes = []byte(v) // For string values, use directly as bytes
            default:
                valueBytes, err := json.Marshal(v) // For non-string values, marshal to bytes
                if err != nil {
                    http.Error(w, "Failed to encode value", http.StatusInternalServerError)
                    return
                }
				err = db.Set(string(keyBytes), valueBytes)
				if err != nil {
					http.Error(w, "Failed to set key-value pair", http.StatusInternalServerError)
					return
				}
				w.WriteHeader(http.StatusOK)
				return
            }

            err := db.Set(string(keyBytes), valueBytes)
            if err != nil {
                http.Error(w, "Failed to set key-value pair", http.StatusInternalServerError)
                return
            }
        }

        w.WriteHeader(http.StatusOK)
    }
}

func RegisterSetHandler(mux *http.ServeMux, db *memdb.DB, wal *memdb.WAL) {
    mux.HandleFunc("/set", SetHandler(db, wal))
}