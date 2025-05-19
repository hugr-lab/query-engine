package main

import (
	"encoding/json"
	"net/http"
	"strconv"
)

// Test http service that exposes a REST API Function (GET)

func main() {
	// Create a new HTTP server
	http.HandleFunc("/test_get", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		vals := r.URL.Query()
		name := vals.Get("name")
		val, _ := strconv.Atoi(vals.Get("val"))
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{\"name\": \"" + name + "\", \"val\": " + strconv.Itoa(val) + "}"))
	})

	// Handle POST requests
	http.HandleFunc("/test_post", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var data struct {
			Name string `json:"name"`
			Val  int    `json:"val"`
		}
		if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{\"name\": \"" + data.Name + "\", \"val\": " + strconv.Itoa(data.Val) + "}"))
	})

	// Start the server
	if err := http.ListenAndServe(":17000", nil); err != nil {
		panic(err)
	}
}
