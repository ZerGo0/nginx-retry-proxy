package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"sync"
)

type counter struct {
	mu     sync.Mutex
	counts map[string]int
}

func main() {
	requests := &counter{counts: make(map[string]int)}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /admin/count", func(w http.ResponseWriter, r *http.Request) {
		requests.mu.Lock()
		count := requests.counts[r.URL.Query().Get("id")]
		requests.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]int{"count": count})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read body", http.StatusBadRequest)
			return
		}
		requestID := r.Header.Get("X-Test-ID")
		requests.mu.Lock()
		requests.counts[requestID]++
		requests.mu.Unlock()

		switch r.URL.Path {
		case "/application-500":
			http.Error(w, "application failure", http.StatusInternalServerError)
		case "/ambiguous":
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				http.Error(w, "hijacking unsupported", http.StatusInternalServerError)
				return
			}
			connection, _, err := hijacker.Hijack()
			if err != nil {
				return
			}
			_ = connection.Close()
		default:
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write(body)
		}
	})

	server := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 0}
	log.Fatal(server.ListenAndServe())
}
