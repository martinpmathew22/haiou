package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
)

type StatusResponse struct {
	Message string `json:"message"`
	Version string `json:"version"`
	Host    string `json:"host"`
}

func enableCORS(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next(w, r)
	}
}

func apiHandler(w http.ResponseWriter, r *http.Request) {
	hostname, _ := os.Hostname()
	resp := StatusResponse{
		Message: "Hello from Go backend deployed via ArgoCD!",
		Version: "v1.0.0",
		Host:    hostname,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func main() {
	http.HandleFunc("/api/data", enableCORS(apiHandler))
	http.HandleFunc("/healthz", healthHandler)

	log.Println("Backend server listening on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}