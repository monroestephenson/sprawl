package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

func main() {
	port := 9090

	// Set up enhanced logging
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	log.Printf("Starting test server on port %d", port)

	// Register handler
	http.HandleFunc("/subscribe", handleSubscribe)

	// Add a simple health endpoint
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]string{
			"status": "ok",
			"time":   time.Now().String(),
		}); err != nil {
			log.Printf("Error encoding health response: %v", err)
		}
	})

	// Start server with a sensible timeout
	server := &http.Server{
		Addr:              fmt.Sprintf("0.0.0.0:%d", port),
		Handler:           http.DefaultServeMux,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
		ReadHeaderTimeout: 2 * time.Second,
	}

	log.Printf("Server starting on http://localhost:%d", port)

	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

func handleSubscribe(w http.ResponseWriter, r *http.Request) {
	log.Printf("Received subscribe request from %s, method: %s", r.RemoteAddr, r.Method)

	// Log request headers
	log.Printf("Headers: %+v", r.Header)

	// Log request body size
	log.Printf("Content length: %d", r.ContentLength)

	defer func() {
		if r := recover(); r != nil {
			log.Printf("PANIC in handler: %v", r)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
		}
	}()

	if r.Method != http.MethodPost {
		log.Printf("Method not allowed: %s", r.Method)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var sub struct {
		ID     string   `json:"id"`
		Topics []string `json:"topics"`
	}

	// Read request body into buffer for logging
	bodyBytes := make([]byte, r.ContentLength)
	_, err := r.Body.Read(bodyBytes)
	if err != nil {
		log.Printf("Error reading body: %v", err)
	}
	log.Printf("Request body: %s", string(bodyBytes))

	// Parse JSON
	if err := json.Unmarshal(bodyBytes, &sub); err != nil {
		log.Printf("Invalid request body: %v - Body: %s", err, string(bodyBytes))
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	log.Printf("Decoded subscribe request: ID=%s, Topics=%v", sub.ID, sub.Topics)

	// Send successful response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	response := map[string]string{
		"status": "subscribed",
		"id":     sub.ID,
	}

	log.Printf("Sending response: %+v", response)

	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Failed to encode subscribe response: %v", err)
	} else {
		log.Printf("Successfully sent subscribe response")
	}
}
