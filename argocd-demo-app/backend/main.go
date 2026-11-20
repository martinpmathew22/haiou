package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	kafkaBroker = "kafka-service:9092"
	topicName   = "demo-events"
)

var (
	messages []string
	mu       sync.Mutex
	writer   *kafka.Writer
)

type KafkaStatusResponse struct {
	Message        string   `json:"message"`
	Version        string   `json:"version"`
	Host           string   `json:"host"`
	RecentMessages []string `json:"recent_messages"`
}

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
	mu.Lock()
	recent := append([]string(nil), messages...) // Create a copy of the messages slice
	mu.Unlock()

	resp := KafkaStatusResponse{
		Message:        "Hello from Go + Kafka via ArgoCD!",
		Version:        "v2.0.0",
		Host:           hostname,
		RecentMessages: recent,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func startConsumer() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		Topic:   topicName,
		GroupID: "demo-consumer-group",
	})

	defer reader.Close()

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Kafka read waiting/retrying: %v", err)
			time.Sleep(3 * time.Second) // Wait before retrying
			continue
		}

		mu.Lock()
		messages = append(messages, string(msg.Value))
		if len(messages) > 10 {
			messages = messages[1:] // Keep only the last 10 messages
		}
		mu.Unlock()
		log.Printf("Consumed from Kafka: %s", string(msg.Value))
	}
}

func publishHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload struct {
		Text string `json:"text"`
	}

	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || payload.Text == "" {
		payload.Text = fmt.Sprintf("Event triggered at %s", time.Now().Format(time.Kitchen))
	}

	err := writer.WriteMessages(r.Context(), kafka.Message{
		Key:   []byte("demo-key"),
		Value: []byte(payload.Text),
	})

	if err != nil {
		log.Printf("Failed to publish message: %v", err)
		http.Error(w, "Failed to publish to kafka: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "Published", "event": payload.Text})
}

func main() {
	writer = &kafka.Writer{
		Addr:                   kafka.TCP(kafkaBroker),
		Topic:                  topicName,
		AllowAutoTopicCreation: true,
	}
	defer writer.Close()

	go startConsumer()

	http.HandleFunc("/api/data", enableCORS(apiHandler))
	http.HandleFunc("/api/publish", enableCORS(publishHandler))
	http.HandleFunc("/healthz", healthHandler)

	log.Println("Backend server listening on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
