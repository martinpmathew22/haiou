package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

const (
	kafkaBroker = "kafka-service:9092"
	redisAddr   = "redis-service:6379"
	topicName   = "demo-events"
	redisKey    = "consumed_events"
)

var (
	// messages []string
	// mu       sync.Mutex
	writer   *kafka.Writer
	rdb      *redis.Client
	verifier *oidc.IDTokenVerifier

	eventsPublished = promauto.NewCounter(prometheus.CounterOpts{
		Name: "demo_kafka_events_published_total",
		Help: "Total number of events published to Kafka",
	})

	eventsConsumed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "demo_kafka_events_consumed_total",
		Help: "Total number of events consumed from Kafka by pod and partition",
	}, []string{"pod", "partition"})

	publishDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "demo_kafka_publish_duration_seconds",
		Help:    "Latency of publishing messages to Kafka",
		Buckets: prometheus.DefBuckets,
	})
)

type KafkaStatusResponse struct {
	Message        string   `json:"message"`
	Version        string   `json:"version"`
	Host           string   `json:"host"`
	User           string   `json:"user"`
	RecentMessages []string `json:"recent_messages"`
}

type StatusResponse struct {
	Message string `json:"message"`
	Version string `json:"version"`
	Host    string `json:"host"`
}

// authMiddleware validates the Bearer token issued by Keycloak
func authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if !strings.HasPrefix(authHeader, "Bearer ") {
			http.Error(w, "Missing or invalid Authorization header", http.StatusUnauthorized)
			return
		}

		rawToken := strings.TrimPrefix(authHeader, "Bearer ")
		token, err := verifier.Verify(r.Context(), rawToken)
		if err != nil {
			http.Error(w, "Invalid token: "+err.Error(), http.StatusUnauthorized)
			return
		}

		var claims struct {
			PreferredUsername string `json:"preferred_username"`
		}

		_ = token.Claims(&claims)
		r.Header.Set("X-Authenticated-User", claims.PreferredUsername)

		next(w, r)
	}
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
	recent, _ := rdb.LRange(r.Context(), redisKey, 0, 20).Result()
	// mu.Lock()
	// recent := append([]string(nil), messages...) // Create a copy of the messages slice
	// mu.Unlock()

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

func startConsumer(hostname string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{kafkaBroker},
		Topic:       topicName,
		GroupID:     "demo-consumer-group",
		StartOffset: kafka.FirstOffset,
		MaxWait:     500 * time.Millisecond,
	})

	defer reader.Close()

	ctx := context.Background()
	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Printf("Kafka read waiting/retrying: %v", err)
			time.Sleep(3 * time.Second) // Wait before retrying
			continue
		}

		// mu.Lock()
		// messages = append(messages, string(msg.Value))
		// if len(messages) > 10 {
		// 	messages = messages[1:] // Keep only the last 10 messages
		// }
		// mu.Unlock()

		// Increment consumed counter with pod and partition labels
		eventsConsumed.WithLabelValues(hostname, fmt.Sprintf("%d", msg.Partition)).Inc()

		entry := fmt.Sprintf("[P%d | %s] %s", msg.Partition, hostname, string(msg.Value))
		rdb.LPush(ctx, redisKey, entry)
		rdb.LTrim(ctx, redisKey, 0, 20) // Keep only the last 20 messages
		log.Printf("Consumed: %s", entry)
		log.Printf("Consumed from Kafka: %s", string(msg.Value))
	}
}

func publishHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	start := time.Now()
	var payload struct {
		Text string `json:"text"`
	}

	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || payload.Text == "" {
		payload.Text = fmt.Sprintf("Event triggered at %s", time.Now().Format(time.Kitchen))
	}

	err := writer.WriteMessages(r.Context(), kafka.Message{
		// Key:   []byte("demo-key"),
		Value: []byte(payload.Text),
	})

	if err != nil {
		log.Printf("Failed to publish message: %v", err)
		http.Error(w, "Failed to publish to kafka: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Record metrics
	eventsPublished.Inc()
	publishDuration.Observe(time.Since(start).Seconds())

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "Published", "event": payload.Text})
}

func main() {
	hostname, _ := os.Hostname()

	issuerURL := os.Getenv("KEYCLOAK_ISSUER_URL") // e.g. http://192.168.0.5:8080/realms/argocd-demo
	provider, err := oidc.NewProvider(context.Background(), issuerURL)
	if err != nil {
		log.Fatalf("Failed to connect to Keycloak OIDC provider (%s): %v", issuerURL, err)
	}

	// SkipClientIDCheck allows verifying Keycloak access tokens whose 'aud' is "account"
	verifier = provider.Verifier(&oidc.Config{SkipClientIDCheck: true})

	rdb = redis.NewClient(&redis.Options{Addr: redisAddr})

	writer = &kafka.Writer{
		Addr:                   kafka.TCP(kafkaBroker),
		Topic:                  topicName,
		Balancer:               &kafka.RoundRobin{},
		AllowAutoTopicCreation: true,
		BatchTimeout:           10 * time.Millisecond,
	}
	defer writer.Close()

	go startConsumer(hostname)

	http.HandleFunc("/api/data", authMiddleware(apiHandler))
	http.HandleFunc("/api/publish", authMiddleware(publishHandler))
	http.HandleFunc("/healthz", healthHandler)

	// Expose Prometheus metrics (includes Go runtime goroutines, memory, GC + custom metrics)
	http.Handle("/metrics", promhttp.Handler())

	log.Println("Backend server listening on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
