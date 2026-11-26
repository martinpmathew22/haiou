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

type contextKey string

const userContextKey contextKey = "user"

type Config struct {
	Port              string
	KeycloakIssuerURL string
	KafkaBroker       string
	KafkaTopic        string
	RedisAddr         string
}

type App struct {
	verifier     *oidc.IDTokenVerifier
	rdb          *redis.Client
	kafkaWriter  *kafka.Writer
	rabbitClient *RabbitClient
}

var (
	eventsPublished = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "app_events_published_total",
			Help: "Total number of events published by broker",
		},
		[]string{"broker"},
	)

	eventsConsumed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "app_events_consumed_total",
			Help: "Total number of events consumed by broker",
		},
		[]string{"broker"},
	)
)

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func loadConfig() Config {
	return Config{
		Port:              getEnv("PORT", "8080"),
		KeycloakIssuerURL: getEnv("KEYCLOAK_ISSUER_URL", "http://keycloak.local:8080/realms/argocd-demo"),
		KafkaBroker:       getEnv("KAFKA_BROKER", "my-cluster-kafka-bootstrap:9092"),
		KafkaTopic:        getEnv("KAFKA_TOPIC", "demo-events"),
		RedisAddr:         getEnv("REDIS_ADDR", "redis-service:6379"),
	}
}

func initOIDCVerifier(ctx context.Context, issuerURL string, maxAttempts int, delay time.Duration) (*oidc.IDTokenVerifier, error) {
	var provider *oidc.Provider
	var err error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		provider, err = oidc.NewProvider(ctx, issuerURL)
		if err == nil {
			log.Printf("Connected to Keycloak OIDC provider at %s", issuerURL)
			return provider.Verifier(&oidc.Config{SkipClientIDCheck: true}), nil
		}
		log.Printf("Keycloak OIDC provider not ready (attempt %d/%d): %v", attempt, maxAttempts, err)
		time.Sleep(delay)
	}

	return nil, fmt.Errorf("failed to initialize OIDC provider after %d attempts: %w", maxAttempts, err)
}

func initKafkaWriter(broker, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Topic:                  topic,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
	}
}

func connectRabbitWithRetry(maxAttempts int, delay time.Duration) *RabbitClient {
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		client, err := NewRabbitClient()
		if err == nil {
			log.Printf("Connected to RabbitMQ on attempt %d", attempt)
			client.StartConsumer()
			return client
		}
		log.Printf("RabbitMQ not ready (attempt %d/%d): %v — retrying in %s", attempt, maxAttempts, err, delay)
		time.Sleep(delay)
	}

	log.Println("Warning: proceeding without RabbitMQ after max retry attempts")
	return nil
}

func startKafkaConsumer(brokers []string, topic string, rdb *redis.Client) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  "backend-consumer-group",
		MinBytes: 1,
		MaxBytes: 10e6,
	})

	go func() {
		defer reader.Close()
		for {
			msg, err := reader.ReadMessage(context.Background())
			if err != nil {
				log.Printf("Kafka consumer read error: %v", err)
				time.Sleep(2 * time.Second)
				continue
			}

			val := string(msg.Value)
			log.Printf("[Kafka Consumer] Received: %s", val)
			eventsConsumed.WithLabelValues("kafka").Inc()

			saveRecentMessage(rdb, val)
		}
	}()
}

func saveRecentMessage(rdb *redis.Client, msg string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := rdb.LPush(ctx, "recent_messages", msg).Err(); err != nil {
		log.Printf("Redis push error: %v", err)
		return
	}
	rdb.LTrim(ctx, "recent_messages", 0, 9)
}

func (a *App) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
			http.Error(w, "Missing or invalid Authorization header", http.StatusUnauthorized)
			return
		}

		rawToken := strings.TrimPrefix(authHeader, "Bearer ")
		idToken, err := a.verifier.Verify(r.Context(), rawToken)
		if err != nil {
			log.Printf("Token verification failed: %v", err)
			http.Error(w, "Invalid or expired token", http.StatusUnauthorized)
			return
		}

		var claims struct {
			PreferredUsername string `json:"preferred_username"`
			Email             string `json:"email"`
		}
		if err := idToken.Claims(&claims); err != nil {
			http.Error(w, "Failed to parse token claims", http.StatusUnauthorized)
			return
		}

		username := claims.PreferredUsername
		if username == "" {
			username = claims.Email
		}

		ctxWithUser := context.WithValue(r.Context(), userContextKey, username)
		next.ServeHTTP(w, r.WithContext(ctxWithUser))
	})
}

func handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func (a *App) handlePublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload struct {
		Text string `json:"text"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || strings.TrimSpace(payload.Text) == "" {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	user, _ := r.Context().Value(userContextKey).(string)
	enrichedMsg := payload.Text + " (by " + user + ")"

	if err := a.publishToBrokers(r.Context(), enrichedMsg); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]string{
		"status": "published",
		"event":  enrichedMsg,
	})
}

func (a *App) publishToBrokers(ctx context.Context, msg string) error {
	if err := a.kafkaWriter.WriteMessages(ctx, kafka.Message{Value: []byte(msg)}); err != nil {
		log.Printf("Kafka publish error: %v", err)
		return fmt.Errorf("failed to publish to Kafka")
	}
	eventsPublished.WithLabelValues("kafka").Inc()

	if a.rabbitClient != nil {
		if err := a.rabbitClient.Publish(ctx, msg); err != nil {
			log.Printf("RabbitMQ publish error: %v", err)
			return fmt.Errorf("failed to publish to RabbitMQ")
		}
		eventsPublished.WithLabelValues("rabbitmq").Inc()
		eventsConsumed.WithLabelValues("rabbitmq").Inc()
	}

	return nil
}

func (a *App) handleData(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	hostname, _ := os.Hostname()
	user, _ := r.Context().Value(userContextKey).(string)

	recentMsgs, err := a.rdb.LRange(r.Context(), "recent_messages", 0, 9).Result()
	if err != nil {
		log.Printf("Redis read error: %v", err)
		recentMsgs = []string{}
	}

	rabbitMsgs := []string{}
	if a.rabbitClient != nil {
		rabbitMsgs = a.rabbitClient.GetRecentMessages()
	}

	writeJSON(w, map[string]any{
		"user":            user,
		"host":            hostname,
		"recent_messages": recentMsgs,
		"rabbit_messages": rabbitMsgs,
	})
}

func writeJSON(w http.ResponseWriter, data any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func (a *App) newRouter() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", handleHealthz)
	mux.Handle("/metrics", promhttp.Handler())
	mux.Handle("/api/publish", a.authMiddleware(http.HandlerFunc(a.handlePublish)))
	mux.Handle("/api/data", a.authMiddleware(http.HandlerFunc(a.handleData)))
	return mux
}

func main() {
	ctx := context.Background()
	cfg := loadConfig()

	verifier, err := initOIDCVerifier(ctx, cfg.KeycloakIssuerURL, 10, 3*time.Second)
	if err != nil {
		log.Fatal(err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rdb.Close()

	kafkaWriter := initKafkaWriter(cfg.KafkaBroker, cfg.KafkaTopic)
	defer kafkaWriter.Close()
	startKafkaConsumer([]string{cfg.KafkaBroker}, cfg.KafkaTopic, rdb)

	rabbitClient := connectRabbitWithRetry(10, 3*time.Second)
	if rabbitClient != nil {
		defer rabbitClient.Close()
	}

	app := &App{
		verifier:     verifier,
		rdb:          rdb,
		kafkaWriter:  kafkaWriter,
		rabbitClient: rabbitClient,
	}

	server := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      app.newRouter(),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	log.Printf("Backend listening on :%s", cfg.Port)
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
