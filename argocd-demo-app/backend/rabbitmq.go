package main

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

type RabbitClient struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	queue   string
	mu      sync.RWMutex
	rdb     *redis.Client
}

func NewRabbitClient(rdb *redis.Client) (*RabbitClient, error) {
	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		url = "amqp://guest:guest@rabbitmq-service:5672/"
	}

	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	q, err := ch.QueueDeclare(
		"events_queue",
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, err
	}

	return &RabbitClient{
		conn:    conn,
		channel: ch,
		queue:   q.Name,
		rdb:     rdb,
	}, nil
}

func (r *RabbitClient) Publish(ctx context.Context, body string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	return r.channel.PublishWithContext(
		ctx,
		"",
		r.queue,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(body),
		},
	)
}

func (r *RabbitClient) StartConsumer() {
	msgs, err := r.channel.Consume(
		r.queue,
		"",
		true, // auto-ack
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Printf("RabbitMQ consumer error: %v", err)
		return
	}

	go func() {
		for d := range msgs {
			msg := string(d.Body)
			log.Printf("[RabbitMQ Consumer] Received: %s", msg)
			eventsConsumed.WithLabelValues("rabbitmq").Inc()

			if r.rdb != nil {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				if err := r.rdb.LPush(ctx, "rabbit_messages", msg).Err(); err != nil {
					log.Printf("Redis push error (rabbit_messages): %v", err)
				} else {
					r.rdb.LTrim(ctx, "rabbit_messages", 0, 9)
				}
				cancel()
			}
		}
	}()
}

func (r *RabbitClient) Close() {
	if r.channel != nil {
		r.channel.Close()
	}
	if r.conn != nil {
		r.conn.Close()
	}
}
