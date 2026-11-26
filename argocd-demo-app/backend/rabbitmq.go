package main

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitClient struct {
	conn     *amqp.Connection
	channel  *amqp.Channel
	queue    string
	mu       sync.RWMutex
	messages []string
}

func NewRabbitClient() (*RabbitClient, error) {
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

	return &RabbitClient{conn: conn, channel: ch, queue: q.Name}, nil
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

			r.mu.Lock()
			r.messages = append([]string{msg}, r.messages...)
			if len(r.messages) > 10 {
				r.messages = r.messages[:10]
			}
			r.mu.Unlock()
		}
	}()
}

func (r *RabbitClient) GetRecentMessages() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]string, len(r.messages))
	copy(result, r.messages)
	return result
}

func (r *RabbitClient) Close() {
	if r.channel != nil {
		r.channel.Close()
	}
	if r.conn != nil {
		r.conn.Close()
	}
}
