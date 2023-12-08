package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
)

type Sender struct {
	ID string `json:"id"`
}

type Message struct {
	ID      string `json:"id"`
	Content string `json:"content"`
	Number  string `json:"number"`
}

type Chat struct {
	ID string `json:"id"`
}

type NewMessageEvent struct {
	Sender  Sender  `json:"sender"`
	Message Message `json:"message"`
	Chat    Chat    `json:"chat"`
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	done := make(chan bool, 1)

	log.Printf("Start consuming Kafka messages")
	go consumeKafkaMessages(done)

	log.Printf("Start SMPP server")
	startSMPPServer()

	select {
	case <-done:
		log.Println("Program finished")
	case <-interrupt:
		log.Println("Received interrupt signal, shutting down...")
	}
}

func consumeKafkaMessages(done chan bool) {
	brokers := os.Getenv("KAFKA_BROKERS")
	topic := os.Getenv("KAFKA_TOPIC")
	groupID := os.Getenv("KAFKA_GROUP_ID")
	writerBrokers := os.Getenv("KAFKA_WRITER_BROKERS")
	writerTopic := os.Getenv("KAFKA_WRITER_TOPIC")

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{brokers},
		Topic:          topic,
		GroupID:        groupID,
		CommitInterval: time.Second,
	})
	defer r.Close()

	w := &kafka.Writer{
		Addr:                   kafka.TCP(writerBrokers),
		Topic:                  writerTopic,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
	}
	defer w.Close()

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Println("Error while trying to read message:", err)
			continue
		}

		processMessage(string(m.Value), w)
	}
}

func processMessage(message string, w *kafka.Writer) {
	log.Println("Processing SMPP message:", message)

	var newMessageEvent NewMessageEvent
	err := json.Unmarshal([]byte(message), &newMessageEvent)
	if err != nil {
		log.Println("Error unmarshalling message:", err)
		return
	}

	broadcastMessage(w, newMessageEvent)
}

func broadcastMessage(w *kafka.Writer, event NewMessageEvent) {
	bytes, err := json.Marshal(event)
	if err != nil {
		log.Println("Error marshalling message for broadcast:", err)
		return
	}

	err = w.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(event.Message.ID),
			Value: bytes,
		},
	)
	if err != nil {
		log.Println("Error broadcasting message:", err)
	}
}

func startSMPPServer() {
	log.Println("Simulated SMPP server started")
}
