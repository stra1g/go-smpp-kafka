package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
)

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

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{brokers},
		Topic:          topic,
		GroupID:        groupID,
		CommitInterval: time.Second,
	})
	defer r.Close()

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Println("Error while trying to read message:", err)
			continue
		}

		processMessage(string(m.Value))
	}
	// done <- true
}

func processMessage(message string) {
	log.Println("Processing SMPP message:", message)
}

func startSMPPServer() {
	log.Println("Simulated SMPP server started")
}
