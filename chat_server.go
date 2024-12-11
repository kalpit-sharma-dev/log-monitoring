package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gorilla/websocket"
)

var clients = make(map[*websocket.Conn]bool) // connected clients
var broadcast = make(chan Message)           // message channel
var producer *kafka.Producer                 // Kafka producer

// Message struct
type Message struct {
	User    string `json:"user"`
	Content string `json:"content"`
}

// WebSocket upgrader
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // allow all connections
	},
}

// Kafka Producer Setup
func createKafkaProducer() *kafka.Producer {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9000", // Kafka broker address
	})
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %s", err)
	}
	return producer
}

// Handle incoming WebSocket connections
func handleConnections(w http.ResponseWriter, r *http.Request) {
	// Upgrade the HTTP connection to a WebSocket connection
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}
	defer conn.Close()

	clients[conn] = true // add new client to the list

	for {
		var msg Message
		// Read a message from the WebSocket
		err := conn.ReadJSON(&msg)
		if err != nil {
			log.Println(err)
			delete(clients, conn) // remove client if an error occurs
			break
		}

		// Publish the message to Kafka
		err = publishToKafka(msg)
		if err != nil {
			log.Println("Failed to publish message to Kafka:", err)
		}

		// Broadcast message to all connected clients
		broadcast <- msg
	}
}

// Publish message to Kafka
func publishToKafka(msg Message) error {
	messageJSON, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	topic := "registered_user"
	// Produce the message to Kafka topic "chat-messages"
	deliveryChan := make(chan kafka.Event)
	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          messageJSON,
	}, deliveryChan)
	if err != nil {
		return err
	}

	// Wait for message delivery
	e := <-deliveryChan
	close(deliveryChan)

	// Log message delivery status
	switch ev := e.(type) {
	case *kafka.Message:
		if ev.TopicPartition.Error != nil {
			log.Printf("Failed to deliver message to Kafka: %v", ev.TopicPartition.Error)
		} else {
			log.Printf("Message delivered to Kafka: %v", ev.TopicPartition)
		}
	}
	return nil
}

// Broadcast messages to all connected clients
func handleMessages() {
	for {
		msg := <-broadcast
		// Send the message to all clients
		for client := range clients {
			err := client.WriteJSON(msg)
			if err != nil {
				log.Println(err)
				client.Close()
				delete(clients, client)
			}
		}
	}
}

func main1() {
	// Create Kafka producer
	producer = createKafkaProducer()
	defer producer.Close()

	// Handle incoming WebSocket connections
	http.HandleFunc("/ws", handleConnections)

	// Start the message broadcaster
	go handleMessages()

	// Start the server
	fmt.Println("Chat server started on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
