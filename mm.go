package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	_ "github.com/go-sql-driver/mysql"
)

// Configuration for Kafka and MySQL
var (
	kafkaBrokers    = "localhost:9092" // Replace with your Kafka broker addresses
	topic           = "logs"
	heartbeatTopic  = "heartbeat"                                    // Topic for heartbeat messages
	dbConnectionStr = "kalpit:password@tcp(localhost:3306)/pipeline" // Replace with your MySQL connection string
)

func main() {
	fmt.Println("Starting Kafka Producer...")

	// Start Kafka Producer
	go startKafkaProducer()

	// Start Kafka Heartbeat
	go startHeartbeatProducer()

	// HTTP Server for Monitoring and Data Insertion
	startHTTPServer()
}

func startKafkaProducer() {
	producer, err := ckafka.NewProducer(&ckafka.ConfigMap{"bootstrap.servers": kafkaBrokers})
	if err != nil {
		log.Fatalf("Error creating Kafka producer: %v", err)
	}
	defer producer.Close()

	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *ckafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("Delivery failed: %v", ev.TopicPartition)
				} else {
					log.Printf("Delivered message to %v", ev.TopicPartition)
				}
			}
		}
	}()

	for {
		// Example log message in JSON format
		message := fmt.Sprintf(`{"timestamp": "%s", "level": "INFO", "message": "Log message generated at %s"}`, time.Now().Format(time.RFC3339), time.Now().Format(time.RFC3339))
		err = producer.Produce(&ckafka.Message{
			TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: ckafka.PartitionAny},
			Value:          []byte(message),
		}, nil)

		if err != nil {
			log.Printf("Failed to produce message: %v", err)
		}

		// Simulate log generation interval
		time.Sleep(2 * time.Second)
	}

	// Wait for all messages to be delivered
	producer.Flush(15 * 1000)
}

func startHeartbeatProducer() {
	producer, err := ckafka.NewProducer(&ckafka.ConfigMap{"bootstrap.servers": kafkaBrokers})
	if err != nil {
		log.Fatalf("Error creating Kafka heartbeat producer: %v", err)
	}
	defer producer.Close()

	for {
		heartbeatMessage := fmt.Sprintf(`{"timestamp": "%s", "status": "alive"}`, time.Now().Format(time.RFC3339))
		err = producer.Produce(&ckafka.Message{
			TopicPartition: ckafka.TopicPartition{Topic: &heartbeatTopic, Partition: ckafka.PartitionAny},
			Value:          []byte(heartbeatMessage),
		}, nil)

		if err != nil {
			log.Printf("Failed to produce heartbeat message: %v", err)
		}

		// Send heartbeat every 1 second
		time.Sleep(1 * time.Second)
	}
}

func startHTTPServer() {
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Producer is healthy!"))
	})

	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		// Add custom metrics handling logic here
		w.Write([]byte("Metrics endpoint is under construction."))
	})

	http.HandleFunc("/addData", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
			return
		}

		var data map[string]string
		if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		if err := saveDataToMySQL(data); err != nil {
			log.Println(err)
			http.Error(w, "Failed to save data to MySQL", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Data saved successfully!"))
	})

	port := os.Getenv("HTTP_PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Starting HTTP server on port %s", port)
	if err := http.ListenAndServe(fmt.Sprintf(":%s", port), nil); err != nil {
		log.Fatalf("Error starting HTTP server: %v", err)
	}
}

func saveDataToMySQL(data map[string]string) error {
	db, err := sql.Open("mysql", dbConnectionStr)
	if err != nil {
		return fmt.Errorf("error connecting to MySQL: %v", err)
	}
	defer db.Close()

	query := "INSERT INTO logs (log_key, value) VALUES (?, ?)"
	for key, value := range data {
		if _, err := db.Exec(query, key, value); err != nil {
			return fmt.Errorf("error inserting data into MySQL: %v", err)
		}
	}
	return nil
}
