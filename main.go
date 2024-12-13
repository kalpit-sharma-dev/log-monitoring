package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Employee struct {
	ID           int
	Name         string
	Age          int
	Sex          string
	Department   string
	Organization string
	Salary       float64
}

// GenerateRandomEmployee creates a random employee record
func GenerateRandomEmployee(id int) Employee {
	names := []string{
		"Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Hank", "Ivy", "Jack",
		"Karen", "Liam", "Mia", "Noah", "Olivia", "Paul", "Quinn", "Rita", "Sam", "Tina",
		"Ursula", "Victor", "Wendy", "Xander", "Yara", "Zane", "Aaron", "Bella", "Cody",
		"Diana", "Ethan", "Fiona", "George", "Holly", "Isla", "James", "Katie", "Luke",
		"Monica", "Nathan", "Owen", "Penelope", "Quincy", "Riley", "Sophia", "Tyler",
		"Uma", "Vince", "Walter", "Ximena", "Yvonne", "Zachary", "Adam", "Beatrice", "Carlos",
		"Danielle", "Edward", "Felix", "Gabriella", "Harrison", "Isabel", "Javier", "Katherine",
		"Louis", "Maria", "Neil", "Olga", "Patricia", "Quinton", "Rebecca", "Steve", "Tracy",
		"Ulysses", "Valerie", "William", "Xander", "Yasmine", "Zara", "Aidan", "Brianna",
		"Chloe", "Daniel", "Eva", "Freddie", "Gemma", "Hugo", "Irene", "Julia", "Ken", "Lilly",
		"Monroe", "Nancy", "Oscar", "Paula", "Quinn", "Rachel", "Sharon", "Toby", "Ursula",
		"Victor", "Willa", "Xenia", "Yara", "Zoe", "Arthur", "Brandon", "Christina",
		"Devin", "Ella", "Finn", "Grace", "Holly", "Ian", "Jackie", "Kevin", "Lorenzo",
		"Madeline", "Nina", "Orlando", "Peter", "Quinn", "Reagan", "Stanley", "Tristan",
		"Ursula", "Vera", "Wyatt", "Xander", "Yvonne", "Zane",
	}

	departments := []string{"Engineering", "Sales", "Marketing", "Human Resources", "Finance", "Operations"}
	organizations := []string{"TechCorp", "HealthCare Inc.", "EduNation", "AutoMotive Ltd.", "Retail Co."}
	sexOptions := []string{"Male", "Female", "Other"}

	rand.Seed(time.Now().UnixNano())
	name := names[rand.Intn(len(names))]
	age := rand.Intn(42) + 18 // Random age between 18 and 60
	sex := sexOptions[rand.Intn(len(sexOptions))]
	department := departments[rand.Intn(len(departments))]
	organization := organizations[rand.Intn(len(organizations))]
	salary := rand.Float64()*50000 + 30000 // Random salary between 30,000 and 80,000

	return Employee{
		ID:           id,
		Name:         name,
		Age:          age,
		Sex:          sex,
		Department:   department,
		Organization: organization,
		Salary:       salary,
	}
}

func main00() {
	// Create a Kafka producer
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		log.Fatalf("Failed to create producer: %s\n", err)
	}
	defer producer.Close()

	// Set up a delivery report handler
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Message delivered to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	// Produce messages to the Kafka topic
	topic := "registered_user"
	for i := 1; i <= 100000; i++ {
		employee := GenerateRandomEmployee(i)

		employeeJSON, err := json.Marshal(employee)
		if err != nil {
			fmt.Println("Error marshaling employee to JSON:", err)
			continue
		}
		message := string(employeeJSON)

		//message := fmt.Sprintf("Message number %d", i)
		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(message),
		}, nil)

		if err != nil {
			fmt.Printf("Error producing message: %v\n", err)
		} else {
			fmt.Printf("Produced message: %s\n", message)
		}
		//time.Sleep(1 * time.Second) // Optional: To slow down message production
	}

	// Wait for all messages to be delivered
	producer.Flush(15 * 1000)
}
