package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"math/rand"
	"os"
	"time"
)

func produce() {
	println("Producer started!")
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"acks":              "all"})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}
	deliveryChan := make(chan kafka.Event, 10000)
	topic := "test"
	for true {
		time.Sleep(1 * time.Second)
		value := time.Now().String() + " Hello world!"
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: int32(rand.Intn(5))},
			Value:          []byte(value)},
			deliveryChan,
		)
		if err != nil {
			fmt.Printf("Failed to produce : %s\n", err)
			continue
		}
		e := <-deliveryChan
		m := e.(*kafka.Message)

		if m.TopicPartition.Error != nil {
			fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
		} else {
			fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
				*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
		}
	}
	close(deliveryChan)
}
func main() {
	produce()
}
