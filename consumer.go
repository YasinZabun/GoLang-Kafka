package main

import (
	"bufio"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"math/rand"
	"os"
	"time"
)

func consume(iteration int) {

	fmt.Printf("Consumer %d started!\n", iteration+1)
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  "localhost:9092",
		"group.id":           "foo",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "false",
		"session.timeout.ms": 6000,
	})
	if err != nil {
		fmt.Printf("Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	err = consumer.Subscribe("test", nil)
	if err != nil {
		fmt.Printf("Failed to subscribe topic: %s\n", err)
		os.Exit(1)
	}

	run := true
	for run == true {

		rand.Seed(time.Now().UnixNano())
		n := rand.Intn(6) + 5
		time.Sleep(time.Duration(n) * time.Second)

		ev := consumer.Poll(100)
		switch e := ev.(type) {
		case *kafka.Message:
			consumer.Commit()
			fmt.Printf("\nMessage is : %s and consumed by %d number thread\n", string(e.Value), iteration+1)
		case kafka.Error:
			fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			run = false
		}
	}
	consumer.Close()
}

func main() {

	for i := 0; i < 5; i++ {
		go consume(i)
	}
	reader := bufio.NewReader(os.Stdin)
	_, _, _ = reader.ReadRune()
}
