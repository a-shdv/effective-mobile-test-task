package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/joho/godotenv"
	"log"
	"os"
	"time"
)

func main() {
	// Загрузка переменных окружения
	if err := godotenv.Load(); err != nil {
		log.Fatalf("error loading env variables: %s", err.Error())
	}

	topic := os.Getenv("KAFKA_TOPIC_DST")

	// Создание нового producer
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": os.Getenv("KAFKA_ADDRESS"),
		"client.id":         os.Getenv("KAFKA_CLIENT_ID"),
		"acks":              "all"})
	if err != nil {
		fmt.Printf("failed to create producer: %s\n", err)
	}

	go func() {
		// Создание нового consumer
		consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers": os.Getenv("KAFKA_ADDRESS"),
			"group.id":          os.Getenv("KAFKA_GROUP_ID"),
			"auto.offset.reset": "smallest"})
		if err != nil {
			log.Printf("failed to create consumer: %s\n", err)
		}

		// Подписка на топик кафки
		err = consumer.Subscribe(topic, nil)
		if err != nil {
			log.Fatalf("failed to subcribe: %s\n", err.Error())
		}

		for {
			ev := consumer.Poll(100)
			switch e := ev.(type) {
			case *kafka.Message:
				log.Printf("fetched message from kafka queue: %s\n", string(e.Value))
			case *kafka.Error:
				log.Printf("error occured while consuming kafka messages: %v\n", e.Error())
			}
		}
	}()

	// Подключение к каналу для отправки сообщения
	dstChan := make(chan kafka.Event, 10000) // создание канала
	for {
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic, // название сервиса кафки
				Partition: kafka.PartitionAny,
			},
			Value: []byte("hjjhjghjghj")}, // сообщение для отправки
			dstChan, // канал, в который хотил передать сообщение
		)
		if err != nil {
			log.Fatalf("could not connect to destination channel: %s\n", err.Error())
		}
		<-dstChan
		time.Sleep(3 * time.Second)
	}
}
