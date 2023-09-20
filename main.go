package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/joho/godotenv"
	"log"
	"os"
	"time"
)

type MessageProducer struct {
	producer *kafka.Producer
	topic    string
	dstChan  chan kafka.Event
}

func NewMessageProducer(p *kafka.Producer, topic string) *MessageProducer {
	return &MessageProducer{
		producer: p,
		topic:    topic,
		dstChan:  make(chan kafka.Event, 10000),
	}
}

func (mp *MessageProducer) produceMessage(message string, size int) error {
	var (
		format  = fmt.Sprintf("%s -  %d", message, size)
		payload = []byte(format)
	)

	err := mp.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &mp.topic,
			Partition: kafka.PartitionAny,
		},
		Value: payload,
	},
		mp.dstChan)

	if err != nil {
		log.Fatalf("error while trying to produce message: %s\n", err.Error())
	}

	<-mp.dstChan
	return nil
}

func main() {
	// Загрузка переменных окружения
	if err := godotenv.Load(); err != nil {
		log.Fatalf("error loading env variables: %s", err.Error())
	}

	// Топик кафки, куда будем отправлять сообщения
	//dstChan := make(chan kafka.Event, 10000) // создание канала

	//srcTopic := os.Getenv("KAFKA_TOPIC_SRC")

	// Создание нового producer
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": os.Getenv("KAFKA_ADDRESS"),
		"client.id":         os.Getenv("KAFKA_CLIENT_ID"),
		"acks":              os.Getenv("KAFKA_ACKS")})
	if err != nil {
		fmt.Printf("failed to create producer: %s\n", err)
	}

	go func() {
		// Создание нового consumer
		consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers": os.Getenv("KAFKA_ADDRESS"),
			"group.id":          os.Getenv("KAFKA_GROUP_ID"),
			"auto.offset.reset": os.Getenv("KAFKA_AUTO_OFFSET_RESET")})
		if err != nil {
			log.Printf("failed to create consumer: %s\n", err)
		}

		// Подписка на топик кафки
		dstTopic := os.Getenv("KAFKA_TOPIC_DST")
		err = consumer.Subscribe(dstTopic, nil)
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

	// Публикация сообщейний
	mp := NewMessageProducer(p, os.Getenv("KAFKA_TOPIC_DST"))

	for i := 0; i < 1000; i++ {
		if err := mp.produceMessage("market", i+1); err != nil {
			log.Fatal(err)
		}
		time.Sleep(time.Second * 3)
	}

}
