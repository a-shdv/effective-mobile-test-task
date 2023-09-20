package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/joho/godotenv"
	"log"
	"os"
	"time"
)

type Producer struct {
	producer *kafka.Producer
	topic    string
	dstChan  chan kafka.Event
}

type Consumer struct {
	consumer *kafka.Consumer
	topic    string
	srcChan  chan kafka.Event
}

func NewConsumer(c *kafka.Consumer, topic string) *Consumer {
	return &Consumer{
		consumer: c,
		topic:    topic,
		srcChan:  make(chan kafka.Event, 10000),
	}
}

func NewProducer(p *kafka.Producer, topic string) *Producer {
	return &Producer{
		producer: p,
		topic:    topic,
		dstChan:  make(chan kafka.Event, 10000),
	}
}

func (mp *Producer) produceMessage(message string, size int) error {
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
	log.Printf("message has been produced: %s\n", format)
	return nil
}

func main() {
	// Загрузка переменных окружения
	if err := godotenv.Load(); err != nil {
		log.Fatalf("error loading env variables: %s", err.Error())
	}

	//Создание нового producer
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": os.Getenv("KAFKA_ADDRESS"),
		"client.id":         os.Getenv("KAFKA_CLIENT_ID"),
		"acks":              os.Getenv("KAFKA_ACKS")})
	if err != nil {
		fmt.Printf("failed to create producer: %s\n", err)
	}

	// Создание нового consumer
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": os.Getenv("KAFKA_ADDRESS"),
		"group.id":          os.Getenv("KAFKA_GROUP_ID"),
		"auto.offset.reset": os.Getenv("KAFKA_AUTO_OFFSET_RESET")})
	if err != nil {
		log.Printf("failed to create consumer: %s\n", err)
	}

	// Подписка на топик кафки
	srcTopic := os.Getenv("KAFKA_TOPIC_SRC")
	err = consumer.Subscribe(srcTopic, nil)
	if err != nil {
		log.Fatalf("failed to subcribe: %s\n", err.Error())
	}

	var data []byte
	go func() {
		for {
			ev := consumer.Poll(100)
			switch msg := ev.(type) {
			case *kafka.Message:
				log.Printf("fetched message from kafka queue: %s\n", string(msg.Value))
				data = msg.Value
				break
			case *kafka.Error:
				log.Printf("error occured while consuming kafka messages: %v\n", msg.Error())
			}
		}
	}()

	// Публикация сообщейний
	mp := NewProducer(producer, os.Getenv("KAFKA_TOPIC_DST"))

	fmt.Printf("%s\n", string(data))

	if len(data) > 0 {
		if err := mp.produceMessage(string(data), 1); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("check: %s\n", data)
	}

	// Создание нового consumer

	// subscribe to target topic
	//err = c.Subscribe(srcTopic, nil)
	//if err != nil {
	//	log.Fatalf("could not subscribe topic: %s\n", err.Error())
	//}

	time.Sleep(time.Second * 30)
}
