package kafka

import (
	"context"
	"github.com/pkg/errors"
	kafkago "github.com/segmentio/kafka-go"
	"log"
	"os"
)

type Reader struct {
	Reader *kafkago.Reader
}

// NewKafkaReader Создание нового reader с соответствующими конфигурациями
func NewKafkaReader() *Reader {
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: []string{os.Getenv("KAFKA_ADDRESS")},
		Topic:   os.Getenv("KAFKA_TOPIC_FIO"),
		GroupID: os.Getenv("KAFKA_GROUP_ID"),
	})

	return &Reader{
		Reader: reader,
	}
}

// FetchMessage Получение сообщений из контекста
func (k *Reader) FetchMessage(ctx context.Context, messages chan<- kafkago.Message) error {
	for {
		message, err := k.Reader.FetchMessage(ctx)
		if err != nil {
			log.Fatalf("error fetching messages: %s", err.Error())
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case messages <- message:
			log.Printf("message fetched and sent to a channel: %v \n", string(message.Value))
		}
	}
}

// CommitMessages Фиксация сообщений
func (k *Reader) CommitMessages(ctx context.Context, messageCommitChan <-chan kafkago.Message) error {
	for {
		select {
		case <-ctx.Done():
		case msg := <-messageCommitChan:
			err := k.Reader.CommitMessages(ctx, msg)
			if err != nil {
				return errors.Wrap(err, "Reader.CommitMessages")
			}
			log.Printf("committed a msg: %v \n", string(msg.Value))
		}
	}
}
