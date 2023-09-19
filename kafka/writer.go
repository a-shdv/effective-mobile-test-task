package kafka

import (
	"context"
	kafkago "github.com/segmentio/kafka-go"
	"os"
)

type Writer struct {
	Writer *kafkago.Writer
}

// NewKafkaWriter Создание нового writer с соответствующими конфигурациями
func NewKafkaWriter() *Writer {
	writer := &kafkago.Writer{
		Addr:  kafkago.TCP(os.Getenv("KAFKA_ADDRESS")),
		Topic: os.Getenv("KAFKA_TOPIC_FIO_FAILED"),
	}
	return &Writer{
		Writer: writer,
	}
}

// WriteMessages Запись сообщений из 1-го канала в другой канал
func (k *Writer) WriteMessages(ctx context.Context, messages chan kafkago.Message, messageCommitChan chan kafkago.Message) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case m := <-messages:
			err := k.Writer.WriteMessages(ctx, kafkago.Message{
				Value: m.Value,
			})
			if err != nil {
				return err
			}

			select {
			case <-ctx.Done():
			case messageCommitChan <- m:
			}
		}
	}
}
