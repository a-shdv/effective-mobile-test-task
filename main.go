package main

import (
	"apache-kafka-golang/kafka"
	"context"
	"github.com/joho/godotenv"
	kafkago "github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
	"log"
)

func main() {
	// Загрузка переменных окружения
	if err := godotenv.Load(); err != nil {
		log.Fatalf("error loading env variables: %s", err.Error())
	}

	// Создание объектов reader, writer
	reader := kafka.NewKafkaReader()
	writer := kafka.NewKafkaWriter()

	// Инициализация каналов
	ctx := context.Background()
	messages := make(chan kafkago.Message, 1000)
	messageCommitChan := make(chan kafkago.Message, 1000)

	//  Инициализация группы горутин 'g' соответствующим контекстом 'ctx'
	g, ctx := errgroup.WithContext(ctx)

	// Получение сообщений с канала
	g.Go(func() error {
		return reader.FetchMessage(ctx, messages)
	})

	// Запись сообщений в другой канал
	g.Go(func() error {
		return writer.WriteMessages(ctx, messages, messageCommitChan)
	})

	// Фиксация сообщений - в противном случае сообщения отправятся в другой канал еще раз
	g.Go(func() error {
		return reader.CommitMessages(ctx, messageCommitChan)
	})

	// Блокирующая операция
	err := g.Wait()
	if err != nil {
		log.Fatalln(err)
	}
}
