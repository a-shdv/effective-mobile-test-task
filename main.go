package main

import (
	"apache-kafka-golang/kafka"
	"apache-kafka-golang/model"
	"context"
	"github.com/joho/godotenv"
	kafkago "github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
	"log"
)

var urls []string

func main() {
	var helper model.Helper
	urls = []string{
		"https://api.agify.io/?name=Dmitriy",
		"https://api.genderize.io/?name=Dmitriy",
		"https://api.nationalize.io/?name=Dmitriy",
	}

	// Загрузка переменных окружения
	if err := godotenv.Load(); err != nil {
		log.Fatalf("error loading env variables: %s", err.Error())
	}

	// Создание объектов reader, writer
	reader := kafka.NewKafkaReader()
	writer := kafka.NewKafkaWriter()

	// Инициализация каналов
	ctx := context.Background()
	kafkaFromChanMessages := make(chan kafkago.Message, 1e3)
	kafkaToChanMessages := make(chan kafkago.Message, 1e3)

	//  Инициализация группы горутин 'g' соответствующим контекстом 'ctx'
	g, ctx := errgroup.WithContext(ctx)

	// Получение сообщений с канала
	g.Go(func() error {
		return reader.FetchMessage(ctx, kafkaFromChanMessages)
	})

	person := model.Person{}
	errJsonData := helper.ParseAndStoreJsonData(urls, &person)
	errKafkaMessages := helper.StoreKafkaMessages(kafkaFromChanMessages, &person)

	if errJsonData != nil || errKafkaMessages != nil {
		// Запись сообщений в другой канал
		g.Go(func() error {
			return writer.WriteMessages(ctx, kafkaFromChanMessages, kafkaToChanMessages)
		})

		// Фиксация сообщений - в противном случае сообщения отправятся в другой канал еще раз
		g.Go(func() error {
			return reader.CommitMessages(ctx, kafkaToChanMessages)
		})
	}

	// Блокирующая операция
	err := g.Wait()
	if err != nil {
		log.Fatalln(err)
	}
}
