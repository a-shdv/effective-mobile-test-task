package main

import (
	"apache-kafka-golang/kafka"
	"apache-kafka-golang/model"
	"context"
	"encoding/json"
	"github.com/joho/godotenv"
	kafkago "github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
	"io"
	"log"
	"net/http"
	"time"
)

var client *http.Client
var person model.Person
var urls []string

func main() {
	client = &http.Client{Timeout: 5 * time.Second}
	urls = []string{
		"https://api.agify.io/?name=Dmitriy",
		"https://api.genderize.io/?name=Dmitriy",
		"https://api.nationalize.io/?name=Dmitriy", // TODO stucture creation from field country
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
	messages := make(chan kafkago.Message, 1e3)
	messageCommitChan := make(chan kafkago.Message, 1e6)

	//  Инициализация группы горутин 'g' соответствующим контекстом 'ctx'
	g, ctx := errgroup.WithContext(ctx)

	// Получение сообщений с канала
	g.Go(func() error {
		return reader.FetchMessage(ctx, messages)
	})

	// Person
	getJsonFromKafka(messages)
	getJsonFromUrls(urls)

	log.Printf("person: %+v\n", person)

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

func getJsonFromKafka(messages chan kafkago.Message) {
	for msg := range messages {
		var err error

		if err != nil {
			log.Fatal(err)
		}

		err = json.Unmarshal(msg.Value, &person)
		if err != nil {
			log.Fatalf("error while unmarshal: %s\n", err.Error())
		}

		break
	}
}

func getJsonFromUrls(urls []string) {
	for _, url := range urls {
		err := decodeJsonToStruct(url, &person)
		if err != nil {
			log.Fatalf("error while decoding json: %s\n", err.Error())
		}
	}
}

func decodeJsonToStruct(url string, target interface{}) error {
	resp, err := client.Get(url)
	if err != nil {
		log.Printf("could not parse json froum url: %s\n", err.Error())
		return err
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Fatalf("err: ", err.Error())
		}
	}(resp.Body)
	return json.NewDecoder(resp.Body).Decode(target)
}
