package main

import (
	"apache-kafka-golang/kafka"
	"apache-kafka-golang/model"
	"context"
	"encoding/json"
	"fmt"
	"github.com/joho/godotenv"
	kafkago "github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
	"io"
	"log"
	"net/http"
	"time"
)

var client *http.Client

func main() {
	// Загрузка переменных окружения
	if err := godotenv.Load(); err != nil {
		log.Fatalf("error loading env variables: %s", err.Error())
	}

	// Создание объектов reader, writer
	reader := kafka.NewKafkaReader()
	//writer := kafka.NewKafkaWriter()

	// Инициализация каналов
	ctx := context.Background()
	messages := make(chan kafkago.Message, 1e1)
	//messageCommitChan := make(chan kafkago.Message, 1000)

	//  Инициализация группы горутин 'g' соответствующим контекстом 'ctx'
	g, ctx := errgroup.WithContext(ctx)

	// Получение сообщений с канала
	g.Go(func() error {
		return reader.FetchMessage(ctx, messages)
	})

	// printing json data from kafka
	var person model.Person

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

	// fetching url
	//1. Возрастом - https://api.agify.io/?name=Dmitriy
	//2. Полом - https://api.genderize.io/?name=Dmitriy
	//3. Национальностью - https://api.nationalize.io/?name=Dmitriy
	client = &http.Client{Timeout: 10 * time.Second}

	urlAge := "https://api.agify.io/?name=Dmitriy"
	urlGender := "https://api.genderize.io/?name=Dmitriy"
	urlCountry := "https://api.nationalize.io/?name=Dmitriy"

	fmt.Printf("GetJson() for Gender\n")
	err0 := GetJson(urlAge, &person)
	err1 := GetJson(urlGender, &person)
	err2 := GetJson(urlCountry, &person.Country)

	if err0 != nil && err1 != nil && err2 != nil {
		fmt.Printf("error getting person - %s\n", err0.Error())
		fmt.Printf("error getting person - %s\n", err1.Error())
		fmt.Printf("error getting person - %s\n", err2.Error())
	} else {
		fmt.Printf("age: %d\n", person.Age)
		fmt.Printf("gender: %v\n", person.Gender)
		fmt.Printf("country: %v\n", person.Country)
	}
	fmt.Println()

	fmt.Printf("PERSON: %+v\n", person)

	// Запись сообщений в другой канал
	//g.Go(func() error {
	//	return writer.WriteMessages(ctx, messages, messageCommitChan)
	//})

	// Фиксация сообщений - в противном случае сообщения отправятся в другой канал еще раз
	//g.Go(func() error {
	//	return reader.CommitMessages(ctx, messageCommitChan)
	//})

	// Блокирующая операция
	err := g.Wait()
	if err != nil {
		log.Fatalln(err)
	}
}

func GetJson(url string, target interface{}) error {
	resp, err := client.Get(url)
	if err != nil {
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
