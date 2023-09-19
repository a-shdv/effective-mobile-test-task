package model

import (
	"encoding/json"
	kafkago "github.com/segmentio/kafka-go"
	"io"
	"log"
	"net/http"
)

type Helper struct {
	Client http.Client
}

// parseJsonData Обрабатывает json-данные, полученные из URL-строки
func (h *Helper) parseJsonData(input string) (*http.Response, error) {
	resp, err := h.Client.Get(input)
	if err != nil {
		log.Printf("could not get data from url: %s\n", err.Error())
		return nil, err
	}
	return resp, nil
}

// storeJsonData Заполняет структуру json-данными, получеными из URL-строки
func storeJsonData(resp *http.Response, target interface{}) error {
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Fatalf("error while closing the connection: ", err.Error())
		}
	}(resp.Body)

	return json.NewDecoder(resp.Body).Decode(target)
}

// ParseAndStoreJsonData Заполняет структуру данными из массива URL-строк
func (h *Helper) ParseAndStoreJsonData(urls []string, target interface{}) error {
	for _, url := range urls {
		resp, err := h.parseJsonData(url)
		if err != nil {
			log.Fatalf("could not parse json-data from url: %s\n", err.Error())
			return err
		}
		err = storeJsonData(resp, &target)
	}
	return nil
}

// StoreKafkaMessages Заполняет структуру данными из очередей kafka
func (h *Helper) StoreKafkaMessages(kafkaMessages chan kafkago.Message, target interface{}) error {
	for msg := range kafkaMessages {
		err := json.Unmarshal(msg.Value, &target)
		if err != nil {
			log.Fatalf("could not fill target with message value: %s\n", err.Error())
			return err
		}
	}
	return nil
}

func (h *Helper) KafkaMessagesToBytes(kafkaMessages chan kafkago.Message) []byte {
	var data []byte
	for msg := range kafkaMessages {
		value := msg.Value
		data = []byte(value)
	}
	return data
}
