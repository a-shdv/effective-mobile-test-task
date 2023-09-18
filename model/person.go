package model

import "encoding/json"

type Person struct {
	Name       string                     `json:"name"`
	Surname    string                     `json:"surname"`
	Patronymic string                     `json:"patronymic"`
	Age        int                        `json:"age"`
	Gender     string                     `json:"gender"`
	Country    map[string]json.RawMessage `json:"country"`
}
