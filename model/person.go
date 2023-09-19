package model

type Person struct {
	Name       string        `json:"name"`
	Surname    string        `json:"surname"`
	Patronymic string        `json:"patronymic"`
	Age        int           `json:"age"`
	Gender     string        `json:"gender"`
	Country    []CountryData `json:"country"`
}

type CountryData struct {
	CountryId   string  `json:"countryId"`
	Probability float64 `json:"probability"`
}
