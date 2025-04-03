package main

import (
	"bufio"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"time"
)

type MedicionPozoMqtt struct {
	Id        string                  `json:"id"`
	Source    string                  `json:"source"`
	Timestamp int64                   `json:"timestamp"`
	Message   MedicionPozoMessageMqtt `json:"message"`
}

type MedicionPozoMessageMqtt struct {
	Topic       string `json:"topic"`
	ContentType string `json:"application/json"`
	Body        string `json:"body"`
}

type BodyJsonMedicionPozo struct {
	SensorDatas []SensorDataMqtt `json:"sensorDatas"`
	Time        string           `json:"time"`
}

type SensorDataMqtt struct {
	Flag  string  `json:"flag"`
	Value float64 `json:"value"`
}

var flagsMap = make(map[string]int64)

func CountFlag(_flag string) {
	fcount, ok := flagsMap[_flag]
	if !ok {
		flagsMap[_flag] = 1
	} else {
		flagsMap[_flag] = fcount + 1
	}
}

func main() {
	scanFile("./mqttdata/mqtt-cAzDWt8-2025-03-31.0.jsonl", time.Now())
}

func scanFile(path string, date time.Time) {
	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// optionally, resize scanner's capacity for lines over 64K, see next example
	for scanner.Scan() {
		jsonSrc := scanner.Text()
		var medicionMqtt MedicionPozoMqtt
		err := json.Unmarshal([]byte(jsonSrc), &medicionMqtt)
		if err != nil {
			log.Fatalf("ERROR MEDICION JSON: %v", err)
		}
		var body BodyJsonMedicionPozo
		err = json.Unmarshal([]byte(medicionMqtt.Message.Body), &body)
		if err != nil {
			log.Fatalf("ERROR BODY JSON: %v", err)
		}
		//log.Printf("%+v\n", medicionMqtt)
		//log.Printf("%s %+v\n", medicionMqtt.Message.Topic, body)
		ti, _ := strconv.Atoi(body.Time)
		t := time.Unix(int64(ti), 0)
		log.Printf("topic: %v time:%v time2:%v\n", medicionMqtt.Message.Topic, time.Unix(medicionMqtt.Timestamp/1000, 0), t)
		for _, x := range body.SensorDatas {
			if x.Value != 0 {
				CountFlag(x.Flag)
			}
		}
	}

	log.Printf("flags count: %+v\n", flagsMap)

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}
