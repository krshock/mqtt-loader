package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"mqttdeco/pg"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
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

func CountFlag(flagsMap *map[string]int64, _flag string) {
	fcount, ok := (*flagsMap)[_flag]
	if !ok {
		(*flagsMap)[_flag] = 1
	} else {
		(*flagsMap)[_flag] = fcount + 1
	}
}

/*
var topicsMap = make(map[string]int64)

func CountTopics(_topic string) {
	tcount, ok := topicsMap[_topic]
	if !ok {
		topicsMap[_topic] = 1
	} else {
		topicsMap[_topic] = tcount + 1
	}
}*/

func main() {
	pool := pg.ObtenerPoolSarcomLocal()
	//scanPozoFile(time.Now(), "8d117b00-137d-499c-9d6f-42dd43004f32")
	//scanPozoTopic("2024-06-19", "2025-03-31", "8d117b00-137d-499c-9d6f-42dd43004f32")
	/*scanPozoTopic(pool, "2024-06-19", "2025-03-31", "51c4faf3-48d1-44bf-9791-713d172b645e")
	scanPozoTopic(pool, "2024-06-19", "2025-03-31", "625396c8-acc0-49fa-a81f-6cd7bc951575")
	scanPozoTopic(pool, "2024-06-19", "2025-03-31", "5c1b28f2-4e44-4009-8fbc-964fa7a0c957")
	scanPozoTopic(pool, "2024-06-19", "2025-03-31", "528a736b-a589-497a-9e9f-4bb5179d8ff2")
	scanPozoTopic(pool, "2024-06-19", "2025-03-31", "8d117b00-137d-499c-9d6f-42dd43004f32")
	scanPozoTopic(pool, "2024-06-19", "2025-03-31", "7267b1c8-81c9-4cfb-b6e9-ad2695cf8279")
	scanPozoTopic(pool, "2024-06-19", "2025-03-31", "ea96ae4e-9822-4da2-95b9-4c0bcbd95d32")
	scanPozoTopic(pool, "2024-06-19", "2025-03-31", "a1fa8029-7dd2-47b6-a3ae-627e24a9956a")*/

}

func scanPozoTopic(pool *pgxpool.Pool, startdate string, endDate string, topicId string) {
	log.Println("==================================================================================")
	log.Printf("Topic: %v\n", topicId)
	start, err := time.Parse(time.DateOnly, startdate)
	if err != nil {
		log.Fatal(err)
	}
	//end := time.Now() //start.AddDate(0, 1, 0)
	end, err := time.Parse(time.DateOnly, endDate)
	if err != nil {
		log.Fatal(err)
	}
	//log.Printf("Rango fechas: %v %v\n", start, end)
	contadorErrores := int64(0)
	contadorLineas := int64(0)
	var flagsMapConCeros = make(map[string]int64)
	var flagsMapSinCeros = make(map[string]int64)
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		//fmt.Println(d.Format("2006-01-02"))

		nerrores, nlineas := scanPozoFile(pool, d, topicId, &flagsMapSinCeros, &flagsMapConCeros)
		contadorErrores = contadorErrores + nerrores
		contadorLineas = contadorLineas + nlineas
	}

	log.Printf("Registros %v", contadorLineas)
	log.Printf("Errores %v", contadorErrores)
	log.Printf("SIN CEROS flags count: %+v\n", flagsMapSinCeros)
	log.Printf("CON CEROS flags count: %+v\n", flagsMapConCeros)
	//log.Printf("topics count: %+v\n", topicsMap)
}

func scanPozoFile(pool *pgxpool.Pool, date time.Time, topicId string, flagsMapSinCero *map[string]int64, flagsMapConCeros *map[string]int64) (int64, int64) {
	filePath := fmt.Sprintf("./mqttdata/mqtt-cAzDWt8-%s.0.jsonl", date.Format(time.DateOnly))
	//log.Println("filepath: ", filePath)
	file, err := os.Open(filePath)
	if err != nil {
		//log.Fatal(err)
		//log.Printf("Archivo no encontrado: %s", filePath)
		return 0, 0
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// optionally, resize scanner's capacity for lines over 64K, see next example
	nlineas := 0
	contadoErrores := int64(0)
	for scanner.Scan() {
		jsonSrc := scanner.Text()
		var medicionMqtt MedicionPozoMqtt
		err := json.Unmarshal([]byte(jsonSrc), &medicionMqtt)
		if err != nil {
			contadoErrores = contadoErrores + 1
			//log.Printf("ERROR MEDICION JSON2: %v %v %v\n", nlineas, err, jsonSrc)
			continue
		}
		if !strings.Contains(medicionMqtt.Message.Topic, topicId) {
			continue
		}
		//CountTopics(medicionMqtt.Message.Topic)
		var body BodyJsonMedicionPozo
		err = json.Unmarshal([]byte(medicionMqtt.Message.Body), &body)
		if err != nil {
			//log.Printf("ERROR BODY JSON: %v %v %v\n", nlineas, err, medicionMqtt.Message.Body)
			continue
		}
		//log.Printf("%+v\n", medicionMqtt)
		//log.Printf("%v %s %+v\n", time.Unix(medicionMqtt.Timestamp/1000, 0), medicionMqtt.Message.Topic, body)
		//ti, _ := strconv.Atoi(body.Time)
		//t := time.Unix(int64(ti), 0)
		//log.Printf("%v topic: %v time:%v time2:%v\n", nlineas, medicionMqtt.Message.Topic, time.Unix(medicionMqtt.Timestamp/1000, 0), t)
		var m = make(map[string]interface{})
		for _, x := range body.SensorDatas {
			m[x.Flag] = x.Value
			CountFlag(flagsMapConCeros, x.Flag)
			if x.Value != 0 {
				CountFlag(flagsMapSinCero, x.Flag)
			}
		}
		if _, ok := m["EXTPWR"]; !ok {
			continue
		}
		_, err = pg.InsertMqttPozo(context.Background(), pool, medicionMqtt.Id, medicionMqtt.Message.Topic, m, time.Unix(medicionMqtt.Timestamp/1000, 0))
		if err != nil {
			log.Printf("Error al insertar: %v\n", err)
			contadoErrores = contadoErrores + 1
		}
		nlineas = nlineas + 1
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	return int64(contadoErrores), int64(nlineas)
}
