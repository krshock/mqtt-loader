package main

import (
	"bufio"
	"context"
	"encoding/csv"
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

func MinFlagF(flagsMap *map[string]float64, _flag string, valueF float64) {
	fval, ok := (*flagsMap)[_flag]
	if !ok {
		(*flagsMap)[_flag] = valueF
	} else {
		if valueF < fval {
			(*flagsMap)[_flag] = valueF
		}
	}
}

func MaxFlagF(flagsMap *map[string]float64, _flag string, valueF float64) {
	fval, ok := (*flagsMap)[_flag]
	if !ok {
		(*flagsMap)[_flag] = valueF
	} else {
		if valueF > fval {
			(*flagsMap)[_flag] = valueF
		}
	}
}

func GetFlagF(flagsMap *map[string]float64, _flag string) string {
	fval, ok := (*flagsMap)[_flag]
	if !ok {
		return ""
	}
	return fmt.Sprintf("%.2f", fval)

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

var writeToPostgres = false

func main() {
	//ScanPozosData()
	PozosReporteDiario()
}

func ScanPozosData() {
	pool := pg.ObtenerPoolSarcom()
	defer pool.Close()
	scanPozoTopic(pool, "2024-06-19", "2025-03-31", "625396c8-acc0-49fa-a81f-6cd7bc951575")
	scanPozoTopic(pool, "2024-06-19", "2025-03-31", "5c1b28f2-4e44-4009-8fbc-964fa7a0c957")
	scanPozoTopic(pool, "2024-06-19", "2025-03-31", "528a736b-a589-497a-9e9f-4bb5179d8ff2")
	scanPozoTopic(pool, "2024-06-19", "2025-03-31", "8d117b00-137d-499c-9d6f-42dd43004f32")
	scanPozoTopic(pool, "2024-06-19", "2025-03-31", "7267b1c8-81c9-4cfb-b6e9-ad2695cf8279")
	scanPozoTopic(pool, "2024-06-19", "2025-03-31", "ea96ae4e-9822-4da2-95b9-4c0bcbd95d32")
	scanPozoTopic(pool, "2024-06-19", "2025-03-31", "a1fa8029-7dd2-47b6-a3ae-627e24a9956a")

}

func PozosReporteDiario() {
	pool := pg.ObtenerPoolSarcom()
	defer pool.Close()
	fechaDesde := "2025-04-01"
	fechaHasta := "2025-04-08"
	PozoResumenByObra(pool, fechaDesde, fechaHasta, "OB-0703-570")
	/*
		statsPozoTopic(fechaDesde, fechaHasta, "51c4faf3-48d1-44bf-9791-713d172b645e")
		statsPozoTopic(fechaDesde, fechaHasta, "625396c8-acc0-49fa-a81f-6cd7bc951575")
		statsPozoTopic(fechaDesde, fechaHasta, "5c1b28f2-4e44-4009-8fbc-964fa7a0c957")
		statsPozoTopic(fechaDesde, fechaHasta, "528a736b-a589-497a-9e9f-4bb5179d8ff2")
		statsPozoTopic(fechaDesde, fechaHasta, "8d117b00-137d-499c-9d6f-42dd43004f32")
		statsPozoTopic(fechaDesde, fechaHasta, "7267b1c8-81c9-4cfb-b6e9-ad2695cf8279")
		statsPozoTopic(fechaDesde, fechaHasta, "ea96ae4e-9822-4da2-95b9-4c0bcbd95d32")
		statsPozoTopic(fechaDesde, fechaHasta, "a1fa8029-7dd2-47b6-a3ae-627e24a9956a")
	*/
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
	log.Println("filepath: ", filePath)
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
		//log.Printf("%v %s %+v\n", time.Unix(medicionMqtt.Timestamp/1000, 0).Add(time.Hour*4), medicionMqtt.Message.Topic, m)

		if writeToPostgres {
			_, err = pg.InsertMqttPozo(context.Background(), pool, medicionMqtt.Id, medicionMqtt.Message.Topic, m, time.Unix(medicionMqtt.Timestamp/1000, 0).Add(time.Hour*4))
			if err != nil {
				log.Printf("Error al insertar: %v\n", err)
				contadoErrores = contadoErrores + 1
			}
		}
		nlineas = nlineas + 1
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	return int64(contadoErrores), int64(nlineas)
}

func PozoResumenByObra(pool *pgxpool.Pool, fechaDesde string, fechaHasta string, codigoObra string) {
	log.Println("==================================================================================")
	log.Printf("Codigo Obra: %v\n", codigoObra)
	desde, err := time.Parse(time.DateOnly, fechaDesde)
	if err != nil {
		log.Fatal(err)
	}
	//end := time.Now() //start.AddDate(0, 1, 0)
	hasta, err := time.Parse(time.DateOnly, fechaHasta)
	if err != nil {
		log.Fatal(err)
	}
	//log.Printf("Rango fechas: %v %v\n", start, end)

	pozoCfg, err := pg.GetPozoConfig(pool, codigoObra)
	if err != nil {
		log.Printf("Error obteniendo configuracion de %s\n%v\n", codigoObra, err)
		return
	}
	if pozoCfg == nil {
		log.Printf("No existe configuracion para el pozo %s\n", codigoObra)
		return
	}
	if pozoCfg.Estado == 0 {
		log.Printf("Pozo desactivado: %s - %s\n", codigoObra, pozoCfg.NombreEstacion)
		return
	}

	contadorErrores := int64(0)
	contadorLineas := int64(0)

	csvPath := fmt.Sprintf("./csv_data/resumen-diario-%s_%s_%s.csv", codigoObra, fechaDesde, fechaHasta)
	csvFile, err := os.Create(csvPath) //deberia pisarlos archivos
	if err != nil {
		log.Fatalf("Cannot create file %s", csvPath)
	}
	defer csvFile.Close()
	csvWriter := csv.NewWriter(csvFile)
	csvWriter.Write([]string{
		"Topic",
		"Fecha",
		"Lineas",
		"FC Ok",
		"NC Flags",
		"AI0 Min",
		"AI0 Max",
		"COUNT Min",
		"COUNT Max",
		"COUNT1 Min",
		"COUNT1 Max",
		"AI1 Min",
		"AI1 Max",
		"EXTPWR Min",
		"EXTPWR Max",
	})
	csvWriter.Flush()

	for d := desde; !d.After(hasta); d = d.AddDate(0, 0, 1) {
		//fmt.Println(d.Format("2006-01-02"))
		log.Printf("%v fecha: %v\n", pozoCfg.NombreEstacion, d)
		nerrores, nlineas := PozoResumenDiario(d, pozoCfg.Topic, csvWriter)
		contadorErrores = contadorErrores + nerrores
		contadorLineas = contadorLineas + nlineas
		csvWriter.Flush()
	}

	log.Printf("Registros %v", contadorLineas)
	log.Printf("Errores %v", contadorErrores)
	//log.Printf("topics count: %+v\n", topicsMap)
}

func PozoResumenDiario(date time.Time, topicId string, csvWriter *csv.Writer) (int64, int64) {
	filePath := fmt.Sprintf("./mqttdata/mqtt-cAzDWt8-%s.0.jsonl", date.Format(time.DateOnly))
	//log.Println("filepath: ", filePath)
	//log.Printf(">>> %v\n", date.Format(time.DateOnly))
	flagsMapSinCero := make(map[string]int64)
	flagsMapConCeros := make(map[string]int64)
	flagsMin := make(map[string]float64)
	flagsMax := make(map[string]float64)
	file, err := os.Open(filePath)
	if err != nil {
		//log.Fatal(err)
		//log.Printf("Archivo no encontrado: %s", filePath)
		csvWriter.Write([]string{
			topicId,
			date.Format(time.DateOnly),
			fmt.Sprintf("%v", 0),
			fmt.Sprintf("%v", true),
			"",
		})
		return 0, 0
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	nlineas := int64(0)
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
		var body BodyJsonMedicionPozo
		err = json.Unmarshal([]byte(medicionMqtt.Message.Body), &body)
		if err != nil {
			continue
		}
		var m = make(map[string]interface{})
		for _, x := range body.SensorDatas {
			m[x.Flag] = x.Value
			CountFlag(&flagsMapConCeros, x.Flag)
			if x.Value != 0 {
				CountFlag(&flagsMapSinCero, x.Flag)
			}
		}

		if val, ok := m["AI0"]; ok {
			MinFlagF(&flagsMin, "AI0", val.(float64))
			MaxFlagF(&flagsMax, "AI0", val.(float64))
		}
		if val, ok := m["AI1"]; ok {
			MinFlagF(&flagsMin, "AI1", val.(float64))
			MaxFlagF(&flagsMax, "AI1", val.(float64))
		}
		if val, ok := m["COUNT"]; ok {
			MinFlagF(&flagsMin, "COUNT", val.(float64))
			MaxFlagF(&flagsMax, "COUNT", val.(float64))
		}
		if val, ok := m["COUNT1"]; ok {
			MinFlagF(&flagsMin, "COUNT1", val.(float64))
			MaxFlagF(&flagsMax, "COUNT1", val.(float64))
		}
		if val, ok := m["EXTPWR"]; ok {
			MinFlagF(&flagsMin, "EXTPWR", val.(float64))
			MaxFlagF(&flagsMax, "EXTPWR", val.(float64))
		}
		if _, ok := m["EXTPWR"]; !ok {
			continue
		}
		nlineas = nlineas + 1
	}
	countOk := true
	for _, v := range flagsMapSinCero {
		if v != nlineas {
			countOk = false
		}
	}
	/*
		if countOk {
			log.Println("FLAGS COUNT :OK ")
		} else {
			log.Println("FLAGS COUNT: NOOK ", topicId)
		}
		log.Printf("SIN CEROS flags count: %+v\n", flagsMapSinCero)
		log.Printf("CON CEROS flags count: %+v\n", flagsMapConCeros)*/

	csvWriter.Write([]string{
		topicId,
		date.Format(time.DateOnly),
		fmt.Sprintf("%v", nlineas),
		fmt.Sprintf("%v", countOk),
		fmt.Sprintf("%v", flagsMapSinCero),
		GetFlagF(&flagsMin, "AI0"),
		GetFlagF(&flagsMax, "AI0"),
		GetFlagF(&flagsMin, "COUNT"),
		GetFlagF(&flagsMax, "COUNT"),
		GetFlagF(&flagsMin, "COUNT1"),
		GetFlagF(&flagsMax, "COUNT1"),
		GetFlagF(&flagsMin, "AI1"),
		GetFlagF(&flagsMax, "AI1"),
		GetFlagF(&flagsMin, "EXTPWR"),
		GetFlagF(&flagsMax, "EXTPWR"),
	})
	/*
		"Topic",
		"Fecha",
		"Lineas",
		"FC Ok",
		"NC Flags",*/

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	return int64(contadoErrores), int64(nlineas)
}
