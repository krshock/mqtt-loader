package pg

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func ObtenerPoolSarcomLocal() *pgxpool.Pool {

	dbconfig, err := pgxpool.ParseConfig(os.Getenv("SARCOM_PG_CONFIG"))
	if err != nil {
		log.Fatal("Error configuracion variable SARCOM_PG_CONFIG", err)
	}
	dbconfig.MaxConns = 32
	dbconfig.MinConns = 0
	dbconfig.MaxConnLifetime = time.Hour * 1
	dbconfig.MaxConnIdleTime = time.Minute * 30
	dbconfig.HealthCheckPeriod = time.Minute
	dbconfig.ConnConfig.ConnectTimeout = time.Second * 5

	connPool, err := pgxpool.NewWithConfig(context.Background(), dbconfig)

	if err != nil {
		log.Fatal("Error al inicializar Pool a Sarcom Local", err)
	}
	return connPool
	/*conn, err := pgx.Connect(context.Background(), os.Getenv("SARCOM_PG_CONFIG"))
	if err != nil {
		log.Println(err)
		panic("ConectarSarcomLocal: No se pudo conectar a posgres sarcom local")
	}
	return conn*/
}

type PozoConfig struct {
	Uuid           string
	CodigoObra     string
	NombreEstacion string
}

/*
func ListarConfiguracionesPozos(conn *pgxpool.Conn) (*[]PozoConfig, error) {

	rows, err := conn.Query(context.Background(), "select id, codigo_obra , nombre_estacion from pozos_config where cod_cliente = 'verfrut'", args)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pozosConfig *[]PozoConfig = new([]PozoConfig)

	for rows.Next() {

	}

	return pozosConfig, nil
}*/

func InsertMqttPozo(ctx context.Context, pool *pgxpool.Pool, mqttId string, topic string, payload map[string]interface{}, ts time.Time) (int64, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("InsertMqttPozo:pool.Acquire")
	}
	defer conn.Release()

	args := pgx.NamedArgs{
		"envioId": mqttId,
		"topic":   topic,
		"payload": payload,
		"ts":      ts,
	}

	ct, err := conn.Exec(ctx, "INSERT INTO mqtt_pozos(id, topic, payload, ts, estado) VALUES (@envioId,@topic,@payload,@ts, 1)", args)
	if err != nil {
		return 0, err
	}
	//log.Printf("insertado ok %+v\n", payload)
	return ct.RowsAffected(), nil
}
