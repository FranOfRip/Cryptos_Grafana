# Crypto Exchange v1.0
### Pipeline de Análisis de Criptomonedas en Tiempo Real

Pipeline Big Data completo que captura precios de Binance en tiempo real, los procesa con Spark Structured Streaming, los almacena en HDFS en formato Parquet y los visualiza en Grafana con métricas de Prometheus.

```
Binance WebSocket ──► Producer Python ──► Apache Kafka ──► Spark Streaming ──► HDFS (Parquet)
                                                                    │
                                                               Prometheus ──► Grafana
```

---

## Stack tecnológico

| Capa | Tecnología | Versión | Función |
|---|---|---|---|
| Fuente | Binance WebSocket | API pública | Stream ~1 msg/s por par |
| Ingesta | Apache Kafka + Zookeeper | 7.6.0 | Bus de eventos distribuido |
| Procesamiento | Spark Structured Streaming | 3.x | SMA, variación %, pump & dump |
| Almacenamiento | HDFS + Apache Hive | Hadoop 3.2.1 / Hive 3.1.3 | Parquet particionado, HQL |
| Observabilidad | Prometheus + Grafana | latest | 8 métricas propias, 2 dashboards |
| Orquestación | Docker Compose | v2 | 13 servicios en red crypto_net |

---

## Requisitos previos

- Docker Desktop con **mínimo 8 GB de RAM** asignados al engine
- Puertos libres: `2181`, `3000`, `9000`, `9083`, `9090`, `9092`, `9864`, `9870`, `10000`
- Conexión a Internet (WebSocket de Binance)

---

## Instalación y ejecución

### 1. Clonar el repositorio

```bash
git clone https://github.com/FranOfRip/crypto-exchange.git
cd crypto-exchange
```

### 2. Levantar el sistema

```bash
docker compose up -d
```

Espera **2-3 minutos** a que HDFS salga de safe mode y Spark procese el primer micro-batch.

### 3. Si algo falla (volúmenes corruptos, error Derby en Hive)

```bash
docker compose down -v && docker compose up -d
```

---

## Verificar que todo funciona

```bash
# Ver contenedores activos
docker compose ps

# Ver topics de Kafka
docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list

# Consumir mensajes del topic BTC en tiempo real
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic crypto_ticker \
  --from-beginning \
  --max-messages 5

# Ver ficheros Parquet en HDFS
docker exec namenode hdfs dfs -ls -R /data/crypto

# Espacio ocupado en HDFS
docker exec namenode hdfs dfs -du -s -h /data/

# Ver logs en tiempo real
docker compose logs -f producer spark
```

---

## Accesos

| Servicio | URL | Credenciales |
|---|---|---|
| Grafana | http://localhost:3000 | admin / 1234 |
| Prometheus | http://localhost:9090 | — |
| Spark UI | http://localhost:4040 | — |
| HDFS Web UI | http://localhost:9870 | — |
| cAdvisor | http://localhost:8082 | — |
| Producer métricas | http://localhost:8000/metrics | — |
| Spark métricas | http://localhost:8001/metrics | — |
| Hive Beeline | localhost:10000 | `docker exec -it hiveserver beeline -u jdbc:hive2://localhost:10000` |

---

## Monitorización

Ejemplos de consultas en Prometheus (`http://localhost:9090`):

```promql
# Tasa de mensajes recibidos desde Binance por par y tipo
rate(binance_mensajes_total[2m])

# Precio actual de BTC
crypto_precio_actual{par="BTCUSDT"}

# Media móvil SMA 5min
spark_sma_5min{par="BTCUSDT"}

# Pump & dump detectados
spark_pump_dump_detectados_total
```

---

## Procesamiento

Spark Structured Streaming procesa los datos en tiempo real mediante ventanas deslizantes:

| Indicador | Cálculo | Ventana | Slide |
|---|---|---|---|
| SMA (media móvil) | avg(precio_actual) | 5 min | 30s |
| Variación % | (máx − mín) / mín × 100 | 5 min | 30s |
| Volumen medio | avg(volumen_base) | 5 min | 30s |
| Pump & dump | variacion_pct > 2% → alerta | 5 min | 30s |

---

## Almacenamiento

Datos en HDFS en formato **Parquet**, particionados por par y fecha:

```
/data/crypto/
├── par=BTCUSDT/
│   └── fecha=2026-06-07/
│       └── part-00000-*.snappy.parquet
└── par=ETHUSDT/
    └── fecha=2026-06-07/
        └── part-00000-*.snappy.parquet
```

---

## Consulta de datos

```bash
# Conectar a Hive
docker exec -it hiveserver beeline -u jdbc:hive2://localhost:10000
```

```sql
-- Crear tabla externa sobre datos HDFS
CREATE EXTERNAL TABLE IF NOT EXISTS crypto_prices (
  tipo STRING, event_time BIGINT, precio_actual DOUBLE,
  precio_apertura DOUBLE, precio_maximo DOUBLE, precio_minimo DOUBLE,
  volumen_base DOUBLE, volumen_quote DOUBLE,
  ingestion_ts BIGINT, event_timestamp TIMESTAMP
) PARTITIONED BY (par STRING, fecha STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/crypto';

MSCK REPAIR TABLE crypto_prices;

-- Resumen diario por par
SELECT par, fecha, AVG(precio_actual) AS precio_medio,
  MAX(precio_actual) AS maximo, MIN(precio_actual) AS minimo,
  COUNT(*) AS num_ticks
FROM crypto_prices GROUP BY par, fecha ORDER BY fecha DESC;

-- Días más volátiles
SELECT par, fecha,
  ROUND(((MAX(precio_actual)-MIN(precio_actual))/MIN(precio_actual))*100,4) AS variacion_pct
FROM crypto_prices GROUP BY par, fecha
HAVING variacion_pct > 1 ORDER BY variacion_pct DESC;

-- Correlación BTC vs ETH
SELECT btc.fecha, btc.precio_medio AS btc, eth.precio_medio AS eth,
  ROUND((eth.precio_medio/btc.precio_medio)*100,4) AS ratio_eth_btc
FROM (SELECT fecha, AVG(precio_actual) AS precio_medio FROM crypto_prices
      WHERE par='BTCUSDT' GROUP BY fecha) btc
JOIN (SELECT fecha, AVG(precio_actual) AS precio_medio FROM crypto_prices
      WHERE par='ETHUSDT' GROUP BY fecha) eth ON btc.fecha=eth.fecha
ORDER BY btc.fecha DESC;
```

---

## Ejemplo de mensaje Kafka (topic crypto_ticker)

```json
{
  "tipo": "miniTicker",
  "event_time": 1717754400000,
  "par": "BTCUSDT",
  "precio_actual": 62441.80,
  "precio_apertura": 62100.00,
  "precio_maximo": 62800.00,
  "precio_minimo": 61900.00,
  "volumen_base": 1234.56,
  "volumen_quote": 77123456.78,
  "ingestion_ts": 1717754400123
}
```

---

## Estructura del proyecto

```
crypto-exchange/
├── docker-compose.yml              # Orquestación de los 13 servicios
├── README.md
├── memoria.pdf                     # Memoria técnica del proyecto
├── producer/
│   ├── binance_producer.py         # WebSocket Binance → Kafka
│   ├── Dockerfile
│   └── requirements.txt
├── spark/
│   ├── spark_streaming.py          # SMA, variación %, pump & dump, HDFS
│   ├── Dockerfile
│   └── requirements.txt
├── grafana/
│   ├── dashboards/                 # Dashboard negocio e infraestructura
│   └── provisioning/               # Carga automática al arrancar Grafana
├── prometheus/
│   ├── prometheus.yml              # Targets: producer, spark, node-exporter, cadvisor
│   └── alerts.yml                  # 3 alertas configuradas
└── hive/
    └── queries.sql                 # 3 queries HQL sobre datos históricos
```

---

## Posibles mejoras

- Añadir CoinGecko REST para market cap y dominancia de BTC
- Incorporar Fear & Greed Index de alternative.me con polling horario
- Implementar RSI de 14 periodos con foreachBatch + Window functions
- Escalar Kafka a 3 brokers con factor de replicación 3
- Umbral dinámico de pump & dump basado en volatilidad histórica del par
- Notificaciones por Telegram o email desde Grafana

---

## Autor

**Francisco José Camarasa** — Proyecto Final · Big Data Aplicado  
[github.com/FranOfRip/crypto-exchange](https://github.com/FranOfRip/crypto-exchange)
