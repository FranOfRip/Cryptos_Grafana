# 🚀 Crypto Exchange Streaming Pipeline

Pipeline de procesamiento de datos en tiempo real para criptomonedas utilizando tecnologías de Big Data y observabilidad.

---

## 📌 Descripción

Este proyecto implementa un sistema completo de ingesta, procesamiento, almacenamiento y visualización de datos de criptomonedas en tiempo real.

El flujo de datos es el siguiente:

**Binance → Producer → Kafka → Spark Streaming → HDFS**
**Métricas → Prometheus → Grafana**

---

## 🏗️ Arquitectura

![Arquitectura](docs/arquitectura.png)

---

## ⚙️ Tecnologías utilizadas

* Python (Producer)
* Apache Kafka
* Apache Spark Streaming
* Hadoop HDFS
* Prometheus
* Grafana
* Docker / Docker Compose

---

## 📂 Estructura del proyecto

```
crypto-exchange/
├── producer/
├── spark/
├── hdfs/
├── prometheus/
├── grafana/
├── hive/
├── docker-compose.yml
└── README.md
```

---

## 🚀 Ejecución

### Levantar el entorno

```bash
docker compose up -d
```

### Verificar contenedores

```bash
docker ps
```

---

## 📊 Monitorización

* Grafana → http://localhost:3000
* Prometheus → http://localhost:9090

Consulta de ejemplo:

```promql
rate(binance_mensajes_total[2m])
```

---

## 🚨 Alertas

Se ha configurado una alerta en Grafana basada en la tasa de mensajes:

```promql
rate(binance_mensajes_total[2m])
```

Estados:

* 🔴 FIRING → alerta activa
* 🟢 NORMAL → comportamiento normal

---

## ⚡ Procesamiento en Spark

El sistema realiza:

* Ventanas de tiempo
* Cálculo de métricas:

  * precio medio
  * máximo y mínimo
  * volumen
* Detección de eventos

---

## 💾 Almacenamiento en HDFS

Datos almacenados en formato Parquet:

```
/data/crypto/par=BTCUSDT/fecha=YYYY-MM-DD
```

Particionado por:

* par
* fecha

---

## 📈 Consulta histórica

Ejemplo en Spark:

```python
df = spark.read.parquet("hdfs://namenode:9000/data/crypto")
df.groupBy("par").avg("precio_actual").show()
```

---

## ⚠️ Limitaciones

* Entorno local
* Escalabilidad limitada
* Sin integración completa con Hive

---

## 🚀 Posibles mejoras

* Integración con Hive / Presto
* Alertas con notificaciones (email, Slack)
* Escalado distribuido

---


