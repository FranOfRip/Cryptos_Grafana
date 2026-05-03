#  Crypto Exchange Streaming Pipeline

Pipeline de datos en tiempo real para criptomonedas usando tecnologías Big Data.

---

##  Descripción

Este proyecto procesa datos en tiempo real desde Binance y los visualiza en dashboards.

Flujo:

**Binance → Kafka → Spark → HDFS → Grafana**

---

##  Arquitectura

![Arquitectura](docs/arquitectura.png)

---

##  Tecnologías

# Python (Producer)
# Kafka
# Spark Streaming
# HDFS
# Prometheus
# Grafana
# Docker

---

##  Instalación y ejecución

### 1. Clonar el repositorio

```bash
git clone https://github.com/FranOfRip/Cryptos_Grafana.git
cd Cryptos_Grafana
```

### 2. Levantar el sistema

```bash
docker compose up -d
```

### 3. Verificar contenedores

```bash
docker ps
```

---

##  Accesos

* Grafana → http://localhost:3000
* Prometheus → http://localhost:9090

---

##  Monitorización

Ejemplo de consulta en Prometheus:

```promql
rate(binance_mensajes_total[2m])
```

---

##  Procesamiento

Spark Streaming procesa los datos en tiempo real mediante ventanas y calcula:

* Precio medio
* Máximo y mínimo
* Volumen

---

##  Almacenamiento

Datos en HDFS en formato Parquet:

```
/data/crypto/par=BTCUSDT/fecha=YYYY-MM-DD
```

---

##  Consulta de datos

Ejemplo:

```python
df.groupBy("par").avg("precio_actual").show()
```

---

##  Autor

Proyecto desarrollado por Fran.

---
