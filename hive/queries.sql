CREATE EXTERNAL TABLE IF NOT EXISTS crypto_prices (
  tipo STRING,
  event_time BIGINT,
  precio_actual DOUBLE,
  precio_apertura DOUBLE,
  precio_maximo DOUBLE,
  precio_minimo DOUBLE,
  volumen_base DOUBLE,
  volumen_quote DOUBLE,
  ingestion_ts BIGINT,
  event_timestamp TIMESTAMP
)
PARTITIONED BY (
  par STRING,
  fecha STRING
)
STORED AS PARQUET
LOCATION '/data/crypto';

MSCK REPAIR TABLE crypto_prices;

SELECT
  par,
  fecha,
  AVG(precio_actual) AS precio_medio,
  MAX(precio_actual) AS precio_maximo,
  MIN(precio_actual) AS precio_minimo,
  AVG(volumen_base) AS volumen_medio
FROM crypto_prices
GROUP BY par, fecha;