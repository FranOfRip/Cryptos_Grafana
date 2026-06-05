CREATE EXTERNAL TABLE IF NOT EXISTS crypto_prices (
  tipo             STRING,
  event_time       BIGINT,
  precio_actual    DOUBLE,
  precio_apertura  DOUBLE,
  precio_maximo    DOUBLE,
  precio_minimo    DOUBLE,
  volumen_base     DOUBLE,
  volumen_quote    DOUBLE,
  ingestion_ts     BIGINT,
  event_timestamp  TIMESTAMP
)
PARTITIONED BY (
  par   STRING,
  fecha STRING
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/crypto';

MSCK REPAIR TABLE crypto_prices;

SELECT
  par,
  fecha,
  AVG(precio_actual)  AS precio_medio,
  MAX(precio_actual)  AS precio_maximo,
  MIN(precio_actual)  AS precio_minimo,
  AVG(volumen_base)   AS volumen_medio,
  COUNT(*)            AS num_ticks
FROM crypto_prices
GROUP BY par, fecha
ORDER BY fecha DESC, par;

SELECT
  par,
  fecha,
  ROUND(((MAX(precio_actual) - MIN(precio_actual)) / MIN(precio_actual)) * 100, 4) AS variacion_pct_dia,
  MAX(precio_actual) AS maximo_dia,
  MIN(precio_actual) AS minimo_dia
FROM crypto_prices
GROUP BY par, fecha
HAVING variacion_pct_dia > 1
ORDER BY variacion_pct_dia DESC;


SELECT
  btc.fecha,
  btc.precio_medio AS precio_medio_btc,
  eth.precio_medio AS precio_medio_eth,
  ROUND((eth.precio_medio / btc.precio_medio) * 100, 4) AS ratio_eth_btc
FROM (
  SELECT fecha, AVG(precio_actual) AS precio_medio
  FROM crypto_prices WHERE par = 'BTCUSDT'
  GROUP BY fecha
) btc
JOIN (
  SELECT fecha, AVG(precio_actual) AS precio_medio
  FROM crypto_prices WHERE par = 'ETHUSDT'
  GROUP BY fecha
) eth ON btc.fecha = eth.fecha
ORDER BY btc.fecha DESC;