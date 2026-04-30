from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    from_unixtime,
    date_format,
    window,
    avg,
    max as spark_max,
    min as spark_min,
    expr
)
from pyspark.sql.types import (
    StructType,
    StringType,
    DoubleType,
    LongType
)


spark = SparkSession.builder \
    .appName("CryptoSparkStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
TOPIC_TICKER = "crypto_ticker"


schema_ticker = StructType() \
    .add("tipo", StringType()) \
    .add("event_time", LongType()) \
    .add("par", StringType()) \
    .add("precio_actual", DoubleType()) \
    .add("precio_apertura", DoubleType()) \
    .add("precio_maximo", DoubleType()) \
    .add("precio_minimo", DoubleType()) \
    .add("volumen_base", DoubleType()) \
    .add("volumen_quote", DoubleType()) \
    .add("ingestion_ts", LongType())


raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", TOPIC_TICKER) \
    .option("startingOffsets", "latest") \
    .load()


json_df = raw_df.selectExpr("CAST(value AS STRING) AS json_value")


parsed_df = json_df \
    .select(from_json(col("json_value"), schema_ticker).alias("data")) \
    .select("data.*")


clean_df = parsed_df \
    .withColumn(
        "event_timestamp",
        to_timestamp(from_unixtime(col("event_time") / 1000))
    ) \
    .withColumn(
        "fecha",
        date_format(col("event_timestamp"), "yyyy-MM-dd")
    )


indicadores_df = clean_df \
    .withWatermark("event_timestamp", "2 minutes") \
    .groupBy(
        window(col("event_timestamp"), "5 minutes", "30 seconds"),
        col("par")
    ) \
    .agg(
        avg("precio_actual").alias("sma_5min"),
        spark_max("precio_actual").alias("precio_maximo_ventana"),
        spark_min("precio_actual").alias("precio_minimo_ventana"),
        avg("volumen_base").alias("volumen_medio")
    ) \
    .withColumn(
        "variacion_pct",
        ((col("precio_maximo_ventana") - col("precio_minimo_ventana")) / col("precio_minimo_ventana")) * 100
    ) \
    .withColumn(
        "pump_dump_detectado",
        expr("CASE WHEN variacion_pct > 2 THEN 1 ELSE 0 END")
    )


query_hdfs = clean_df.writeStream \
    .format("parquet") \
    .option("path", "hdfs://namenode:9000/data/crypto") \
    .option("checkpointLocation", "hdfs://namenode:9000/checkpoints/crypto_ticker") \
    .partitionBy("par", "fecha") \
    .outputMode("append") \
    .start()


query_console = indicadores_df.writeStream \
    .format("console") \
    .outputMode("update") \
    .option("truncate", "false") \
    .start()


spark.streams.awaitAnyTermination()