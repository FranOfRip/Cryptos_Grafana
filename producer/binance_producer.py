import json
import os
import time

import websocket
from kafka import KafkaProducer
from prometheus_client import Counter, Gauge, start_http_server


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

TOPIC_TICKER = "crypto_ticker"
TOPIC_KLINE = "crypto_kline"

STREAMS = [
    "btcusdt@miniTicker",
    "ethusdt@miniTicker",
    "btcusdt@kline_1m",
    "ethusdt@kline_1m",
]

BINANCE_WS_URL = "wss://stream.binance.com:9443/stream?streams=" + "/".join(STREAMS)


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    key_serializer=lambda key: key.encode("utf-8") if key else None,
)


mensajes_binance = Counter(
    "binance_mensajes_total",
    "Total de mensajes recibidos desde Binance",
    ["tipo", "par"],
)

mensajes_kafka = Counter(
    "kafka_mensajes_publicados_total",
    "Total de mensajes publicados en Kafka",
    ["topic", "par"],
)

precio_actual = Gauge(
    "crypto_precio_actual",
    "Precio actual de la criptomoneda",
    ["par"],
)


def normalizar_miniticker(payload):
    """
    Convierte el mensaje miniTicker de Binance a un formato más limpio.
    """
    return {
        "tipo": "miniTicker",
        "event_time": payload.get("E"),
        "par": payload.get("s"),
        "precio_actual": float(payload.get("c")),
        "precio_apertura": float(payload.get("o")),
        "precio_maximo": float(payload.get("h")),
        "precio_minimo": float(payload.get("l")),
        "volumen_base": float(payload.get("v")),
        "volumen_quote": float(payload.get("q")),
        "ingestion_ts": int(time.time() * 1000),
    }


def normalizar_kline(payload):
    """
    Convierte el mensaje kline_1m de Binance a un formato más limpio.
    """
    kline = payload.get("k", {})

    return {
        "tipo": "kline_1m",
        "event_time": payload.get("E"),
        "par": payload.get("s"),
        "intervalo": kline.get("i"),
        "open_time": kline.get("t"),
        "close_time": kline.get("T"),
        "precio_apertura": float(kline.get("o")),
        "precio_cierre": float(kline.get("c")),
        "precio_maximo": float(kline.get("h")),
        "precio_minimo": float(kline.get("l")),
        "volumen_base": float(kline.get("v")),
        "numero_trades": int(kline.get("n")),
        "vela_cerrada": bool(kline.get("x")),
        "ingestion_ts": int(time.time() * 1000),
    }


def publicar_en_kafka(topic, key, mensaje):
    producer.send(topic, key=key, value=mensaje)
    producer.flush()
    mensajes_kafka.labels(topic=topic, par=key).inc()
    print(f"Publicado en Kafka | topic={topic} | par={key}")


def on_message(ws, message):
    try:
        data = json.loads(message)
        payload = data.get("data", {})

        event_type = payload.get("e")
        par = payload.get("s")

        if not event_type or not par:
            print("Mensaje ignorado: no tiene event_type o par")
            return

        if event_type == "24hrMiniTicker":
            mensaje = normalizar_miniticker(payload)
            topic = TOPIC_TICKER

            precio_actual.labels(par=par).set(mensaje["precio_actual"])
            mensajes_binance.labels(tipo="miniTicker", par=par).inc()

        elif event_type == "kline":
            mensaje = normalizar_kline(payload)
            topic = TOPIC_KLINE

            mensajes_binance.labels(tipo="kline_1m", par=par).inc()

        else:
            print(f"Evento ignorado: {event_type}")
            return

        publicar_en_kafka(topic, par, mensaje)

    except Exception as error:
        print(f"Error procesando mensaje: {error}")


def on_error(ws, error):
    print(f"Error WebSocket: {error}")


def on_close(ws, close_status_code, close_msg):
    print(f"WebSocket cerrado. Código={close_status_code}, mensaje={close_msg}")


def on_open(ws):
    print("Conexión WebSocket abierta con Binance")
    print(f"Streams activos: {', '.join(STREAMS)}")


def iniciar_websocket():
    ws = websocket.WebSocketApp(
        BINANCE_WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    ws.run_forever(ping_interval=30, ping_timeout=10)


if __name__ == "__main__":
    print("Iniciando productor Binance → Kafka")
    print(f"Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")

    start_http_server(8000)
    print("Servidor de métricas Prometheus iniciado en puerto 8000")

    while True:
        try:
            iniciar_websocket()
        except Exception as error:
            print(f"Error general del productor: {error}")

        print("Reintentando conexión en 5 segundos...")
        time.sleep(5)