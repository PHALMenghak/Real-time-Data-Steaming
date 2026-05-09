import json
import os
import time
from datetime import datetime

from kafka import KafkaProducer
from websocket import WebSocketApp
from dotenv import load_dotenv

# ----------------------------------------
# Load Environment Variables
# ----------------------------------------

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")

# ----------------------------------------
# Kafka Producer
# ----------------------------------------

producer = None


def create_kafka_producer():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5,
                linger_ms=10,
            )
        except Exception as error:
            print(f"Kafka not ready yet: {error}. Retrying in 5 seconds...")
            time.sleep(5)

# ----------------------------------------
# WebSocket URL
# ----------------------------------------

socket_url = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"

# ----------------------------------------
# Stock Symbols
# ----------------------------------------

SYMBOLS = [
    "AAPL",
    "MSFT",
    "GOOGL",
    "AMZN",
    "TSLA"
]

# ----------------------------------------
# WebSocket Handlers
# ----------------------------------------

def on_open(ws):

    print("Connected to Finnhub WebSocket")

    for symbol in SYMBOLS:

        subscribe_message = {
            "type": "subscribe",
            "symbol": symbol
        }

        ws.send(json.dumps(subscribe_message))

        print(f"Subscribed to {symbol}")

# ----------------------------------------

def on_message(ws, message):

    try:

        data = json.loads(message)

        if data.get("type") != "trade":
            return

        trades = data.get("data", [])

        for trade in trades:

            stock_event = {
                "symbol": trade.get("s"),
                "price": trade.get("p"),
                "volume": trade.get("v"),
                "timestamp": datetime.utcfromtimestamp(
                    trade.get("t") / 1000
                ).isoformat()
            }

            # Send to Kafka
            producer.send(
                KAFKA_TOPIC,
                key=stock_event["symbol"].encode("utf-8"),
                value=stock_event
            )

            print(f"Sent to Kafka: {stock_event}")

        producer.flush()

    except Exception as e:
        print(f"Message processing error: {e}")

# ----------------------------------------

def on_error(ws, error):
    print(f"WebSocket Error: {error}")

# ----------------------------------------

def on_close(ws, close_status_code, close_msg):
    print("WebSocket connection closed")

# ----------------------------------------
# Main
# ----------------------------------------

if __name__ == "__main__":

    producer = create_kafka_producer()

    ws = WebSocketApp(
        socket_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )

    ws.run_forever()