"""
Low-latency BlazingMQ example for routing Indian stock trading orders.
All operations are local (tcp://localhost:30114) to avoid IP blocking.
"""

import blazingmq
import time
import threading
import signal
import logging
import json
import matplotlib.pyplot as plt

# -------------------
# Logging
# -------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# -------------------
# Broker config
# -------------------
BROKER_URI = "tcp://localhost:30114"
QUEUE_URI = "bmq://bmq.test.persistent.priority/india_stocks_orders"

# Ensure local connection
def is_localhost_connection(uri):
    host = uri.split("://")[1].split(":")[0]
    return host in ("localhost", "127.0.0.1")

if not is_localhost_connection(BROKER_URI):
    raise ValueError("Non-local broker URI detected! Use localhost.")

# -------------------
# Stock parameters
# -------------------
STOCK_SYMBOLS = ["RELIANCE", "HDFCBANK", "TCS"]
NUM_MESSAGES = 10
QTY_PER_ORDER = 100
STARTING_PRICES = {"RELIANCE": 2500, "HDFCBANK": 1600, "TCS": 4200}

# -------------------
# Graceful shutdown
# -------------------
stop_event = threading.Event()
def handler(signum, _):
    logging.info("Termination signal received. Stopping...")
    stop_event.set()

signal.signal(signal.SIGINT, handler)
signal.signal(signal.SIGTERM, handler)

# -------------------
# Connect helper
# -------------------
def connect_with_retry(factory, max_retries=5, delay=2):
    for attempt in range(max_retries):
        try:
            return factory()
        except Exception as e:
            logging.warning(f"Connection attempt {attempt+1} failed: {e}. Retrying in {delay}s...")
            time.sleep(delay)
    raise RuntimeError("Failed to connect after retries.")

# -------------------
# Producer
# -------------------
def producer():
    def create_session():
        return blazingmq.Session(on_session_event=blazingmq.session_events.log_session_event)

    message_times = []
    with connect_with_retry(create_session) as session:
        logging.info("Producer connected.")
        session.open_queue(QUEUE_URI, read=False, write=True, options=blazingmq.QueueOptions())
        logging.info("Queue opened for producing.")

        for i in range(NUM_MESSAGES):
            symbol = STOCK_SYMBOLS[i % len(STOCK_SYMBOLS)]
            price = STARTING_PRICES[symbol] + (i * 10)
            order = {
                "type": "BUY_ORDER",
                "symbol": symbol,
                "qty": QTY_PER_ORDER,
                "price": price,
                "exchange": "NSE",
                "msg_id": i
            }
            payload = json.dumps(order).encode()
            start_time = time.time()
            session.post(QUEUE_URI, payload)
            message_times.append(time.time() - start_time)
            logging.info(f"Posted: {order}")
            time.sleep(0.5)

        session.close_queue(QUEUE_URI)
    return message_times

# -------------------
# Consumer
# -------------------
def on_message(msg, handle):
    try:
        data = json.loads(msg.data.decode())
        logging.info(f"Received: {data} | GUID: {msg.guid} | Queue: {msg.queue_uri}")
        handle.confirm()
    except Exception as e:
        logging.error(f"Failed to process message: {e}")

def consumer():
    def create_session():
        return blazingmq.Session(on_session_event=blazingmq.session_events.log_session_event,
                                 on_message=on_message)
    with connect_with_retry(create_session) as session:
        logging.info("Consumer connected.")
        session.open_queue(
            QUEUE_URI,
            read=True,
            write=False,
            options=blazingmq.QueueOptions(max_unconfirmed_messages=1000, max_unconfirmed_bytes=1000000, consumer_priority=1)
        )
        logging.info("Consumer waiting for messages...")
        while not stop_event.is_set():
            time.sleep(1)
        session.close_queue(QUEUE_URI)

# -------------------
# Run consumer thread
# -------------------
consumer_thread = threading.Thread(target=consumer, daemon=True)
consumer_thread.start()
time.sleep(1)

# -------------------
# Run producer
# -------------------
message_times = producer()
time.sleep(2)
stop_event.set()
consumer_thread.join()

logging.info("Finished all operations.")

# -------------------
# Plot latency
# -------------------
plt.figure(figsize=(8, 4))
plt.plot(range(1, len(message_times) + 1), message_times, marker='o', linestyle='-', color='#1f77b4')
plt.xlabel("Message Number")
plt.ylabel("Time to Post (seconds)")
plt.title("BlazingMQ Message Posting Latency")
plt.grid(True)
plt.tight_layout()
plt.show()
