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
import numpy as np

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
    message_symbols = []
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
            latency = time.time() - start_time
            message_times.append(latency)
            message_symbols.append(symbol)
            logging.info(f"Posted: {order} | Latency: {latency:.4f}s")
            time.sleep(0.5)

        session.close_queue(QUEUE_URI)
    return message_times, message_symbols

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
message_times, message_symbols = producer()
time.sleep(2)
stop_event.set()
consumer_thread.join()

logging.info("Finished all operations.")

# -------------------
# Enhanced plotting
# -------------------

# Color map for symbols
color_map = {"RELIANCE": "#1f77b4", "HDFCBANK": "#ff7f0e", "TCS": "#2ca02c"}
colors = [color_map[s] for s in message_symbols]

# 1️⃣ Bar plot
fig1, ax1 = plt.subplots(figsize=(12,6))
ax1.bar(range(1, len(message_times)+1), message_times, color=colors, alpha=0.7)
mean_latency = np.mean(message_times)
ax1.axhline(mean_latency, color='red', linestyle='--', label=f'Mean Latency: {mean_latency:.4f}s')

for idx, latency in enumerate(message_times):
    ax1.text(idx+1, latency + 0.01, f"{latency:.3f}", ha='center', va='bottom', fontsize=8)

ax1.set_xlabel("Message Number")
ax1.set_ylabel("Time to Post (seconds)")
ax1.set_title("BlazingMQ Message Posting Latency")
ax1.grid(True, axis='y', linestyle='--', alpha=0.7)
ax1.legend()
fig1.tight_layout()

# 2️⃣ Histogram
fig2, ax2 = plt.subplots(figsize=(8,4))
ax2.hist(message_times, bins=min(5, len(message_times)), color='skyblue', edgecolor='black')
ax2.set_xlabel("Latency (s)")
ax2.set_ylabel("Frequency")
ax2.set_title("Latency Distribution Histogram")
ax2.grid(True, linestyle='--', alpha=0.7)
fig2.tight_layout()

# 3️⃣ Optional: Cumulative latency plot
fig3, ax3 = plt.subplots(figsize=(10,4))
ax3.plot(np.cumsum(message_times), marker='o', linestyle='-', color='purple')
ax3.set_xlabel("Message Number")
ax3.set_ylabel("Cumulative Latency (s)")
ax3.set_title("Cumulative BlazingMQ Message Posting Latency")
ax3.grid(True, linestyle='--', alpha=0.7)
fig3.tight_layout()

plt.show()
