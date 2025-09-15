import blazingmq
import time
import threading
import signal
import logging
import json
import random
import numpy as np

try:
    import matplotlib.pyplot as plt
except ImportError:
    plt = None
    logging.warning("Matplotlib not installed. Skipping plot generation.")

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
ORDER_QTY = 100
PRICE_MAP = {"RELIANCE": 2500, "HDFCBANK": 1600, "TCS": 4200}
PRICE_VARIANCE = 50
MAX_RETRIES = 5
RETRY_DELAY = 2  # seconds

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
def connect_with_retry(factory, max_retries=MAX_RETRIES, delay=RETRY_DELAY):
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
    sent_orders = []
    with connect_with_retry(create_session) as session:
        logging.info("Producer session established.")
        session.open_queue(QUEUE_URI, read=False, write=True, options=blazingmq.QueueOptions())
        logging.info("Queue opened for producing.")

        for i in range(NUM_MESSAGES):
            symbol = random.choice(STOCK_SYMBOLS)
            base_price = PRICE_MAP[symbol]
            price = base_price + random.randint(-PRICE_VARIANCE, PRICE_VARIANCE)
            order = {
                "type": "BUY_ORDER",
                "symbol": symbol,
                "qty": ORDER_QTY,
                "price": price,
                "exchange": "NSE",
                "msg_id": i
            }
            payload = json.dumps(order).encode()
            start_time = time.time()
            session.post(QUEUE_URI, payload)
            message_times.append(time.time() - start_time)
            sent_orders.append(order)
            logging.info(f"Posted: {order}")
            time.sleep(0.5)

        session.close_queue(QUEUE_URI)
        
        # Save sent orders to JSON
        with open("sent_orders.json", "w") as f:
            json.dump(sent_orders, f, indent=2)
        logging.info("Saved sent orders to sent_orders.json")
        
        return message_times, sent_orders

# -------------------
# Consumer
# -------------------
def on_message(msg, handle):
    try:
        data = json.loads(msg.data.decode())
        logging.info(f"Received: {data} | GUID: {msg.guid} | Queue: {msg.queue_uri}")
        # Save received orders
        with open("received_orders.json", "a") as f:
            json.dump(data, f, indent=2)
            f.write("\n")
        handle.confirm()
    except Exception as e:
        logging.error(f"Failed to process message: {e}")

def consumer():
    def create_session():
        return blazingmq.Session(
            on_session_event=blazingmq.session_events.log_session_event,
            on_message=on_message
        )
    with connect_with_retry(create_session) as session:
        logging.info("Consumer session established.")
        session.open_queue(
            QUEUE_URI,
            read=True,
            write=False,
            options=blazingmq.QueueOptions(
                max_unconfirmed_messages=1000,
                max_unconfirmed_bytes=1000000,
                consumer_priority=1
            )
        )
        logging.info("Consumer waiting for messages...")
        while not stop_event.is_set():
            time.sleep(1)
        session.close_queue(QUEUE_URI)

# -------------------
# Run consumer and producer
# -------------------
if __name__ == "__main__":
    # Clear received orders file
    open("received_orders.json", "w").close()
    
    # Start consumer thread
    consumer_thread = threading.Thread(target=consumer, daemon=True)
    consumer_thread.start()
    time.sleep(1)

    # Run producer
    try:
        message_times, sent_orders = producer()
    except Exception as e:
        logging.error(f"Producer failed: {e}")
        message_times, sent_orders = [], []

    # Wait for consumer to process messages
    time.sleep(2)
    stop_event.set()
    consumer_thread.join()
    logging.info("Finished all operations.")

    # -------------------
    # Compute statistics
    # -------------------
    if message_times:
        stats = {
            "mean_latency_ms": np.mean(message_times) * 1000,
            "median_latency_ms": np.median(message_times) * 1000,
            "min_latency_ms": min(message_times) * 1000,
            "max_latency_ms": max(message_times) * 1000,
            "std_latency_ms": np.std(message_times) * 1000
        }
        with open("latency_stats.json", "w") as f:
            json.dump(stats, f, indent=2)
        logging.info(f"Latency stats: {stats}")

    # -------------------
    # Plot latency (Matplotlib)
    # -------------------
    if plt and message_times:
        # Enhanced latency line plot
        plt.figure(figsize=(10, 5))
        colors = {"RELIANCE": "#1f77b4", "HDFCBANK": "#ff7f0e", "TCS": "#2ca02c"}
        markers = {"RELIANCE": "o", "HDFCBANK": "s", "TCS": "^"}
        for i, (time, order) in enumerate(zip(message_times, sent_orders)):
            symbol = order["symbol"]
            plt.plot(i + 1, time * 1000, marker=markers[symbol], color=colors[symbol], label=symbol if i == 0 or order["symbol"] != sent_orders[i-1]["symbol"] else "")
            plt.text(i + 1, time * 1000, f"{time*1000:.2f}ms", fontsize=8, ha="center", va="bottom")
        plt.yscale("log")
        plt.xlabel("Message Number")
        plt.ylabel("Time to Post (ms)")
        plt.title("BlazingMQ Message Posting Latency (Log Scale)")
        plt.grid(True, which="both", ls="--")
        plt.legend()
        plt.tight_layout()
        plt.savefig("blazingmq_latency.png", dpi=300)
        plt.show()

        # Bar chart for average latency per symbol
        avg_latencies = {symbol: [] for symbol in STOCK_SYMBOLS}
        for time, order in zip(message_times, sent_orders):
            avg_latencies[order["symbol"]].append(time * 1000)
        avg_latencies = {k: np.mean(v) for k, v in avg_latencies.items() if v}
        
        plt.figure(figsize=(8, 5))
        plt.bar(avg_latencies.keys(), avg_latencies.values(), color=[colors[s] for s in avg_latencies.keys()])
        plt.xlabel("Stock Symbol")
        plt.ylabel("Average Latency (ms)")
        plt.title("Average Message Posting Latency by Stock")
        plt.grid(True, axis="y")
        plt.tight_layout()
        plt.savefig("blazingmq_avg_latency.png", dpi=300)
        plt.show()

        # Histogram of latencies
        plt.figure(figsize=(8, 5))
        plt.hist([t * 1000 for t in message_times], bins=10, color="#1f77b4", edgecolor="black")
        plt.xlabel("Latency (ms)")
        plt.ylabel("Frequency")
        plt.title("Distribution of Message Posting Latencies")
        plt.grid(True, axis="y")
        plt.tight_layout()
        plt.savefig("blazingmq_latency_hist.png", dpi=300)
        plt.show()

    # -------------------
    # Chart.js config for web
    # -------------------
    chartjs_config = {
        "type": "line",
        "data": {
            "labels": list(range(1, len(message_times) + 1)),
            "datasets": [{
                "label": "Message Posting Latency",
                "data": [t * 1000 for t in message_times],
                "borderColor": "#1f77b4",
                "backgroundColor": "rgba(31, 119, 180, 0.2)",
                "fill": False,
                "tension": 0.1,
                "pointRadius": 5,
                "pointHoverRadius": 8
            }]
        },
        "options": {
            "responsive": True,
            "plugins": {
                "title": {"display": True, "text": "BlazingMQ Message Posting Latency"},
                "legend": {"display": True}
            },
            "scales": {
                "x": {"title": {"display": True, "text": "Message Number"}},
                "y": {
                    "title": {"display": True, "text": "Time to Post (ms)"},
                    "beginAtZero": True,
                    "type": "logarithmic"
                }
            }
        }
    }
    with open("blazingmq_latency_chartjs.json", "w") as f:
        json.dump(chartjs_config, f, indent=2)
    logging.info("Saved Chart.js config to blazingmq_latency_chartjs.json")
