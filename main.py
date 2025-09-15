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
    message_symbols = []
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
            latency = time.time() - start_time
            message_times.append(latency)
            message_symbols.append(symbol)
            sent_orders.append(order)
            logging.info(f"Posted: {order} | Latency: {latency:.4f}s")
            time.sleep(0.5)

        session.close_queue(QUEUE_URI)
        
        # Save sent orders to JSON
        with open("sent_orders.json", "w") as f:
            json.dump(sent_orders, f, indent=2)
        logging.info("Saved sent orders to sent_orders.json")
        
        return message_times, message_symbols, sent_orders

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
        message_times, message_symbols, sent_orders = producer()
    except Exception as e:
        logging.error(f"Producer failed: {e}")
        message_times, message_symbols, sent_orders = [], [], []

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
    # Plotting (all graphs in one figure)
    # -------------------
    if plt and message_times:
        # Color map for symbols
        color_map = {"RELIANCE": "#1f77b4", "HDFCBANK": "#ff7f0e", "TCS": "#2ca02c"}
        colors = [color_map[s] for s in message_symbols]

        # Single figure with subplots
        fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 12), sharex=False)
        fig.suptitle("BlazingMQ Trading Desk Performance Metrics", fontsize=16)

        # 1️⃣ Bar plot
        ax1.bar(range(1, len(message_times) + 1), [t * 1000 for t in message_times], color=colors, alpha=0.7)
        mean_latency = np.mean(message_times) * 1000
        ax1.axhline(mean_latency, color='red', linestyle='--', label=f'Mean Latency: {mean_latency:.2f}ms')
        for idx, latency in enumerate(message_times):
            ax1.text(idx + 1, latency * 1000 + 0.01, f"{latency*1000:.2f}", ha='center', va='bottom', fontsize=8)
        ax1.set_xlabel("Message Number")
        ax1.set_ylabel("Time to Post (ms)")
        ax1.set_title("Message Posting Latency")
        ax1.grid(True, axis='y', linestyle='--', alpha=0.7)
        ax1.legend()

        # 2️⃣ Histogram
        ax2.hist([t * 1000 for t in message_times], bins=min(5, len(message_times)), color='skyblue', edgecolor='black')
        ax2.set_xlabel("Latency (ms)")
        ax2.set_ylabel("Frequency")
        ax2.set_title("Latency Distribution")
        ax2.grid(True, linestyle='--', alpha=0.7)

        # 3️⃣ Cumulative latency plot
        ax3.plot(range(1, len(message_times) + 1), np.cumsum([t * 1000 for t in message_times]), marker='o', linestyle='-', color='purple')
        ax3.set_xlabel("Message Number")
        ax3.set_ylabel("Cumulative Latency (ms)")
        ax3.set_title("Cumulative Message Posting Latency")
        ax3.grid(True, linestyle='--', alpha=0.7)

        plt.tight_layout(rect=[0, 0, 1, 0.95])
        plt.savefig("blazingmq_combined_plots.png", dpi=300)
        plt.show()

        # Save individual plots
        # Bar plot
        fig1 = plt.figure(figsize=(12, 6))
        plt.bar(range(1, len(message_times) + 1), [t * 1000 for t in message_times], color=colors, alpha=0.7)
        plt.axhline(mean_latency, color='red', linestyle='--', label=f'Mean Latency: {mean_latency:.2f}ms')
        for idx, latency in enumerate(message_times):
            plt.text(idx + 1, latency * 1000 + 0.01, f"{latency*1000:.2f}", ha='center', va='bottom', fontsize=8)
        plt.xlabel("Message Number")
        plt.ylabel("Time to Post (ms)")
        plt.title("BlazingMQ Message Posting Latency")
        plt.grid(True, axis='y', linestyle='--', alpha=0.7)
        plt.legend()
        plt.tight_layout()
        plt.savefig("blazingmq_latency_bar.png", dpi=300)
        plt.close(fig1)

        # Histogram
        fig2 = plt.figure(figsize=(8, 4))
        plt.hist([t * 1000 for t in message_times], bins=min(5, len(message_times)), color='skyblue', edgecolor='black')
        plt.xlabel("Latency (ms)")
        plt.ylabel("Frequency")
        plt.title("BlazingMQ Latency Distribution Histogram")
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.tight_layout()
        plt.savefig("blazingmq_latency_hist.png", dpi=300)
        plt.close(fig2)

        # Cumulative latency
        fig3 = plt.figure(figsize=(10, 4))
        plt.plot(range(1, len(message_times) + 1), np.cumsum([t * 1000 for t in message_times]), marker='o', linestyle='-', color='purple')
        plt.xlabel("Message Number")
        plt.ylabel("Cumulative Latency (ms)")
        plt.title("BlazingMQ Cumulative Message Posting Latency")
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.tight_layout()
        plt.savefig("blazingmq_cumulative_latency.png", dpi=300)
        plt.close(fig3)

    # -------------------
    # Chart.js config for web
    # -------------------
    chartjs_config = {
        "type": "bar",
        "data": {
            "labels": list(range(1, len(message_times) + 1)),
            "datasets": [{
                "label": "Message Posting Latency",
                "data": [t * 1000 for t in message_times],
                "backgroundColor": colors,
                "borderColor": colors,
                "borderWidth": 1,
                "opacity": 0.7
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
                    "beginAtZero": True
                }
            }
        }
    }
    with open("blazingmq_latency_chartjs.json", "w") as f:
        json.dump(chartjs_config, f, indent=2)
    logging.info("Saved Chart.js config to blazingmq_latency_chartjs.json")
