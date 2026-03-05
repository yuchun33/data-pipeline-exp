import argparse
import json
import logging
import os
import random
import signal
import sys
import time
from typing import Any, Dict
from kafka import KafkaProducer


# -------- configuration ---------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Kafka production log producer")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
                        help="comma-separated list of Kafka brokers")
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "production-log"),
                        help="topic to publish to")
    parser.add_argument("--interval", type=float, default=float(os.getenv("PRODUCER_INTERVAL", "5")),
                        help="seconds between messages")
    return parser.parse_args()


# -------- logging ---------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


# -------- producer helpers ------------------------------------------------

def make_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        linger_ms=5,
        retries=5,
        acks="all",
    )


def on_send_success(record_metadata: Any) -> None:
    logger.debug("message sent to %s partition %s offset %s", record_metadata.topic,
                 record_metadata.partition, record_metadata.offset)


def on_send_error(excp) -> None:
    logger.error("send failed: %s", excp)


def build_message() -> Dict[str, Any]:
    work_orders = [f"WO-{i:03d}" for i in range(1, 21)]
    # Distribute 20 work orders across 4 machines with 25 slot-fifo combinations each
    work_order_config = {
        work_orders[i]: {
            "machine_id": f"machine-{i // 5}",
            "module": f"module-{i // 5}",
            "slot": f"slot-{(i // 5) * 25 + (i % 5) * 5}",
            "fifo": f"fifo-{(i // 5) * 25 + (i % 5) * 5 + (i % 5)}"
        }
        for i in range(len(work_orders))
    }
    
    chosen_order = random.choice(work_orders)
    config = work_order_config[chosen_order]
    
    return {
        "machine_id": os.getenv("MACHINE_ID", config["machine_id"]),
        "module": os.getenv("MODULE_ID", config["module"]),
        "slot": os.getenv("SLOT_ID", config["slot"]),
        "fifo": os.getenv("FIFO_ID", config["fifo"]),
        "order": chosen_order,
        "timestamp": time.time(),
        "status": "FAIL" if random.random() < 0.05 else "OK",
    }


# -------- graceful shutdown ------------------------------------------------

running = True


def handle_signal(signum, frame):
    global running
    logger.info("received signal %s, shutting down", signum)
    running = False


for sig in (signal.SIGINT, signal.SIGTERM):
    signal.signal(sig, handle_signal)


# -------- main ----------------------------------------------------------------

def main() -> None:
    args = parse_args()

    producer = make_producer(args.bootstrap_servers)
    logger.info("starting producer to %s on %s",
                args.topic, args.bootstrap_servers)

    try:
        while running:
            msg = build_message()
            future = producer.send(
                args.topic, key=msg["machine_id"], value=msg)
            future.add_callback(on_send_success)
            future.add_errback(on_send_error)

            # optionally track metrics here
            time.sleep(args.interval)
    except Exception as e:  # pragma: no cover - incidental
        logger.exception("unexpected error: %s", e)
    finally:
        logger.info("flushing and closing producer")
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
