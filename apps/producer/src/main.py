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
    return {
        "machine_id": os.getenv("MACHINE_ID", "machine-id"),
        "module": os.getenv("MODULE_ID", "module-id"),
        "slot": os.getenv("SLOT_ID", "slot-id"),
        "fifo": os.getenv("FIFO_ID", "fifo-id"),
        "order": os.getenv("ORDER_ID", "order-id"),
        "timestamp": time.time(),
        "status": random.choice(["OK", "FAIL"]),
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
