from airflow.sdk import DAG, task
from airflow.models import Variable
from kafka import KafkaConsumer
from minio import Minio
import json
import logging
from datetime import datetime, timedelta

# default arguments for production readiness
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="bronze_load_production_logs",
    default_args=default_args,
    schedule_interval='*/5 * * * *',
    catchup=False,
    tags=["bronze", "kafka", "minio"],
) as dag:

    @task()
    def load_kafka_to_minio():
        # configuration from Airflow Variables (or fallbacks)
        kafka_servers = Variable.get(
            "kafka_bootstrap_servers", default_var="localhost:9092")
        topic = Variable.get("kafka_topic", "production-log")
        minio_endpoint = Variable.get("minio_endpoint", "localhost:9000")
        minio_access = Variable.get("minio_access_key", "minioadmin")
        minio_secret = Variable.get("minio_secret_key", "minioadmin")
        bucket = Variable.get("minio_bucket", "bronze")

        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=kafka_servers.split(","),
            enable_auto_commit=False,
            auto_offset_reset='earliest',
            consumer_timeout_ms=30000,
        )

        minio_client = Minio(
            minio_endpoint,
            access_key=minio_access,
            secret_key=minio_secret,
            secure=False,  # toggle if using TLS
        )

        # ensure bucket exists
        if not minio_client.bucket_exists(bucket):
            minio_client.make_bucket(bucket)

        errors = []
        for message in consumer:
            try:
                payload = message.value.decode('utf-8')
                data = json.loads(payload)
                key = f"logs/partition={message.partition}/offset={message.offset}.json"
                body = json.dumps(data).encode('utf-8')
                minio_client.put_object(bucket, key, body, length=len(body))

                # commit offset only after successful write
                consumer.commit()
            except Exception as exc:  # catch broad exceptions to avoid stopping the loop
                logging.exception(
                    "failed to process kafka record at %s:%s", message.partition, message.offset)
                errors.append((message, str(exc)))
                # here you could route the bad record to a DLQ topic or store in an error bucket

        consumer.close()
        if errors:
            # raise to mark the task as failed and trigger retry
            raise Exception(
                f"{len(errors)} messages failed, see logs for details")

    load_kafka_to_minio()
