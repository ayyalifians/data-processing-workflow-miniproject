import csv
import json
import argparse
from pathlib import Path

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders"
SOURCE_PATH = Path("data/orders.csv")


def ensure_topic():
    admin = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id="orders-topic-admin"
    )

    try:
        admin.create_topics([
            NewTopic(
                name=TOPIC,
                num_partitions=3,
                replication_factor=1
            )
        ])
        print(f"[INFO] Topic '{TOPIC}' berhasil dibuat.")

    except TopicAlreadyExistsError:
        print(f"[INFO] Topic '{TOPIC}' sudah tersedia.")

    finally:
        admin.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--duplicate-first",
        action="store_true",
        help="Kirim ulang event pertama untuk demonstrasi duplikasi"
    )
    args = parser.parse_args()

    if not SOURCE_PATH.exists():
        raise FileNotFoundError(
            f"CSV tidak ditemukan: {SOURCE_PATH}"
        )

    with SOURCE_PATH.open(
        "r", encoding="utf-8-sig", newline=""
    ) as file:
        records = list(csv.DictReader(file))

    if len(records) < 20:
        raise ValueError(
            f"Minimal 20 event diperlukan, ditemukan {len(records)}"
        )

    ensure_topic()

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(
            value, ensure_ascii=False
        ).encode("utf-8"),
        acks="all",
        retries=5
    )

    try:
        sent = 0

        for record in records:
            order_id = record.get("order_id", "").strip()

            producer.send(
                TOPIC,
                key=order_id or None,
                value=record
            )
            sent += 1

        # Menunggu agar semua pesan yang dikirim selesai diproses broker
        producer.flush()

        print(f"[INFO] Total event dikirim: {sent}")

        if args.duplicate_first:
            producer.send(
                TOPIC,
                key=records[0].get("order_id", "").strip() or None,
                value=records[0]
            )
            producer.flush()
            print(
                "[INFO] Event pertama dikirim ulang "
                "untuk pengujian duplikasi."
            )

    finally:
        producer.close()


if __name__ == "__main__":
    main()
