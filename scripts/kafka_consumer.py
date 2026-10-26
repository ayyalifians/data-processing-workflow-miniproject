
import argparse
import json
from pathlib import Path

import pandas as pd
from kafka import KafkaConsumer
from kafka.structs import TopicPartition, OffsetAndMetadata
from pydantic import ValidationError

# Memakai kontrak yang sudah dibuat pada Tahap 2
from validate_orders import OrderContract


BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders"

OUTPUT_DIR = Path("data/processed")
VALID_PATH = OUTPUT_DIR / "valid_events.parquet"
REJECTED_PATH = OUTPUT_DIR / "rejected_events.parquet"


def upsert_parquet(record, path):
    """Simpan atau perbarui satu record berdasarkan order_id."""
    new_df = pd.DataFrame([record])

    if path.exists():
        old_df = pd.read_parquet(path)
        combined = pd.concat(
            [old_df, new_df],
            ignore_index=True
        )
    else:
        combined = new_df

    # Upsert sederhana: order_id yang sama tidak menjadi baris ganda
    combined = combined.drop_duplicates(
        subset=["order_id"],
        keep="last"
    )

    combined.to_parquet(path, index=False)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--max-messages",
        type=int,
        default=36,
        help="Jumlah pesan yang akan diproses"
    )
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    consumer = KafkaConsumer(
        "orders",
        bootstrap_servers="localhost:9092",
        group_id="orders-validator-v1",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=30000,
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
        key_deserializer=lambda key: (
            key.decode("utf-8") if key else None
        ),
    )

    consumer.subscribe([TOPIC])

    processed = 0
    valid_count = 0
    rejected_count = 0

    try:
        print(f"[INFO] Menunggu pesan dari topic '{TOPIC}'...")

        for message in consumer:
            try:
                # Membaca payload JSON
                record = message.value

                if not isinstance(record, dict):
                    raise ValueError(
                        "Payload harus berupa objek JSON"
                    )

                # Validasi dengan kontrak Tahap 2
                order = OrderContract.model_validate(record)
                valid_record = order.model_dump(mode="json")

                upsert_parquet(valid_record, VALID_PATH)
                valid_count += 1

            except (ValidationError, ValueError, TypeError) as error:
                record = message.value

                if isinstance(record, dict):
                    rejected_record = dict(record)
                else:
                    rejected_record = {
                        "raw_payload": repr(record)
                    }

                rejected_record["rejection_reason"] = json.dumps(
                    error.errors(
                        include_url=False,
                        include_context=False
                    ) if isinstance(error, ValidationError)
                    else [{"message": str(error)}],
                    ensure_ascii=False,
                    default=str
                )

                upsert_parquet(
                    rejected_record,
                    REJECTED_PATH
                )
                rejected_count += 1

            # Catat metadata Kafka untuk bukti pemrosesan
            print(
                f"[MESSAGE] topic={message.topic}, "
                f"partition={message.partition}, "
                f"offset={message.offset}, "
                f"key={message.key!r}"
            )

            # Commit offset setelah hasil ditulis ke sink
            tp = TopicPartition(
                message.topic,
                message.partition
            )
            consumer.commit({
                tp: OffsetAndMetadata(
                    message.offset + 1,
                    ""
                )
            })

            processed += 1

            if processed >= args.max_messages:
                break

        print("\n=== HASIL CONSUMER ===")
        print(f"Pesan diproses : {processed}")
        print(f"Valid          : {valid_count}")
        print(f"Rejected       : {rejected_count}")
        print(f"File valid     : {VALID_PATH}")
        print(f"File rejected  : {REJECTED_PATH}")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
