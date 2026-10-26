import json
from pathlib import Path
from datetime import datetime
from typing import Literal

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, ValidationError


# 1. Menentukan lokasi input dan output
INPUT_PATH = Path("data/raw/orders.parquet")
OUTPUT_DIR = Path("data/processed")

VALID_PATH = OUTPUT_DIR / "valid_orders.parquet"
REJECTED_PATH = OUTPUT_DIR / "rejected_orders.parquet"


# 2. Mendefinisikan kontrak data pesanan
class OrderContract(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    order_id: str = Field(min_length=1)
    customer_id: str = Field(min_length=1)
    order_ts: datetime
    product_category: str = Field(min_length=1)
    quantity: int = Field(gt=0)
    unit_price: float = Field(ge=0)
    discount_pct: float = Field(ge=0, le=1)
    payment_method: str = Field(min_length=1)
    status: Literal["paid", "shipped", "cancelled", "refunded"]


def main():
    # 3. Membaca data hasil ingestion
    if not INPUT_PATH.exists():
        raise FileNotFoundError(
            f"File input tidak ditemukan: {INPUT_PATH}"
        )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.read_parquet(INPUT_PATH)

    valid_records = []
    rejected_records = []

    # 4. Memvalidasi setiap record
    for record in df.to_dict(orient="records"):
        try:
            order = OrderContract.model_validate(record)

            # Simpan data valid dengan tipe data yang sudah tervalidasi
            valid_records.append(order.model_dump(mode="json"))

        except ValidationError as error:
            # Simpan data asli beserta alasan penolakannya
            rejected_records.append({
                **record,
                "rejection_reason": json.dumps(
                    error.errors(
                        include_url=False,
                        include_context=False
                    ),
                    ensure_ascii=False,
                    default=str
                )
            })

    # 5. Mengubah hasil validasi menjadi DataFrame
    valid_df = pd.DataFrame(
        valid_records,
        columns=list(OrderContract.model_fields.keys())
    )

    rejected_df = pd.DataFrame(
        rejected_records,
        columns=list(df.columns) + ["rejection_reason"]
    )

    # 6. Menyimpan hasil ke Parquet
    valid_df.to_parquet(VALID_PATH, index=False)
    rejected_df.to_parquet(REJECTED_PATH, index=False)

    # 7. Menampilkan ringkasan hasil
    total = len(df)
    valid_count = len(valid_df)
    rejected_count = len(rejected_df)

    print("\n=== HASIL VALIDASI DATA ===")
    print(f"Total record   : {total}")
    print(f"Record valid   : {valid_count}")
    print(f"Record rejected: {rejected_count}")
    print(f"Total hasil    : {valid_count + rejected_count}")
    print(f"\nFile valid     : {VALID_PATH}")
    print(f"File rejected  : {REJECTED_PATH}")

    if not rejected_df.empty:
        print("\n=== CONTOH ALASAN PENOLAKAN ===")
        print(
            rejected_df[
                ["order_id", "rejection_reason"]
            ].head(5).to_string(index=False)
        )


if __name__ == "__main__":
    main()
