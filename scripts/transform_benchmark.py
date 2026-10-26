
from pathlib import Path
import time

import pandas as pd
import polars as pl


# ============================================================
# 1. KONFIGURASI PATH
# ============================================================

BASE_DIR = Path(__file__).resolve().parent.parent

INPUT_FILE = BASE_DIR / "data" / "processed" / "valid_orders.parquet"
OUTPUT_DIR = BASE_DIR / "data" / "processed"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

PANDAS_OUTPUT = OUTPUT_DIR / "transformed_pandas.parquet"
POLARS_OUTPUT = OUTPUT_DIR / "transformed_polars.parquet"
BENCHMARK_OUTPUT = OUTPUT_DIR / "benchmark_results.csv"
SKEW_OUTPUT = OUTPUT_DIR / "skew_results.csv"


# ============================================================
# 2. PEMERIKSAAN INPUT
# ============================================================

if not INPUT_FILE.exists():
    raise FileNotFoundError(
        f"File input tidak ditemukan: {INPUT_FILE}"
    )

df_pd = pd.read_parquet(INPUT_FILE)

if df_pd.empty:
    raise ValueError("Data input kosong.")

required_columns = [
    "order_id",
    "customer_id",
    "order_ts",
    "product_category",
    "quantity",
    "unit_price",
    "discount_pct",
    "payment_method",
    "status",
]

missing_columns = set(required_columns) - set(df_pd.columns)

if missing_columns:
    raise ValueError(
        f"Kolom berikut tidak ditemukan: {sorted(missing_columns)}"
    )

print("=" * 60)
print("INFORMASI DATA INPUT")
print("=" * 60)
print("Jumlah baris:", len(df_pd))
print("Jumlah kolom:", len(df_pd.columns))
print(df_pd.dtypes)


# ============================================================
# 3. TRANSFORMASI MENGGUNAKAN PANDAS
# ============================================================

def transform_pandas(df):
    result = df.copy()

    result["order_ts"] = pd.to_datetime(
        result["order_ts"],
        errors="raise",
        utc=True,
    )

    result["quantity"] = result["quantity"].astype("int64")
    result["unit_price"] = result["unit_price"].astype("float64")
    result["discount_pct"] = result["discount_pct"].astype("float64")

    result["gross_amount"] = (
        result["quantity"] * result["unit_price"]
    )

    result["net_amount"] = (
        result["gross_amount"] * (1 - result["discount_pct"])
    )

    return result


start = time.perf_counter()
result_pd = transform_pandas(df_pd)
pandas_time = time.perf_counter() - start

result_pd.to_parquet(PANDAS_OUTPUT, index=False)

print("\nPANDAS")
print("Waktu transformasi:", round(pandas_time, 6), "detik")
print(result_pd[
    ["order_id", "gross_amount", "net_amount"]
].head())


# ============================================================
# 4. TRANSFORMASI MENGGUNAKAN POLARS EAGER
# ============================================================

df_pl = pl.read_parquet(INPUT_FILE)

start = time.perf_counter()

result_pl = (
    df_pl
    .with_columns([
        pl.col("order_ts").str.to_datetime(
            time_zone="UTC",
            strict=True,
        ),
        pl.col("quantity").cast(pl.Int64),
        pl.col("unit_price").cast(pl.Float64),
        pl.col("discount_pct").cast(pl.Float64),
    ])
    .with_columns([
        (
            pl.col("quantity") * pl.col("unit_price")
        ).alias("gross_amount"),
    ])
    .with_columns([
        (
            pl.col("gross_amount")
            * (1 - pl.col("discount_pct"))
        ).alias("net_amount"),
    ])
)

polars_eager_time = time.perf_counter() - start

result_pl.write_parquet(POLARS_OUTPUT)

print("\nPOLARS EAGER")
print("Waktu transformasi:", round(polars_eager_time, 6), "detik")
print(
    result_pl.select(
        ["order_id", "gross_amount", "net_amount"]
    ).head()
)


# ============================================================
# 5. POLARS LAZY EXECUTION
# ============================================================

sstart = time.perf_counter()

result_lazy = (
    pl.scan_parquet(INPUT_FILE)
    .with_columns([
        pl.col("order_ts").str.to_datetime(
            time_zone="UTC",
            strict=True,
        ),
        pl.col("quantity").cast(pl.Int64),
        pl.col("unit_price").cast(pl.Float64),
        pl.col("discount_pct").cast(pl.Float64),
    ])
    .with_columns([
        (
            pl.col("quantity") * pl.col("unit_price")
        ).alias("gross_amount"),
    ])
    .with_columns([
        (
            pl.col("gross_amount")
            * (1 - pl.col("discount_pct"))
        ).alias("net_amount"),
    ])
    .collect()
)

polars_lazy_time = time.perf_counter() - start

print("\nPOLARS LAZY")
print(
    "Waktu scan, optimasi, dan collect:",
    round(polars_lazy_time, 6),
    "detik",
)
print("Jumlah baris hasil:", result_lazy.height)


# ============================================================
# 6. VALIDASI HASIL TRANSFORMASI
# ============================================================

assert len(result_pd) == result_pl.height
assert result_pd["order_id"].tolist() == result_pl["order_id"].to_list()

assert result_pd["gross_amount"].round(8).tolist() == (
    result_pl["gross_amount"].round(8).to_list()
)

assert result_pd["net_amount"].round(8).tolist() == (
    result_pl["net_amount"].round(8).to_list()
)

print("\nValidasi Pandas vs Polars: BERHASIL")


# ============================================================
# 7. SIMPAN HASIL BENCHMARK
# ============================================================

benchmark = pd.DataFrame([
    {
        "method": "Pandas",
        "execution_time_seconds": pandas_time,
        "rows": len(df_pd),
    },
    {
        "method": "Polars Eager",
        "execution_time_seconds": polars_eager_time,
        "rows": df_pl.height,
    },
    {
        "method": "Polars Lazy",
        "execution_time_seconds": polars_lazy_time,
        "rows": result_lazy.height,
    },
])

benchmark.to_csv(BENCHMARK_OUTPUT, index=False)

print("\nHASIL BENCHMARK")
print(benchmark.to_string(index=False))


# ============================================================
# 8. EKSPERIMEN DATA SKEW
# ============================================================

# Buat data sintetis dengan customer_id dominan.
# Data asli tidak diubah.

repeat_count = 1000

skew_pd = pd.concat(
    [result_pd] * repeat_count,
    ignore_index=True,
)

dominant_customer = "CUSTOMER_DOMINAN"

# Jadikan 80% baris berasal dari satu customer.
dominant_count = int(len(skew_pd) * 0.80)

skew_pd.loc[:dominant_count - 1, "customer_id"] = dominant_customer

skew_pl = pl.from_pandas(skew_pd)

# Pandas groupby
start = time.perf_counter()

group_pd = (
    skew_pd
    .groupby("customer_id", dropna=False)
    .agg(
        total_orders=("order_id", "count"),
        total_net_amount=("net_amount", "sum"),
    )
    .reset_index()
)

skew_pandas_time = time.perf_counter() - start

# Polars group_by
start = time.perf_counter()

group_pl = (
    skew_pl
    .group_by("customer_id")
    .agg([
        pl.len().alias("total_orders"),
        pl.col("net_amount").sum().alias("total_net_amount"),
    ])
)

skew_polars_time = time.perf_counter() - start

skew_results = pd.DataFrame([
    {
        "method": "Pandas groupby",
        "rows": len(skew_pd),
        "groups": len(group_pd),
        "execution_time_seconds": skew_pandas_time,
    },
    {
        "method": "Polars group_by",
        "rows": skew_pl.height,
        "groups": group_pl.height,
        "execution_time_seconds": skew_polars_time,
    },
])

skew_results.to_csv(SKEW_OUTPUT, index=False)

print("\nEKSPERIMEN DATA SKEW")
print("Jumlah baris sintetis:", len(skew_pd))
print("Customer dominan:", dominant_customer)
print("Proporsi customer dominan:", "80%")
print(skew_results.to_string(index=False))

print("\nDistribusi 5 customer teratas:")
print(
    skew_pd["customer_id"]
    .value_counts()
    .head()
)

print("\nSemua proses selesai.")
print("File benchmark:", BENCHMARK_OUTPUT)
print("File hasil skew:", SKEW_OUTPUT)
