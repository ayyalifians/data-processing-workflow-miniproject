import duckdb
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "warehouse_dbt" / "dev.duckdb"
REJECTED_PATH = ROOT / "data" / "processed" / "rejected_orders.parquet"

con = duckdb.connect(str(DB_PATH), read_only=True)

def tampilkan(judul, query):
    print("\n" + "=" * 75)
    print(judul)
    print("=" * 75)
    hasil = con.execute(query).df()
    print(hasil.to_string(index=False))
    return hasil

# 1. Penjualan bersih per hari
tampilkan(
    "1. PENJUALAN BERSIH PER HARI",
    """
    SELECT
        CAST(order_ts AS DATE) AS tanggal,
        ROUND(SUM(net_amount), 2) AS total_penjualan_bersih
    FROM main.fct_orders
    GROUP BY CAST(order_ts AS DATE)
    ORDER BY tanggal
    """
)

# 2. Kategori produk dengan penjualan bersih tertinggi
tampilkan(
    "2. KATEGORI DENGAN PENJUALAN BERSIH TERTINGGI",
    """
    SELECT
        product_category AS kategori_produk,
        ROUND(SUM(net_amount), 2) AS total_penjualan_bersih
    FROM main.fct_orders
    GROUP BY product_category
    ORDER BY total_penjualan_bersih DESC
    """
)

# 3. Jumlah dan nilai pesanan menurut status
tampilkan(
    "3. JUMLAH DAN NILAI PESANAN MENURUT STATUS",
    """
    SELECT
        status,
        COUNT(*) AS jumlah_pesanan,
        ROUND(SUM(net_amount), 2) AS total_nilai_pesanan
    FROM main.fct_orders
    GROUP BY status
    ORDER BY jumlah_pesanan DESC
    """
)

# 4. Metode pembayaran yang paling sering digunakan
tampilkan(
    "4. FREKUENSI PENGGUNAAN METODE PEMBAYARAN",
    """
    SELECT
        payment_method AS metode_pembayaran,
        COUNT(*) AS jumlah_pesanan
    FROM main.fct_orders
    GROUP BY payment_method
    ORDER BY jumlah_pesanan DESC
    """
)

con.close()

# 5. Jumlah record yang ditolak oleh data contract dan alasannya
print("\n" + "=" * 75)
print("5. RECORD YANG DITOLAK OLEH DATA CONTRACT")
print("=" * 75)

if not REJECTED_PATH.exists():
    print(f"File tidak ditemukan: {REJECTED_PATH}")
else:
    c = duckdb.connect()
    path_sql = str(REJECTED_PATH).replace("\\", "/").replace("'", "''")
    rejected = f"read_parquet('{path_sql}')"

    # Periksa struktur kolom agar nama kolom alasan tidak ditebak.
    schema = c.execute(f"DESCRIBE SELECT * FROM {rejected}").df()
    print("Kolom pada file rejected:")
    print(schema[["column_name", "column_type"]].to_string(index=False))

    jumlah = c.execute(
        f"SELECT COUNT(*) FROM {rejected}"
    ).fetchone()[0]
    print(f"\nTotal record ditolak: {jumlah}")

    nama_kolom = schema["column_name"].tolist()
    kandidat = [
        "reason", "reasons", "error", "errors",
        "rejection_reason", "validation_error",
        "validation_errors", "error_message"
    ]
    kolom_alasan = next(
        (nama for nama in kandidat if nama in nama_kolom),
        None
    )

    if kolom_alasan:
        # Mengelompokkan berdasarkan alasan yang tercatat.
        kolom_sql = '"' + kolom_alasan.replace('"', '""') + '"'
        print(f"\nDistribusi alasan penolakan (kolom: {kolom_alasan}):")
        print(c.execute(
            f"""
            SELECT CAST({kolom_sql} AS VARCHAR) AS alasan,
                   COUNT(*) AS jumlah
            FROM {rejected}
            GROUP BY CAST({kolom_sql} AS VARCHAR)
            ORDER BY jumlah DESC
            """
        ).df().to_string(index=False))
    else:
        print(
            "\nNama kolom alasan belum dikenali otomatis. "
            "Lihat struktur kolom di atas; nama kolom tersebut "
            "perlu digunakan untuk menghitung frekuensi alasan."
        )
        print("\nContoh isi record rejected:")
        print(c.execute(
            f"SELECT * FROM {rejected} LIMIT 10"
        ).df().to_string(index=False))

    c.close()
