import argparse
import json
import time
from pathlib import Path

import pandas as pd


def get_page(df, page, page_size, retries=3, simulate_failure_page=-1):
    """Simulasi pengambilan halaman dari API lokal."""
    start = page * page_size
    last_error = None

    for attempt in range(retries):
        try:
            # Simulasikan kegagalan sementara pada percobaan pertama.
            if page == simulate_failure_page and attempt == 0:
                raise RuntimeError("Simulasi kegagalan sementara")

            return df.iloc[start:start + page_size].copy()

        except Exception as error:
            last_error = error

            if attempt == retries - 1:
                raise

            wait_seconds = 2 ** attempt
            print(
                f"[RETRY] page={page}, "
                f"attempt={attempt + 1}, "
                f"error={error}, wait={wait_seconds}s"
            )
            time.sleep(wait_seconds)

    raise last_error


def main():
    parser = argparse.ArgumentParser(
        description="Batch ingestion CSV dengan pagination dan watermark"
    )
    parser.add_argument("--source", default="data/orders.csv")
    parser.add_argument("--out", default="data/raw")
    parser.add_argument("--page-size", type=int, default=10)
    parser.add_argument("--checkpoint", default="data/watermark.json")
    parser.add_argument("--lookback-hours", type=int, default=24)
    parser.add_argument(
        "--simulate-failure-page",
        type=int,
        default=-1,
        help="Nomor halaman yang gagal sekali untuk menguji retry"
    )
    args = parser.parse_args()

    if args.page_size <= 0:
        parser.error("--page-size harus lebih besar dari 0")
    if args.lookback_hours < 0:
        parser.error("--lookback-hours tidak boleh negatif")

    source_path = Path(args.source)
    target_dir = Path(args.out)
    target_dir.mkdir(parents=True, exist_ok=True)

    checkpoint_path = Path(args.checkpoint)
    target_path = target_dir / "orders.parquet"

    # Simpan semua kolom sebagai string agar nilai sumber tetap terjaga.
    source = pd.read_csv(
        source_path,
        dtype=str,
        keep_default_na=False
    )

    required = {"order_id", "order_ts"}
    if not required.issubset(source.columns):
        raise ValueError(f"Kolom wajib tidak tersedia: {required}")

    previous_watermark = None

    if checkpoint_path.exists():
        checkpoint = json.loads(
            checkpoint_path.read_text(encoding="utf-8")
        )
        previous_watermark = checkpoint.get("watermark")

    pages = []
    page = 0

    while True:
        chunk = get_page(
            source,
            page=page,
            page_size=args.page_size,
            retries=3,
            simulate_failure_page=args.simulate_failure_page
        )

        if chunk.empty:
            break

        print(f"[PAGE] page={page}, rows={len(chunk)}")
        pages.append(chunk)
        page += 1

    extracted = (
        pd.concat(pages, ignore_index=True)
        if pages
        else source.iloc[0:0].copy()
    )

    print(f"[INFO] Total record dibaca: {len(extracted)}")

    # Kolom bantu untuk watermark, bukan untuk disimpan sebagai raw.
    parsed_ts = pd.to_datetime(
        extracted["order_ts"],
        errors="coerce",
        utc=True
    )

    if previous_watermark:
        watermark_ts = pd.to_datetime(
            previous_watermark,
            utc=True
        )
        lower_bound = watermark_ts - pd.Timedelta(
            hours=args.lookback_hours
        )

        mask = parsed_ts.isna() | (parsed_ts >= lower_bound)
        selected = extracted.loc[mask].copy()
    else:
        selected = extracted.copy()

    print(f"[INFO] Record dalam window ingestion: {len(selected)}")

    # Gabungkan dengan hasil sebelumnya agar proses dapat diulang.
    if target_path.exists():
        existing = pd.read_parquet(target_path)
        combined = pd.concat(
            [existing, selected],
            ignore_index=True
        )
    else:
        existing = pd.DataFrame(columns=source.columns)
        combined = selected.copy()

    before_dedup = len(combined)

    # Kunci bisnis: satu order_id hanya satu record.
    combined = combined.drop_duplicates(
        subset=["order_id"],
        keep="last"
    )

    added = max(0, len(combined) - len(existing))

    # Simpan raw Parquet terlebih dahulu.
    combined.to_parquet(target_path, index=False)

    # Simpan watermark setelah penulisan berhasil.
    all_ts = pd.to_datetime(
        combined["order_ts"],
        errors="coerce",
        utc=True
    )
    valid_ts = all_ts.dropna()

    if not valid_ts.empty:
        new_watermark = valid_ts.max().isoformat()
        checkpoint_path.parent.mkdir(
            parents=True,
            exist_ok=True
        )
        checkpoint_path.write_text(
            json.dumps(
                {"watermark": new_watermark},
                indent=2
            ),
            encoding="utf-8"
        )

    print(f"[INFO] Record sebelum dedup: {before_dedup}")
    print(f"[INFO] Record unik tersimpan: {len(combined)}")
    print(f"[INFO] Perkiraan record baru: {added}")
    print(f"[OUTPUT] {target_path}")

    if not valid_ts.empty:
        print(f"[WATERMARK] {valid_ts.max().isoformat()}")

    print("[DONE] Batch ingestion selesai.")


if __name__ == "__main__":
    main()