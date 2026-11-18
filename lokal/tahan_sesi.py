"""Menahan sesi Spark agar Spark UI (port 4040) bisa diambil screenshot-nya."""
import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from pipeline_medallion import buat_spark, spark_baca


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--menit", type=float, default=10.0)
    args = ap.parse_args()

    spark = buat_spark("Praktikum Delta - Sesi Pantau Kelompok 2")
    spark.sparkContext.setLogLevel("ERROR")
    for lap, nama in [("bronze", "gempa_usgs"), ("silver", "gempa_tervalidasi"),
                      ("silver", "laporan_edge_aktif"), ("silver", "quarantine"),
                      ("gold", "prioritas_area")]:
        try:
            df = spark_baca(lap, nama)
            df.createOrReplaceTempView(f"{lap}_{nama}")
            print(f"  view {lap}_{nama}: {df.count()} baris", flush=True)
        except Exception as e:
            print(f"  [lewati] {lap}.{nama}: {type(e).__name__}", flush=True)

    print("\n  Spark UI: http://localhost:4040", flush=True)
    try:
        spark.sql("SELECT nama_wilayah, jumlah_gempa, mag_maks, jalan_putus, skor_prioritas "
                  "FROM gold_prioritas_area ORDER BY skor_prioritas DESC LIMIT 10").show(truncate=False)
        spark.sql("SELECT alasan, count(*) AS jumlah FROM silver_quarantine GROUP BY alasan ORDER BY jumlah DESC").show(truncate=False)
    except Exception as e:
        print(f"  [lewati] query pantau: {e}", flush=True)

    print(f"\nSesi ditahan {args.menit} menit. Tekan Ctrl+C untuk berhenti.", flush=True)
    time.sleep(args.menit * 60)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())