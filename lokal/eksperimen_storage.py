"""Enam eksperimen lapisan penyimpanan Delta Lake: bukti fisik, time travel, skema, dan retensi."""
import argparse
import hashlib
import json
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

sys.path.insert(0, str(Path(__file__).resolve().parent))
from pipeline_medallion import DELTA, BUKTI, buat_spark, path_tabel, spark_baca, simpan_evidence

TABEL_UJI = str(DELTA / "gold" / "uji_file_kecil")
RINGKASAN = BUKTI / "M4_ringkasan.json"


def muat_ringkasan():
    if RINGKASAN.exists():
        return json.loads(RINGKASAN.read_text(encoding="utf-8"))
    return {}


def simpan_ringkasan(d):
    BUKTI.mkdir(parents=True, exist_ok=True)
    RINGKASAN.write_text(json.dumps(d, indent=2, ensure_ascii=False, default=str), encoding="utf-8")


def hitung_parquet(p):
    return sorted(x.name for x in Path(p).glob("*.parquet"))


def e1_bedah_fisik(spark, ring):
    print("\n=== E1: tabel Delta itu apa secara fisik? ===")
    p = path_tabel("silver", "laporan_edge_aktif")
    parquet = hitung_parquet(p)
    log_dir = Path(p) / "_delta_log"
    logs = sorted(x.name for x in log_dir.glob("*.json"))
    print(f"  lokasi tabel: {p}")
    print(f"  berkas data  : {len(parquet)} file Parquet")
    print(f"  buku transaksi: {len(logs)} file JSON di _delta_log/ -> {logs}")

    commit_pertama = (log_dir / logs[0]).read_text(encoding="utf-8").strip().splitlines()
    aksi = [json.loads(x) for x in commit_pertama]
    jenis = {}
    for a in aksi:
        for k in a:
            jenis[k] = jenis.get(k, 0) + 1
    print(f"  aksi pada commit versi 0: {jenis}")

    contoh_add = next((a["add"] for a in aksi if "add" in a), None)
    meta = next((a["metaData"] for a in aksi if "metaData" in a), None)
    teks = [f"LOKASI TABEL: {p}", f"JUMLAH FILE PARQUET: {len(parquet)}",
            f"FILE LOG: {logs}", f"AKSI VERSI 0: {jenis}", "",
            "--- metaData (skema tersimpan di log, bukan di file data) ---",
            json.dumps(meta, indent=2, ensure_ascii=False)[:2500], "",
            "--- add (satu file Parquet + statistik untuk data skipping) ---",
            json.dumps(contoh_add, indent=2, ensure_ascii=False)[:2500], "",
            "--- isi mentah commit versi 0 ---"] + commit_pertama[:6]
    simpan_evidence("M4_E1_bedah_delta_log.txt", "\n".join(teks))

    ukuran_data = sum((Path(p) / f).stat().st_size for f in parquet)
    ukuran_log = sum((log_dir / f).stat().st_size for f in logs)
    print(f"  ukuran data {ukuran_data:,} byte vs log {ukuran_log:,} byte")
    ring["E1"] = {"jumlah_parquet": len(parquet), "jumlah_log": len(logs),
                  "aksi_versi_0": jenis, "bytes_data": ukuran_data, "bytes_log": ukuran_log}
    return True


def e2_time_travel(spark, ring):
    print("\n=== E2: time travel & rekonsiliasi laporan terlambat ===")
    from delta.tables import DeltaTable
    p = path_tabel("silver", "laporan_edge_aktif")
    dt = DeltaTable.forPath(spark, p)
    hist = dt.history().select("version", "timestamp", "operation", "operationParameters").orderBy("version")
    baris = hist.collect()
    teks = ["version\ttimestamp\toperation"]
    for r in baris:
        teks.append(f"{r['version']}\t{r['timestamp']}\t{r['operation']}")
    print("  DESCRIBE HISTORY:")
    for t in teks[1:]:
        print("   ", t)

    aktif = spark_baca("silver", "laporan_edge_aktif")
    target = aktif.filter(F.col("revision") > 1).select("report_id").first()
    if target is None:
        print("  [GAGAL] tidak ada laporan revisi untuk dibandingkan")
        ring["E2"] = {"lulus": False, "alasan": "tidak ada revisi"}
        return False
    rid = target["report_id"]
    kolom = ["report_id", "revision", "status_jalan", "jiwa_terdampak", "event_time", "received_at"]
    lama = spark_baca("silver", "laporan_edge_aktif", versi=0).filter(F.col("report_id") == rid).select(*kolom)
    baru = aktif.filter(F.col("report_id") == rid).select(*kolom)
    rl, rb = lama.first(), baru.first()
    print(f"  laporan {rid}:")
    print(f"    versi 0 : revisi={rl['revision']} jalan={rl['status_jalan']} jiwa={rl['jiwa_terdampak']}")
    print(f"    kini    : revisi={rb['revision']} jalan={rb['status_jalan']} jiwa={rb['jiwa_terdampak']}")
    print(f"    event_time tetap {rl['event_time']} (kejadian lama), received_at maju ke {rb['received_at']}")

    sql_lama = spark.sql(f"SELECT revision, status_jalan, jiwa_terdampak FROM delta.`{p}` VERSION AS OF 0 WHERE report_id = '{rid}'").first()
    teks += ["", f"PERBANDINGAN LAPORAN {rid}",
             f"versi 0 : revisi={rl['revision']} jalan={rl['status_jalan']} jiwa={rl['jiwa_terdampak']} event_time={rl['event_time']}",
             f"versi kini: revisi={rb['revision']} jalan={rb['status_jalan']} jiwa={rb['jiwa_terdampak']} event_time={rb['event_time']}",
             f"SQL VERSION AS OF 0 -> revisi={sql_lama['revision']} jalan={sql_lama['status_jalan']} jiwa={sql_lama['jiwa_terdampak']}",
             "", "Catatan: event_time tidak berubah (kejadian memang lebih tua), yang berubah adalah",
             "received_at dan isi laporan. Laporan lama tidak dihapus, tetap bisa dibaca lewat versi."]
    simpan_evidence("M4_E2_time_travel.txt", "\n".join(teks))
    lulus = rl["revision"] == 1 and rb["revision"] > 1 and rl["jiwa_terdampak"] != rb["jiwa_terdampak"]
    ring["E2"] = {"lulus": lulus, "report_id": rid, "versi_0": dict(rl.asDict()), "versi_kini": dict(rb.asDict()),
                  "jumlah_versi": len(baris)}
    print("  [OK] versi lama dan baru berbeda, versi lama tetap terbaca" if lulus else "  [GAGAL] perbandingan tidak sesuai")
    return lulus


def e3_schema_enforcement(spark, ring):
    print("\n=== E3: Delta menolak skema yang tidak cocok ===")
    p = path_tabel("silver", "laporan_edge_aktif")
    skema_salah = StructType([
        StructField("report_id", StringType()), StructField("jiwa_terdampak", StringType()),
    ])
    df = spark.createDataFrame([("RPT-SCHEMA-01", "banyak sekali")], skema_salah)
    pesan = ""
    ditolak = False
    try:
        df.write.format("delta").mode("append").save(p)
        pesan = "TIDAK DITOLAK (tak terduga)"
    except Exception as e:
        ditolak = True
        pesan = f"{type(e).__name__}: {e}"
        print(f"  [OK] ditulis ulang dengan tipe salah -> DITOLAK")
        print(f"  {pesan.splitlines()[0][:220]}")
    n = spark_baca("silver", "laporan_edge_aktif").count()
    teks = ["PERCOBAAN: append kolom jiwa_terdampak bertipe STRING ke tabel yang kolomnya BIGINT",
            f"HASIL: {'DITOLAK' if ditolak else 'DITERIMA (tak terduga)'}", "", pesan,
            "", f"Jumlah baris tabel setelah percobaan: {n} (tidak bertambah)"]
    simpan_evidence("M4_E3_schema_enforcement.txt", "\n".join(teks))
    ring["E3"] = {"ditolak": ditolak, "baris_setelah": n, "pesan_singkat": pesan.splitlines()[0][:400] if pesan else ""}
    return ditolak


def file_aktif(spark, p):
    """Jumlah file yang benar-benar dirujuk versi terkini.

    File fisik di disk bisa lebih banyak karena OPTIMIZE hanya menandai file lama
    dengan aksi remove; berkasnya baru hilang setelah VACUUM (lihat E5).
    """
    from delta.tables import DeltaTable
    return int(DeltaTable.forPath(spark, p).detail().select("numFiles").first()["numFiles"])


def e4_file_kecil_optimize(spark, ring):
    print("\n=== E4: masalah file kecil dan OPTIMIZE ===")
    spark.sql(f"DROP TABLE IF EXISTS delta.`{TABEL_UJI}`")
    import shutil
    if Path(TABEL_UJI).exists():
        shutil.rmtree(TABEL_UJI)
    df0 = spark.range(1).withColumn("batch", F.lit(0))
    df0.write.format("delta").save(TABEL_UJI)
    for i in range(1, 12):
        spark.range(1).withColumn("batch", F.lit(i)).write.format("delta").mode("append").save(TABEL_UJI)
    sebelum = file_aktif(spark, TABEL_UJI)
    fisik_sebelum = len(hitung_parquet(TABEL_UJI))
    print(f"  12 tulis kecil -> {sebelum} file Parquet dirujuk versi terkini")
    spark.sql(f"OPTIMIZE delta.`{TABEL_UJI}`")
    sesudah = file_aktif(spark, TABEL_UJI)
    fisik_sesudah = len(hitung_parquet(TABEL_UJI))
    print(f"  setelah OPTIMIZE -> {sesudah} file Parquet dirujuk versi terkini")
    print(f"  file fisik di disk: {fisik_sebelum} -> {fisik_sesudah} (file lama baru dihapus oleh VACUUM)")
    n = spark.read.format("delta").load(TABEL_UJI).count()
    riwayat = spark.sql(f"DESCRIBE HISTORY delta.`{TABEL_UJI}`").select("version", "operation").orderBy("version").collect()
    teks = [f"12 operasi tulis kecil menghasilkan {sebelum} file Parquet (versi 0..11)",
            f"OPTIMIZE memadatkannya menjadi {sesudah} file Parquet pada versi terkini",
            f"Jumlah baris tetap {n} (tidak ada data hilang)", "",
            f"File fisik di disk: {fisik_sebelum} sebelum -> {fisik_sesudah} sesudah.",
            f"Selisih {fisik_sebelum - sebelum} berkas sebelum OPTIMIZE adalah file kosong (376 byte)",
            "sisa task Spark tanpa baris: tidak dirujuk log, ikut dibersihkan VACUUM.",
            "File lama tidak langsung dihapus: OPTIMIZE hanya menulis aksi remove di _delta_log,",
            "berkasnya baru dibuang oleh VACUUM. Karena itu versi lama masih bisa dibaca (E2/E5).", "",
            "Riwayat versi:"] + [f"  versi {r['version']}: {r['operation']}" for r in riwayat]
    simpan_evidence("M4_E4_optimize.txt", "\n".join(teks))
    ring["E4"] = {"file_sebelum": sebelum, "file_sesudah": sesudah, "baris": n,
                  "fisik_sebelum": fisik_sebelum, "fisik_sesudah": fisik_sesudah}
    return sesudah < sebelum and n == 12


def e5_vacuum_vs_audit(spark, ring):
    print("\n=== E5: VACUUM membatasi time travel (konsekuensi untuk audit trail) ===")
    import shutil
    p = str(DELTA / "gold" / "uji_retensi_audit")
    spark.sql(f"DROP TABLE IF EXISTS delta.`{p}`")
    if Path(p).exists():
        shutil.rmtree(p)
    # Versi 0 harus punya file yang sudah tidak dirujuk versi terkini, kalau tidak
    # VACUUM tidak punya apa-apa untuk dihapus. OPTIMIZE membuat file versi 0 jadi tombstone.
    spark.range(1).withColumn("batch", F.lit(0)).write.format("delta").save(p)
    for i in range(1, 4):
        spark.range(1).withColumn("batch", F.lit(i)).write.format("delta").mode("append").save(p)
    spark.sql(f"OPTIMIZE delta.`{p}`")
    fisik_sebelum = len(hitung_parquet(p))
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    # collect(), bukan count(): count bisa terjawab dari metadata tanpa menyentuh berkas.
    before = len(spark.read.format("delta").option("versionAsOf", 0).load(p).collect())
    print(f"  sebelum VACUUM: baca versi 0 berhasil ({before} baris), {fisik_sebelum} file fisik di disk")
    spark.sql(f"VACUUM delta.`{p}` RETAIN 0 HOURS")
    fisik_sesudah = len(hitung_parquet(p))
    pesan = ""
    gagal = False
    try:
        spark.read.format("delta").option("versionAsOf", 0).load(p).collect()
        pesan = "versi 0 masih terbaca (tak terduga)"
    except Exception as e:
        gagal = True
        inti = next((l.strip() for l in str(e).splitlines() if "SparkException" in l), str(e).splitlines()[0])
        pesan = f"{type(e).__name__}: {inti}"
        print("  setelah VACUUM: baca versi 0 GAGAL -> file lama sudah dihapus fisik")
        print(f"  {pesan.splitlines()[0][:200]}")
    kini = spark.read.format("delta").load(p).count()
    print(f"  file fisik: {fisik_sebelum} -> {fisik_sesudah}; data versi terkini tetap {kini} baris")
    teks = [f"Tabel uji: {p} (4 tulis kecil lalu OPTIMIZE agar file versi 0 tidak lagi dirujuk)",
            f"Sebelum VACUUM: versi 0 terbaca, {before} baris; {fisik_sebelum} file fisik di disk",
            "VACUUM RETAIN 0 HOURS dijalankan (retention check dimatikan khusus demo)",
            f"Sesudah VACUUM: {'versi 0 TIDAK bisa dibaca' if gagal else 'versi 0 masih terbaca'}; "
            f"{fisik_sesudah} file fisik di disk",
            f"Data versi terbaru tetap utuh: {kini} baris", "", pesan, "",
            "Pengecekan memakai pembacaan baris (collect), bukan COUNT: COUNT saja bisa",
            "terjawab dari metadata tanpa menyentuh berkas yang sudah dihapus.", "",
            "IMPLIKASI UNTUK BPBD/BNPB: klaim 'bukti historis abadi' hanya berlaku selama",
            "berkas belum di-VACUUM. Retensi audit harus disetel lebih panjang dari kebutuhan",
            "penelusuran, atau arsipkan versi ke penyimpanan terpisah."]
    simpan_evidence("M4_E5_vacuum_vs_audit.txt", "\n".join(teks))
    ring["E5"] = {"versi0_sebelum": before, "versi0_gagal_setelah": gagal, "baris_kini": kini,
                  "fisik_sebelum": fisik_sebelum, "fisik_sesudah": fisik_sesudah}
    return gagal and kini == 4


def _hash_tabel(p):
    out = {}
    for f in sorted(Path(p).rglob("*")):
        if f.is_file():
            out[str(f.relative_to(p)).replace("\\", "/")] = hashlib.sha256(f.read_bytes()).hexdigest()
    return out


def e6_pemisahan(spark, ring, fase):
    p = path_tabel("gold", "prioritas_area")
    kunci = BUKTI / "M4_E6_hash_sebelum.json"
    if fase == "sebelum":
        h = _hash_tabel(p)
        kunci.write_text(json.dumps(h, indent=2), encoding="utf-8")
        n = spark_baca("gold", "prioritas_area").count()
        print(f"\n=== E6 (fase sebelum): {len(h)} berkas di gold.prioritas_area, {n} baris ===")
        print("  hash disimpan. Sekarang matikan container dari host, lalu jalankan fase 'sesudah'.")
        ring["E6"] = {"fase_sebelum": {"berkas": len(h), "baris": n}}
        simpan_evidence("M4_E6_fase_sebelum.txt",
                        f"tabel: {p}\nberkas: {len(h)}\nbaris: {n}\nhash disimpan di bukti/M4_E6_hash_sebelum.json\n")
        return True
    if not kunci.exists():
        print("  [GAGAL] jalankan fase 'sebelum' lebih dulu")
        return False
    lama = json.loads(kunci.read_text(encoding="utf-8"))
    baru = _hash_tabel(p)
    sama = lama == baru
    n = spark_baca("gold", "prioritas_area").count()
    atas = spark_baca("gold", "prioritas_area").orderBy(F.desc("skor_prioritas")).first()
    print(f"\n=== E6 (fase sesudah restart): {len(baru)} berkas, {n} baris, hash {'IDENTIK' if sama else 'BERBEDA'} ===")
    print(f"  query tetap jalan tanpa compute lama: prioritas teratas {atas['nama_wilayah']} skor {atas['skor_prioritas']}")
    teks = [f"berkas sebelum: {len(lama)} | sesudah: {len(baru)}",
            f"hash seluruh berkas: {'IDENTIK' if sama else 'BERBEDA'}",
            f"baris terbaca setelah container dimatikan lalu dinyalakan: {n}",
            f"prioritas teratas: {atas['nama_wilayah']} (skor {atas['skor_prioritas']})", "",
            "Compute (Spark) mati total, data di volume tetap utuh byte-per-byte.",
            "Inilah pemisahan storage-compute yang dipakai Databricks: cluster bisa dimatikan",
            "saat idle tanpa kehilangan data, dan dinyalakan lagi saat ada pekerjaan."]
    simpan_evidence("M4_E6_pemisahan_storage_compute.txt", "\n".join(teks))
    ring["E6"] = {"hash_identik": sama, "berkas": len(baru), "baris": n}
    return sama


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--eksperimen", default="semua",
                    choices=["e1", "e2", "e3", "e4", "e5", "e6", "inti", "semua"])
    ap.add_argument("--e6-fase", choices=["sebelum", "sesudah"])
    args = ap.parse_args()

    spark = buat_spark("Eksperimen Storage Delta - Kelompok 2")
    spark.sparkContext.setLogLevel("ERROR")
    ring = muat_ringkasan()
    hasil = {}
    try:
        pilihan = args.eksperimen
        if pilihan == "e6":
            if not args.e6_fase:
                print("--e6-fase wajib (sebelum|sesudah)")
                return 2
            hasil["E6"] = e6_pemisahan(spark, ring, args.e6_fase)
        else:
            if pilihan in ("e1", "inti", "semua"):
                hasil["E1"] = e1_bedah_fisik(spark, ring)
            if pilihan in ("e2", "inti", "semua"):
                hasil["E2"] = e2_time_travel(spark, ring)
            if pilihan in ("e3", "inti", "semua"):
                hasil["E3"] = e3_schema_enforcement(spark, ring)
            if pilihan in ("e4", "semua"):
                hasil["E4"] = e4_file_kecil_optimize(spark, ring)
            if pilihan in ("e5", "semua"):
                hasil["E5"] = e5_vacuum_vs_audit(spark, ring)
        simpan_ringkasan(ring)
        print("\n=== RINGKASAN EKSPERIMEN ===")
        for k, v in hasil.items():
            print(f"  {k}: {'LULUS' if v else 'GAGAL'}")
        return 0 if all(hasil.values()) else 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())