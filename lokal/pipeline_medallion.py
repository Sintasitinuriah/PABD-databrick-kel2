"""Pipeline medallion Bronze-Silver-Gold berbasis Delta Lake untuk studi kasus gempa NTT."""
import argparse
import json
import sys
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

DELTA = Path("/delta")
DATA = Path("/workspace/data")
BUKTI = Path("/workspace/bukti")
DERIVED = DATA / "derived"

BBOX_ID = (-12.0, 6.0, 94.0, 141.0)
BBOX_NTT = (-11.5, -7.5, 118.0, 126.0)


def buat_spark(app="Praktikum Delta - Kelompok 2"):
    return (SparkSession.builder
            .master("local[2]")
            .appName(app)
            .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.4.0")
            .config("spark.jars.ivySettings", "/workspace/lokal/ivysettings.xml")
            .config("spark.driver.extraJavaOptions",
                    "-Divy.cache.dir=/tmp/ivy -Divy.home=/tmp/ivy -Dio.netty.tryReflectionSetAccessible=true")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.driver.memory", "2g")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate())


def path_tabel(lapisan, nama):
    return str(DELTA / lapisan / nama)


def simpan_evidence(nama, teks):
    BUKTI.mkdir(parents=True, exist_ok=True)
    (BUKTI / nama).write_text(teks, encoding="utf-8")
    print(f"  bukti -> bukti/{nama}")


def tambah_metadata(df, sumber, batch_id):
    kolom_asli = df.columns
    return (df
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source", F.lit(sumber))
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_checksum", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in kolom_asli]), 256)))


def tulis_delta(df, lapisan, nama, mode="overwrite"):
    p = path_tabel(lapisan, nama)
    df.write.format("delta").mode(mode).save(p)
    n = spark_baca(lapisan, nama).count()
    print(f"  {lapisan}.{nama}: {n} baris -> {p}")
    return n


def spark_baca(lapisan, nama, versi=None):
    from pyspark.sql import SparkSession as S
    spark = S.getActiveSession()
    r = spark.read.format("delta")
    if versi is not None:
        r = r.option("versionAsOf", versi)
    return r.load(path_tabel(lapisan, nama))


def siapkan_bmkg_jsonl():
    """Pecah payload BMKG menjadi satu event per baris; berkas asli tetap utuh di Bronze."""
    DERIVED.mkdir(parents=True, exist_ok=True)
    out = DERIVED / "bmkg_events.jsonl"
    n = 0
    with open(out, "w", encoding="utf-8") as f:
        for pth in sorted((DATA / "raw").glob("bmkg_*.json")):
            payload = json.loads(pth.read_text(encoding="utf-8"))
            gempa = payload.get("Infogempa", {}).get("gempa", [])
            if isinstance(gempa, dict):
                gempa = [gempa]
            for ev in gempa:
                ev = dict(ev)
                ev["_endpoint"] = pth.stem.replace("bmkg_", "")
                f.write(json.dumps(ev, ensure_ascii=False) + "\n")
                n += 1
    print(f"  turunan BMKG: {n} event -> data/derived/bmkg_events.jsonl")
    return out, n


def lapisan_bronze(spark, batch_id):
    print("\n=== BRONZE: payload mentah, append-only ===")
    hasil = {}

    rows = []
    for pth in sorted((DATA / "raw").glob("bmkg_*.json")):
        rows.append((pth.name, pth.read_text(encoding="utf-8")))
    if rows:
        df = spark.createDataFrame(rows, ["nama_berkas", "payload"])
        hasil["sumber_mentah"] = tulis_delta(tambah_metadata(df, "BMKG TEWS (berkas utuh)", batch_id),
                                            "bronze", "sumber_mentah")

    out, n_ev = siapkan_bmkg_jsonl()
    if n_ev:
        df = spark.read.option("multiLine", False).json(str(out))
        hasil["gempa_bmkg"] = tulis_delta(tambah_metadata(df, "BMKG TEWS", batch_id), "bronze", "gempa_bmkg")

    p_usgs = DATA / "usgs_indonesia.csv"
    if p_usgs.exists():
        df = (spark.read.option("header", True).option("inferSchema", False).csv(str(p_usgs)))
        hasil["gempa_usgs"] = tulis_delta(tambah_metadata(df, "USGS FDSN", batch_id), "bronze", "gempa_usgs")

    p_wil = DATA / "ref_wilayah_ntt.csv"
    if p_wil.exists():
        df = spark.read.option("header", True).option("inferSchema", False).csv(str(p_wil))
        hasil["ref_wilayah"] = tulis_delta(tambah_metadata(df, "referensi wilayah NTT", batch_id), "bronze", "ref_wilayah")

    p_edge = DATA / "laporan_edge.csv"
    if p_edge.exists():
        df = spark.read.option("header", True).option("inferSchema", False).csv(str(p_edge))
        hasil["laporan_edge"] = tulis_delta(tambah_metadata(df, "Edge Node BPBD (sintetis)", batch_id), "bronze", "laporan_edge")

    simpan_evidence("M3_bronze_ringkasan.json", json.dumps(hasil, indent=2))
    return hasil


def _alasan_gempa(df):
    lat_min, lat_max, lon_min, lon_max = BBOX_ID
    return (F.when(F.col("waktu_utc").isNull(), F.lit("waktu_tidak_terbaca"))
            .when(F.col("lat").isNull() | F.col("lon").isNull(), F.lit("koordinat_kosong"))
            .when((F.col("lat") < lat_min) | (F.col("lat") > lat_max) |
                  (F.col("lon") < lon_min) | (F.col("lon") > lon_max), F.lit("koordinat_di_luar_cakupan"))
            .when(F.col("magnitudo").isNull() | (F.col("magnitudo") < 0) | (F.col("magnitudo") > 10),
                  F.lit("magnitudo_tidak_valid"))
            .otherwise(F.lit(None).cast("string")))


def lapisan_silver(spark, batch_id):
    print("\n=== SILVER: validasi skema, dedup, rekonsiliasi revisi ===")
    hasil = {}
    quarantine_parts = []

    # --- Gempa: BMKG + USGS disatukan ke skema kanonik ---
    bagian = []
    try:
        bm = spark_baca("bronze", "gempa_bmkg")
        koordinat = F.split(F.col("Coordinates"), ",")
        bagian.append(bm.select(
            F.concat(F.lit("BMKG-"), F.col("DateTime")).alias("event_id"),
            F.lit("BMKG").alias("sumber"),
            F.to_timestamp(F.col("DateTime")).alias("waktu_utc"),
            koordinat.getItem(0).cast("double").alias("lat"),
            koordinat.getItem(1).cast("double").alias("lon"),
            F.regexp_replace(F.col("Kedalaman"), "[^0-9.]", "").cast("double").alias("kedalaman_km"),
            F.col("Magnitude").cast("double").alias("magnitudo"),
            F.col("Wilayah").alias("wilayah_teks"),
            F.lit("BMKG").alias("mag_type"),
            F.col("_checksum").alias("_checksum_sumber")))
    except Exception as e:
        print(f"  [LEWATI] Bronze gempa_bmkg tidak ada: {e}")
    try:
        us = spark_baca("bronze", "gempa_usgs")
        bagian.append(us.select(
            F.col("id").alias("event_id"),
            F.lit("USGS").alias("sumber"),
            F.to_timestamp(F.col("time")).alias("waktu_utc"),
            F.col("latitude").cast("double").alias("lat"),
            F.col("longitude").cast("double").alias("lon"),
            F.col("depth").cast("double").alias("kedalaman_km"),
            F.col("mag").cast("double").alias("magnitudo"),
            F.col("place").alias("wilayah_teks"),
            F.col("magType").alias("mag_type"),
            F.col("_checksum").alias("_checksum_sumber")))
    except Exception as e:
        print(f"  [LEWATI] Bronze gempa_usgs tidak ada: {e}")

    if bagian:
        gempa = bagian[0]
        for b in bagian[1:]:
            gempa = gempa.unionByName(b)
        gempa = gempa.withColumn("_alasan", _alasan_gempa(gempa))
        w = Window.partitionBy("event_id").orderBy(F.col("waktu_utc").desc_nulls_last())
        gempa = (gempa.withColumn("_rn", F.row_number().over(w))
                 .withColumn("_duplikat", F.when(F.col("_rn") > 1, F.lit("duplikat_event_id")).otherwise(F.lit(None).cast("string")))
                 .drop("_rn"))
        gempa = gempa.withColumn("_alasan_final", F.coalesce(F.col("_alasan"), F.col("_duplikat")))

        valid = gempa.filter(F.col("_alasan_final").isNull()).drop("_alasan", "_duplikat", "_alasan_final")
        hasil["gempa_tervalidasi"] = tulis_delta(valid, "silver", "gempa_tervalidasi")

        buruk = (gempa.filter(F.col("_alasan_final").isNotNull())
                 .select(F.lit("gempa").alias("jenis_record"), F.col("event_id").alias("record_id"),
                         F.col("sumber"), F.col("_alasan_final").alias("alasan"),
                         F.to_json(F.struct("lat", "lon", "magnitudo", "kedalaman_km")).alias("payload_ringkas"),
                         F.current_timestamp().alias("_quarantined_at"), F.lit(batch_id).alias("_batch_id")))
        quarantine_parts.append(buruk)

    # --- Laporan edge: batch 1 (revisi 1) lalu batch 2 (revisi terlambat) via MERGE ---
    from delta.tables import DeltaTable
    try:
        edge = spark_baca("bronze", "laporan_edge")
        edge = (edge
                .withColumn("revision", F.col("revision").cast("int"))
                .withColumn("jiwa_terdampak", F.col("jiwa_terdampak").cast("long"))
                .withColumn("lat", F.col("lat").cast("double"))
                .withColumn("lon", F.col("lon").cast("double"))
                .withColumn("event_time", F.to_timestamp("event_time"))
                .withColumn("received_at", F.to_timestamp("received_at")))
        lat_min, lat_max, lon_min, lon_max = BBOX_NTT
        edge = edge.withColumn("_alasan",
            F.when(F.trim(F.coalesce(F.col("reporter_id"), F.lit(""))) == "", F.lit("identitas_pelapor_kosong"))
            .when(F.col("lat").isNull() | F.col("lon").isNull(), F.lit("koordinat_kosong"))
            .when((F.col("lat") < lat_min) | (F.col("lat") > lat_max) |
                  (F.col("lon") < lon_min) | (F.col("lon") > lon_max), F.lit("koordinat_di_luar_NTT"))
            .when(F.col("jiwa_terdampak").isNull() | (F.col("jiwa_terdampak") < 0), F.lit("jiwa_terdampak_tidak_valid"))
            .otherwise(F.lit(None).cast("string")))

        bersih = edge.filter(F.col("_alasan").isNull()).drop("_alasan")
        buruk_edge = (edge.filter(F.col("_alasan").isNotNull())
                      .select(F.lit("laporan_edge").alias("jenis_record"), F.col("report_id").alias("record_id"),
                              F.col("_source").alias("sumber"), F.col("_alasan").alias("alasan"),
                              F.to_json(F.struct("lat", "lon", "reporter_id", "jiwa_terdampak")).alias("payload_ringkas"),
                              F.current_timestamp().alias("_quarantined_at"), F.lit(batch_id).alias("_batch_id")))
        quarantine_parts.append(buruk_edge)

        # Duplikat persis (checksum sama) dibuang, dicatat sebagai alasan.
        wd = Window.partitionBy("_checksum").orderBy("received_at")
        bersih = bersih.withColumn("_rn", F.row_number().over(wd))
        dup = (bersih.filter(F.col("_rn") > 1)
               .select(F.lit("laporan_edge").alias("jenis_record"), F.col("report_id").alias("record_id"),
                       F.col("_source").alias("sumber"), F.lit("duplikat_persis").alias("alasan"),
                       F.to_json(F.struct("revision", "received_at")).alias("payload_ringkas"),
                       F.current_timestamp().alias("_quarantined_at"), F.lit(batch_id).alias("_batch_id")))
        quarantine_parts.append(dup)
        bersih = bersih.filter(F.col("_rn") == 1).drop("_rn")

        # Konflik: report_id + revision sama tetapi isi berbeda.
        # Spark menolak countDistinct di dalam window, jadi dihitung lewat agregasi lalu di-join.
        n_versi = (bersih.groupBy("report_id", "revision")
                   .agg(F.countDistinct("_checksum").alias("_n_versi")))
        konflik_keys = n_versi.filter(F.col("_n_versi") > 1).select("report_id", "revision")
        konflik = bersih.join(konflik_keys, ["report_id", "revision"], "inner")
        if konflik.count() > 0:
            quarantine_parts.append(konflik.select(
                F.lit("laporan_edge").alias("jenis_record"), F.col("report_id").alias("record_id"),
                F.col("_source").alias("sumber"), F.lit("konflik_revisi_isi_berbeda").alias("alasan"),
                F.to_json(F.struct("revision", "status_jalan", "jiwa_terdampak", "_checksum")).alias("payload_ringkas"),
                F.current_timestamp().alias("_quarantined_at"), F.lit(batch_id).alias("_batch_id")))
        bersih = bersih.join(konflik_keys, ["report_id", "revision"], "left_anti")

        batch1 = bersih.filter(F.col("revision") == 1)
        batch2 = bersih.filter(F.col("revision") > 1)

        p_aktif = path_tabel("silver", "laporan_edge_aktif")
        batch1.write.format("delta").mode("overwrite").save(p_aktif)
        n1 = spark_baca("silver", "laporan_edge_aktif").count()
        print(f"  silver.laporan_edge_aktif batch-1 (revisi 1): {n1} baris")

        dt = DeltaTable.forPath(spark, p_aktif)
        (dt.alias("s").merge(batch2.alias("t"), "s.report_id = t.report_id")
         .whenMatchedUpdate(condition="s.revision < t.revision", set={c: F.col(f"t.{c}") for c in batch2.columns})
         .whenNotMatchedInsert(values={c: F.col(f"t.{c}") for c in batch2.columns})
         .execute())
        n2 = spark_baca("silver", "laporan_edge_aktif").count()
        terjalan = spark_baca("silver", "laporan_edge_aktif").filter(F.col("revision") > 1).count()
        print(f"  silver.laporan_edge_aktif setelah MERGE revisi terlambat: {n2} baris "
              f"({terjalan} baris kini revisi > 1)")
        hasil["laporan_edge_aktif"] = n2
        hasil["revisi_diterapkan"] = terjalan
        hasil["batch2_baris"] = batch2.count()
    except Exception as e:
        print(f"  [GAGAL] Silver laporan_edge: {type(e).__name__}: {e}")
        hasil["gagal_edge"] = f"{type(e).__name__}: {e}"

    if quarantine_parts:
        q = quarantine_parts[0]
        for p in quarantine_parts[1:]:
            q = q.unionByName(p)
        hasil["quarantine"] = tulis_delta(q, "silver", "quarantine")
        print("\n  Alasan quarantine:")
        for r in spark_baca("silver", "quarantine").groupBy("alasan").count().orderBy(F.desc("count")).collect():
            print(f"    {r['alasan']:<32} {r['count']}")

    simpan_evidence("M3_silver_ringkasan.json", json.dumps(hasil, indent=2))
    return hasil


def lapisan_gold(spark, batch_id):
    print("\n=== GOLD: tampilan operasional untuk keputusan ===")
    hasil = {}
    gempa = spark_baca("silver", "gempa_tervalidasi")
    wil = (spark_baca("bronze", "ref_wilayah")
           .select(F.col("wilayah_id"), F.col("nama_wilayah"),
                   F.col("lat_pusat").cast("double").alias("lat_w"),
                   F.col("lon_pusat").cast("double").alias("lon_w"),
                   F.col("populasi").cast("long").alias("populasi"),
                   F.col("indeks_risiko").cast("double").alias("indeks_risiko")))

    lat_min, lat_max, lon_min, lon_max = BBOX_NTT
    ntt = gempa.filter((F.col("lat") >= lat_min) & (F.col("lat") <= lat_max) &
                       (F.col("lon") >= lon_min) & (F.col("lon") <= lon_max))
    t_max = ntt.agg(F.max("waktu_utc").alias("t")).collect()[0]["t"]
    jendela_hari = 30
    potong = t_max
    recent = ntt.filter(F.col("waktu_utc") >= F.date_sub(F.lit(potong.date()), jendela_hari))
    if recent.count() == 0:
        jendela_hari = 365
        recent = ntt.filter(F.col("waktu_utc") >= F.date_sub(F.lit(potong.date()), jendela_hari))
    print(f"  jendela waktu operasional: {jendela_hari} hari terakhir s.d. {potong} "
          f"({recent.count()} gempa NTT)")
    hasil["jendela_hari"] = jendela_hari
    hasil["gempa_ntt_total"] = ntt.count()
    hasil["gempa_ntt_jendela"] = recent.count()

    # Spatial join sederhana: wilayah terdekat berdasarkan jarak derajat (bukan proyeksi resmi).
    gab = recent.crossJoin(wil).withColumn(
        "jarak_derajat", F.sqrt(F.pow(F.col("lat") - F.col("lat_w"), 2) + F.pow(F.col("lon") - F.col("lon_w"), 2)))
    w = Window.partitionBy("event_id").orderBy("jarak_derajat")
    terdekat = (gab.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1)
                .filter(F.col("jarak_derajat") <= 1.5))
    agg_gempa = (terdekat.groupBy("wilayah_id")
                 .agg(F.count("*").alias("jumlah_gempa"),
                      F.max("magnitudo").alias("mag_maks"),
                      F.min("kedalaman_km").alias("kedalaman_min_km"),
                      F.max("waktu_utc").alias("gempa_terakhir")))

    aktif = spark_baca("silver", "laporan_edge_aktif")
    agg_lap = (aktif.groupBy("wilayah_id")
               .agg(F.count("*").alias("jumlah_laporan"),
                    F.sum("jiwa_terdampak").alias("jiwa_terdampak"),
                    F.sum(F.when(F.col("status_jalan") == "putus", 1).otherwise(0)).alias("jalan_putus"),
                    F.sum(F.when(F.col("status_jalan") == "terbatas", 1).otherwise(0)).alias("jalan_terbatas")))

    prio = (wil.join(agg_gempa, "wilayah_id", "left").join(agg_lap, "wilayah_id", "left")
            .fillna(0, subset=["jumlah_gempa", "jumlah_laporan", "jiwa_terdampak", "jalan_putus", "jalan_terbatas"])
            .withColumn("skor_prioritas", F.round(
                F.coalesce(F.col("mag_maks"), F.lit(0.0)) * 12.0
                + F.col("indeks_risiko") * 20.0
                + F.log1p(F.col("populasi").cast("double")) * 2.0
                + F.col("jalan_putus") * 8.0 + F.col("jalan_terbatas") * 3.0
                + F.log1p(F.col("jiwa_terdampak").cast("double")) * 4.0, 2))
            .withColumn("_dihitung_pada", F.current_timestamp())
            .withColumn("_batch_id", F.lit(batch_id))
            .orderBy(F.desc("skor_prioritas")))
    hasil["prioritas_area"] = tulis_delta(prio, "gold", "prioritas_area")

    akses = (aktif.groupBy("wilayah_id", "jenis_fasilitas")
             .agg(F.count("*").alias("jumlah_fasilitas"),
                  F.sum(F.when(F.col("status_jalan") == "putus", 1).otherwise(0)).alias("akses_putus"),
                  F.sum(F.when(F.col("status_jalan") == "terbatas", 1).otherwise(0)).alias("akses_terbatas"),
                  F.sum(F.when(F.col("status_jalan") == "lancar", 1).otherwise(0)).alias("akses_lancar"))
             .withColumn("_dihitung_pada", F.current_timestamp()))
    hasil["status_akses_fasilitas"] = tulis_delta(akses, "gold", "status_akses_fasilitas")

    rekap = (aktif.groupBy("wilayah_id", "kebutuhan_utama")
             .agg(F.count("*").alias("jumlah_laporan"), F.sum("jiwa_terdampak").alias("jiwa_terdampak"))
             .withColumn("_dihitung_pada", F.current_timestamp()))
    hasil["rekap_bantuan"] = tulis_delta(rekap, "gold", "rekap_bantuan")

    print("\n  5 area prioritas teratas:")
    top = spark_baca("gold", "prioritas_area").select(
        "nama_wilayah", "jumlah_gempa", "mag_maks", "jalan_putus", "jiwa_terdampak", "skor_prioritas").limit(5)
    for r in top.collect():
        print(f"    {r['nama_wilayah']:<24} gempa={r['jumlah_gempa']:<3} mag={r['mag_maks']} "
              f"jalan_putus={r['jalan_putus']:<3} jiwa={r['jiwa_terdampak']:<7} skor={r['skor_prioritas']}")
    teks = "\n".join([f"{r['nama_wilayah']}\t{r['jumlah_gempa']}\t{r['mag_maks']}\t{r['jalan_putus']}\t{r['jiwa_terdampak']}\t{r['skor_prioritas']}" for r in top.collect()])
    simpan_evidence("M3_gold_top5.tsv", "nama_wilayah\tjumlah_gempa\tmag_maks\tjalan_putus\tjiwa_terdampak\tskor_prioritas\n" + teks + "\n")
    simpan_evidence("M3_gold_ringkasan.json", json.dumps(hasil, indent=2, default=str))
    return hasil


def verifikasi(spark):
    print("\n=== VERIFIKASI GERBANG M3 ===")
    ok = True
    wajib = [("bronze", "gempa_usgs"), ("bronze", "gempa_bmkg"), ("bronze", "laporan_edge"),
             ("silver", "gempa_tervalidasi"), ("silver", "laporan_edge_aktif"), ("silver", "quarantine"),
             ("gold", "prioritas_area"), ("gold", "status_akses_fasilitas"), ("gold", "rekap_bantuan")]
    ring = {}
    for lap, nama in wajib:
        try:
            n = spark_baca(lap, nama).count()
            ring[f"{lap}.{nama}"] = n
            print(f"  {lap}.{nama:<24} {n:>8} baris")
        except Exception as e:
            ok = False
            print(f"  {lap}.{nama:<24} TIDAK ADA ({type(e).__name__})")

    try:
        q = spark_baca("silver", "quarantine")
        if q.count() == 0:
            ok = False
            print("  [GAGAL] quarantine kosong, seharusnya ada record tertolak")
        buruk = spark_baca("silver", "gempa_tervalidasi").filter(
            (F.col("lat") < BBOX_ID[0]) | (F.col("lat") > BBOX_ID[1]) |
            (F.col("lon") < BBOX_ID[2]) | (F.col("lon") > BBOX_ID[3])).count()
        if buruk:
            ok = False
            print(f"  [GAGAL] {buruk} baris Silver di luar cakupan koordinat")
        else:
            print("  [OK] semua koordinat Silver dalam cakupan Indonesia")
        dup = spark_baca("silver", "gempa_tervalidasi").groupBy("event_id").count().filter("count > 1").count()
        if dup:
            ok = False
            print(f"  [GAGAL] {dup} event_id duplikat lolos ke Silver")
        else:
            print("  [OK] tidak ada event_id duplikat di Silver")
        rev = spark_baca("silver", "laporan_edge_aktif").filter(F.col("revision") > 1).count()
        if rev == 0:
            ok = False
            print("  [GAGAL] tidak ada revisi terlambat yang diterapkan")
        else:
            print(f"  [OK] {rev} laporan revisi terlambat berhasil direkonsiliasi")
        p = spark_baca("gold", "prioritas_area")
        if p.count() == 0:
            ok = False
            print("  [GAGAL] gold.prioritas_area kosong")
        else:
            atas = p.orderBy(F.desc("skor_prioritas")).first()
            print(f"  [OK] prioritas teratas: {atas['nama_wilayah']} (skor {atas['skor_prioritas']})")
    except Exception as e:
        ok = False
        print(f"  [GAGAL] pemeriksaan lanjutan: {e}")

    simpan_evidence("M3_verifikasi.json", json.dumps({"lulus": ok, "jumlah_baris": ring}, indent=2))
    print("\nGERBANG M3:", "LULUS" if ok else "GAGAL")
    return ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lapisan", choices=["bronze", "silver", "gold", "semua"], default="semua")
    ap.add_argument("--verify", action="store_true")
    ap.add_argument("--batch-id", default="BATCH-20261111-01")
    args = ap.parse_args()

    spark = buat_spark()
    spark.sparkContext.setLogLevel("ERROR")
    DELTA.mkdir(parents=True, exist_ok=True)
    try:
        if args.verify:
            return 0 if verifikasi(spark) else 1
        if args.lapisan in ("bronze", "semua"):
            lapisan_bronze(spark, args.batch_id)
        if args.lapisan in ("silver", "semua"):
            lapisan_silver(spark, args.batch_id)
        if args.lapisan in ("gold", "semua"):
            lapisan_gold(spark, args.batch_id)
        if args.lapisan == "semua":
            return 0 if verifikasi(spark) else 1
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())