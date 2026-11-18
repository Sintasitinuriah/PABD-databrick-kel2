# Databricks notebook source
# MAGIC %md
# MAGIC # Praktikum Data Storage — Databricks Lakehouse (Kelompok 2)
# MAGIC **Studi kasus: mitigasi gempa NTT.** Pipeline medallion Bronze → Silver → Gold dengan Delta Lake.
# MAGIC Notebook ini identik secara logika dengan replika lokal (`lokal/pipeline_medallion.py`), sehingga angka
# MAGIC di kedua lingkungan bisa dibandingkan. Data gempa asli (USGS/BMKG); laporan lapangan sintetis berlabel.

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "kelompok2"
VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/data_praktikum"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

import os
from pyspark.sql import Window, functions as F

BERKAS = ["usgs_indonesia.csv", "laporan_edge.csv", "ref_wilayah_ntt.csv", "bmkg_gempaterkini.json"]
print("Berkas di Volume:")
tersedia = {}
for b in BERKAS:
    p = f"{VOLUME}/{b}"
    ada = os.path.exists(p)
    tersedia[b] = ada
    print(f"  {'[ADA]' if ada else '[BELUM ADA]'} {b}")
if not tersedia["usgs_indonesia.csv"]:
    raise FileNotFoundError(
        f"usgs_indonesia.csv belum ada di {VOLUME}. Ikuti Langkah 2-3 di PANDUAN_FREE_EDITION.md")

BATCH_ID = "BATCH-CLOUD-20261111"
BBOX_ID = (-12.0, 6.0, 94.0, 141.0)
BBOX_NTT = (-11.5, -7.5, 118.0, 126.0)


def metadata(df, sumber):
    cols = df.columns
    return (df.withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source", F.lit(sumber))
            .withColumn("_batch_id", F.lit(BATCH_ID))
            .withColumn("_checksum", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]), 256)))


def tabel(nama):
    return f"{CATALOG}.{SCHEMA}.{nama}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## BRONZE — payload mentah, append-only
# MAGIC Disimpan apa adanya plus waktu masuk, sumber, batch, dan checksum per baris. Tidak ada pembersihan di sini.

# COMMAND ----------

usgs = spark.read.option("header", True).option("inferSchema", False).csv(f"{VOLUME}/usgs_indonesia.csv")
metadata(usgs, "USGS FDSN").write.format("delta").mode("overwrite").saveAsTable(tabel("bronze_gempa_usgs"))

wil = spark.read.option("header", True).option("inferSchema", False).csv(f"{VOLUME}/ref_wilayah_ntt.csv")
metadata(wil, "referensi wilayah NTT").write.format("delta").mode("overwrite").saveAsTable(tabel("bronze_ref_wilayah"))

edge = spark.read.option("header", True).option("inferSchema", False).csv(f"{VOLUME}/laporan_edge.csv")
metadata(edge, "Edge Node BPBD (sintetis)").write.format("delta").mode("overwrite").saveAsTable(tabel("bronze_laporan_edge"))

if tersedia["bmkg_gempaterkini.json"]:
    mentah = spark.read.text(f"{VOLUME}/bmkg_gempaterkini.json", wholetext=True)
    metadata(mentah.withColumnRenamed("value", "payload"), "BMKG TEWS (berkas utuh)") \
        .write.format("delta").mode("overwrite").saveAsTable(tabel("bronze_sumber_mentah"))
    bm = spark.read.option("multiLine", True).json(f"{VOLUME}/bmkg_gempaterkini.json")
    ev = bm.select(F.explode(F.col("Infogempa.gempa")).alias("g")).select("g.*")
    metadata(ev, "BMKG TEWS").write.format("delta").mode("overwrite").saveAsTable(tabel("bronze_gempa_bmkg"))

for t in ["bronze_gempa_usgs", "bronze_ref_wilayah", "bronze_laporan_edge"]:
    print(f"{t}: {spark.table(tabel(t)).count()} baris")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SILVER — validasi skema, dedup, rekonsiliasi revisi terlambat
# MAGIC Record yang gagal validasi tidak dibuang diam-diam, tapi masuk **quarantine** beserta alasannya.

# COMMAND ----------

us = spark.table(tabel("bronze_gempa_usgs"))
gempa = us.select(
    F.col("id").alias("event_id"), F.lit("USGS").alias("sumber"),
    F.to_timestamp("time").alias("waktu_utc"),
    F.col("latitude").cast("double").alias("lat"), F.col("longitude").cast("double").alias("lon"),
    F.col("depth").cast("double").alias("kedalaman_km"), F.col("mag").cast("double").alias("magnitudo"),
    F.col("place").alias("wilayah_teks"), F.col("magType").alias("mag_type"))

if spark.catalog.tableExists(tabel("bronze_gempa_bmkg")):
    bm = spark.table(tabel("bronze_gempa_bmkg"))
    k = F.split(F.col("Coordinates"), ",")
    gempa = gempa.unionByName(bm.select(
        F.concat(F.lit("BMKG-"), F.col("DateTime")).alias("event_id"), F.lit("BMKG").alias("sumber"),
        F.to_timestamp("DateTime").alias("waktu_utc"),
        k.getItem(0).cast("double").alias("lat"), k.getItem(1).cast("double").alias("lon"),
        F.regexp_replace(F.col("Kedalaman"), "[^0-9.]", "").cast("double").alias("kedalaman_km"),
        F.col("Magnitude").cast("double").alias("magnitudo"),
        F.col("Wilayah").alias("wilayah_teks"), F.lit("BMKG").alias("mag_type")))

lat_min, lat_max, lon_min, lon_max = BBOX_ID
gempa = gempa.withColumn("_alasan",
    F.when(F.col("waktu_utc").isNull(), F.lit("waktu_tidak_terbaca"))
    .when(F.col("lat").isNull() | F.col("lon").isNull(), F.lit("koordinat_kosong"))
    .when((F.col("lat") < lat_min) | (F.col("lat") > lat_max) | (F.col("lon") < lon_min) | (F.col("lon") > lon_max),
          F.lit("koordinat_di_luar_cakupan"))
    .when(F.col("magnitudo").isNull() | (F.col("magnitudo") < 0) | (F.col("magnitudo") > 10), F.lit("magnitudo_tidak_valid"))
    .otherwise(F.lit(None).cast("string")))
w = Window.partitionBy("event_id").orderBy(F.col("waktu_utc").desc_nulls_last())
gempa = gempa.withColumn("_rn", F.row_number().over(w)) \
             .withColumn("_alasan", F.coalesce(F.col("_alasan"), F.when(F.col("_rn") > 1, F.lit("duplikat_event_id"))))

gempa.filter(F.col("_alasan").isNull()).drop("_alasan", "_rn") \
    .write.format("delta").mode("overwrite").saveAsTable(tabel("silver_gempa_tervalidasi"))

q_gempa = gempa.filter(F.col("_alasan").isNotNull()).select(
    F.lit("gempa").alias("jenis_record"), F.col("event_id").alias("record_id"), F.col("sumber"),
    F.col("_alasan").alias("alasan"), F.to_json(F.struct("lat", "lon", "magnitudo")).alias("payload_ringkas"),
    F.current_timestamp().alias("_quarantined_at"), F.lit(BATCH_ID).alias("_batch_id"))

print("silver_gempa_tervalidasi:", spark.table(tabel("silver_gempa_tervalidasi")).count(), "baris")

# COMMAND ----------

from delta.tables import DeltaTable

e = (spark.table(tabel("bronze_laporan_edge"))
     .withColumn("revision", F.col("revision").cast("int"))
     .withColumn("jiwa_terdampak", F.col("jiwa_terdampak").cast("long"))
     .withColumn("lat", F.col("lat").cast("double")).withColumn("lon", F.col("lon").cast("double"))
     .withColumn("event_time", F.to_timestamp("event_time")).withColumn("received_at", F.to_timestamp("received_at")))
nlat_min, nlat_max, nlon_min, nlon_max = BBOX_NTT
e = e.withColumn("_alasan",
    F.when(F.trim(F.coalesce(F.col("reporter_id"), F.lit(""))) == "", F.lit("identitas_pelapor_kosong"))
    .when((F.col("lat") < nlat_min) | (F.col("lat") > nlat_max) | (F.col("lon") < nlon_min) | (F.col("lon") > nlon_max),
          F.lit("koordinat_di_luar_NTT"))
    .when(F.col("jiwa_terdampak").isNull() | (F.col("jiwa_terdampak") < 0), F.lit("jiwa_terdampak_tidak_valid"))
    .otherwise(F.lit(None).cast("string")))

q_edge = e.filter(F.col("_alasan").isNotNull()).select(
    F.lit("laporan_edge").alias("jenis_record"), F.col("report_id").alias("record_id"),
    F.col("_source").alias("sumber"), F.col("_alasan").alias("alasan"),
    F.to_json(F.struct("lat", "lon", "reporter_id")).alias("payload_ringkas"),
    F.current_timestamp().alias("_quarantined_at"), F.lit(BATCH_ID).alias("_batch_id"))

bersih = e.filter(F.col("_alasan").isNull()).drop("_alasan")
wd = Window.partitionBy("_checksum").orderBy("received_at")
bersih = bersih.withColumn("_rn", F.row_number().over(wd))
q_dup = bersih.filter(F.col("_rn") > 1).select(
    F.lit("laporan_edge").alias("jenis_record"), F.col("report_id").alias("record_id"),
    F.col("_source").alias("sumber"), F.lit("duplikat_persis").alias("alasan"),
    F.to_json(F.struct("revision", "received_at")).alias("payload_ringkas"),
    F.current_timestamp().alias("_quarantined_at"), F.lit(BATCH_ID).alias("_batch_id"))
bersih = bersih.filter(F.col("_rn") == 1).drop("_rn")

wg = Window.partitionBy("report_id", "revision")
konflik_keys = bersih.withColumn("_nv", F.countDistinct("_checksum").over(wg)) \
                     .filter(F.col("_nv") > 1).select("report_id", "revision").distinct()
q_konflik = bersih.join(konflik_keys, ["report_id", "revision"], "inner").select(
    F.lit("laporan_edge").alias("jenis_record"), F.col("report_id").alias("record_id"),
    F.col("_source").alias("sumber"), F.lit("konflik_revisi_isi_berbeda").alias("alasan"),
    F.to_json(F.struct("revision", "status_jalan", "jiwa_terdampak")).alias("payload_ringkas"),
    F.current_timestamp().alias("_quarantined_at"), F.lit(BATCH_ID).alias("_batch_id"))
bersih = bersih.join(konflik_keys, ["report_id", "revision"], "left_anti")

q_semua = q_gempa.unionByName(q_edge).unionByName(q_dup).unionByName(q_konflik)
q_semua.write.format("delta").mode("overwrite").saveAsTable(tabel("silver_quarantine"))

batch1 = bersih.filter(F.col("revision") == 1)
batch2 = bersih.filter(F.col("revision") > 1)
batch1.write.format("delta").mode("overwrite").saveAsTable(tabel("silver_laporan_edge_aktif"))
print("batch-1 (revisi 1):", spark.table(tabel("silver_laporan_edge_aktif")).count(), "baris")

dt = DeltaTable.forName(spark, tabel("silver_laporan_edge_aktif"))
(dt.alias("s").merge(batch2.alias("t"), "s.report_id = t.report_id")
 .whenMatchedUpdate(condition="s.revision < t.revision", set={c: F.col(f"t.{c}") for c in batch2.columns})
 .whenNotMatchedInsert(values={c: F.col(f"t.{c}") for c in batch2.columns})
 .execute())
aktif = spark.table(tabel("silver_laporan_edge_aktif"))
print("setelah MERGE revisi terlambat:", aktif.count(), "baris;",
      aktif.filter(F.col("revision") > 1).count(), "baris kini revisi > 1")
print("\nAlasan quarantine:")
spark.table(tabel("silver_quarantine")).groupBy("alasan").count().orderBy(F.desc("count")).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## GOLD — tampilan operasional untuk pengambil keputusan
# MAGIC Spatial join sederhana: gempa NTT dipetakan ke kabupaten/kota terdekat, digabung dengan populasi,
# MAGIC indeks risiko, dan laporan akses jalan dari lapangan.

# COMMAND ----------

g = spark.table(tabel("silver_gempa_tervalidasi"))
wil = (spark.table(tabel("bronze_ref_wilayah"))
       .select("wilayah_id", "nama_wilayah",
               F.col("lat_pusat").cast("double").alias("lat_w"), F.col("lon_pusat").cast("double").alias("lon_w"),
               F.col("populasi").cast("long").alias("populasi"), F.col("indeks_risiko").cast("double").alias("indeks_risiko")))
nlat_min, nlat_max, nlon_min, nlon_max = BBOX_NTT
ntt = g.filter((F.col("lat") >= nlat_min) & (F.col("lat") <= nlat_max) &
               (F.col("lon") >= nlon_min) & (F.col("lon") <= nlon_max))
t_max = ntt.agg(F.max("waktu_utc").alias("t")).collect()[0]["t"]
hari = 30
recent = ntt.filter(F.col("waktu_utc") >= F.date_sub(F.lit(t_max.date()), hari))
if recent.count() == 0:
    hari = 365
    recent = ntt.filter(F.col("waktu_utc") >= F.date_sub(F.lit(t_max.date()), hari))
print(f"jendela operasional: {hari} hari s.d. {t_max} | {recent.count()} gempa NTT")

gab = recent.crossJoin(wil).withColumn("jarak", F.sqrt(F.pow(F.col("lat") - F.col("lat_w"), 2) + F.pow(F.col("lon") - F.col("lon_w"), 2)))
terdekat = gab.withColumn("_rn", F.row_number().over(Window.partitionBy("event_id").orderBy("jarak"))) \
              .filter((F.col("_rn") == 1) & (F.col("jarak") <= 1.5))
agg_g = terdekat.groupBy("wilayah_id").agg(F.count("*").alias("jumlah_gempa"),
                                          F.max("magnitudo").alias("mag_maks"),
                                          F.min("kedalaman_km").alias("kedalaman_min_km"),
                                          F.max("waktu_utc").alias("gempa_terakhir"))
agg_l = aktif.groupBy("wilayah_id").agg(F.count("*").alias("jumlah_laporan"),
                                       F.sum("jiwa_terdampak").alias("jiwa_terdampak"),
                                       F.sum(F.when(F.col("status_jalan") == "putus", 1).otherwise(0)).alias("jalan_putus"),
                                       F.sum(F.when(F.col("status_jalan") == "terbatas", 1).otherwise(0)).alias("jalan_terbatas"))
prio = (wil.join(agg_g, "wilayah_id", "left").join(agg_l, "wilayah_id", "left")
        .fillna(0, subset=["jumlah_gempa", "jumlah_laporan", "jiwa_terdampak", "jalan_putus", "jalan_terbatas"])
        .withColumn("skor_prioritas", F.round(
            F.coalesce(F.col("mag_maks"), F.lit(0.0)) * 12.0 + F.col("indeks_risiko") * 20.0
            + F.log1p(F.col("populasi").cast("double")) * 2.0
            + F.col("jalan_putus") * 8.0 + F.col("jalan_terbatas") * 3.0
            + F.log1p(F.col("jiwa_terdampak").cast("double")) * 4.0, 2))
        .withColumn("_dihitung_pada", F.current_timestamp()).withColumn("_batch_id", F.lit(BATCH_ID))
        .orderBy(F.desc("skor_prioritas")))
prio.write.format("delta").mode("overwrite").saveAsTable(tabel("gold_prioritas_area"))

(aktif.groupBy("wilayah_id", "jenis_fasilitas")
 .agg(F.count("*").alias("jumlah_fasilitas"),
      F.sum(F.when(F.col("status_jalan") == "putus", 1).otherwise(0)).alias("akses_putus"),
      F.sum(F.when(F.col("status_jalan") == "terbatas", 1).otherwise(0)).alias("akses_terbatas"),
      F.sum(F.when(F.col("status_jalan") == "lancar", 1).otherwise(0)).alias("akses_lancar"))
 .write.format("delta").mode("overwrite").saveAsTable(tabel("gold_status_akses_fasilitas")))

(aktif.groupBy("wilayah_id", "kebutuhan_utama")
 .agg(F.count("*").alias("jumlah_laporan"), F.sum("jiwa_terdampak").alias("jiwa_terdampak"))
 .write.format("delta").mode("overwrite").saveAsTable(tabel("gold_rekap_bantuan")))

print("gold_prioritas_area:", spark.table(tabel("gold_prioritas_area")).count(), "baris")
display(spark.table(tabel("gold_prioritas_area")).select(
    "nama_wilayah", "jumlah_gempa", "mag_maks", "jalan_putus", "jiwa_terdampak", "skor_prioritas").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bukti operasional: riwayat versi dan time travel
# MAGIC `MERGE` tadi menghasilkan versi baru. Versi lama tetap bisa dibaca — inilah dasar klaim
# MAGIC "melihat kembali kondisi data sebelum laporan susulan masuk".

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY workspace.kelompok2.silver_laporan_edge_aktif

# COMMAND ----------

rid = spark.table(tabel("silver_laporan_edge_aktif")).filter(F.col("revision") > 1).select("report_id").first()["report_id"]
print("Laporan yang direvisi:", rid)
print("\n--- VERSI 0 (sebelum laporan susulan masuk) ---")
spark.sql(f"SELECT report_id, revision, status_jalan, jiwa_terdampak, event_time, received_at "
          f"FROM {tabel('silver_laporan_edge_aktif')} VERSION AS OF 0 WHERE report_id = '{rid}'").show(truncate=False)
print("--- VERSI KINI (setelah rekonsiliasi) ---")
spark.sql(f"SELECT report_id, revision, status_jalan, jiwa_terdampak, event_time, received_at "
          f"FROM {tabel('silver_laporan_edge_aktif')} WHERE report_id = '{rid}'").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ringkasan untuk slide
# MAGIC Angka ini dibandingkan dengan hasil replika lokal pada slide 15.

# COMMAND ----------

ringkasan = {}
for t in ["bronze_gempa_usgs", "bronze_laporan_edge", "silver_gempa_tervalidasi",
          "silver_laporan_edge_aktif", "silver_quarantine", "gold_prioritas_area",
          "gold_status_akses_fasilitas", "gold_rekap_bantuan"]:
    try:
        ringkasan[t] = spark.table(tabel(t)).count()
    except Exception as ex:
        ringkasan[t] = f"TIDAK ADA: {type(ex).__name__}"
print("RINGKASAN JUMLAH BARIS (cloud):")
for k, v in ringkasan.items():
    print(f"  {k:<32} {v}")
print("\nJendela waktu operasional:", hari, "hari | gempa NTT dalam jendela:", recent.count())
print("Skor tertinggi:", spark.table(tabel("gold_prioritas_area")).first()["skor_prioritas"])