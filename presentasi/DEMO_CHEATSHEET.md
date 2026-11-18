# Cheatsheet Demo Live — Presentasi 12 November 2026

Panduan untuk pembicara yang memegang laptop saat demo. Semua perintah PowerShell-friendly
dan bisa dijalankan dari folder proyek. **Jangan menjalankan demo bila waktu tersisa < 3
menit** — pakai slide 15 sebagai gantinya dan sebut bahwa bukti ada di folder `bukti/`.

Prasyarat: Docker Desktop hidup, container `delta-lake-lab` Up.
Cek cepat: `docker ps --filter name=delta-lake-lab`

---

## Demo 1 — Time travel (slide 13, ±40 dtk)

Tujuan: menunjukkan versi 0 dan versi kini berbeda untuk laporan yang sama.

```powershell
docker exec delta-lake-lab bash -lc "/usr/bin/python3.10 -c \"
import sys; sys.path.insert(0,'/workspace/lokal')
from pipeline_medallion import buat_spark, spark_baca
from pyspark.sql import functions as F
s = buat_spark('demo'); s.sparkContext.setLogLevel('ERROR')
k = ['report_id','revision','status_jalan','jiwa_terdampak']
print('VERSI 0 :', spark_baca('silver','laporan_edge_aktif',versi=0).filter(F.col('report_id')=='RPT-0008').select(*k).first())
print('KINI    :', spark_baca('silver','laporan_edge_aktif').filter(F.col('report_id')=='RPT-0008').select(*k).first())
s.stop()\""
```

Output yang diharapkan:
```
VERSI 0 : Row(report_id='RPT-0008', revision=1, status_jalan='lancar', jiwa_terdampak=1279)
KINI    : Row(report_id='RPT-0008', revision=2, status_jalan='putus', jiwa_terdampak=2046)
```
Kalimat pengiring: "Baris lama tidak ditimpa — ia menjadi versi 0 yang tetap bisa dibaca."
Bila gagal: buka `bukti/M4_E2_time_travel.txt` dan bacakan dua baris perbandingannya.

## Demo 2 — Fisik tabel Delta (slide 6, ±30 dtk)

```powershell
docker exec delta-lake-lab bash -lc "ls -la /delta/silver/laporan_edge_aktif /delta/silver/laporan_edge_aktif/_delta_log"
```
Output yang diharapkan: 2 berkas `.snappy.parquet` dan 2 berkas JSON
`00000000000000000000.json`, `00000000000000000001.json`.
Kalimat pengiring: "Tabel Delta = folder Parquet + buku transaksi JSON. Tidak ada server database."
Bila gagal: tunjuk diagram pada slide 6.

## Demo 3 — OPTIMIZE (slide 14, ±40 dtk, opsional)

```powershell
docker exec delta-lake-lab bash -lc "cd /workspace && /usr/bin/python3.10 lokal/eksperimen_storage.py --eksperimen e4"
```
Output yang diharapkan (akhir log):
```
12 tulis kecil -> 12 file Parquet dirujuk versi terkini
setelah OPTIMIZE -> 1 file Parquet dirujuk versi terkini
E4: LULUS
```
Kalimat pengiring: "Dua belas file kecil menjadi satu, tanpa satu baris pun hilang."
Bila gagal: bacakan `bukti/M4_E4_optimize.txt`.

## Demo 4 — Storage-compute terpisah (slide 15, ±60 dtk, hanya bila waktu longgar)

**Peringatan:** ini mematikan container Spark selama ±15 detik. Jangan lakukan bila ada
proses lain sedang berjalan di container. Container tesis `typed-evidence-postgres` tidak
tersentuh oleh perintah ini.

```powershell
powershell -ExecutionPolicy Bypass -File scripts\jalankan_semua.ps1 -Langkah e6
```
Output yang diharapkan (akhir):
```
hash seluruh berkas: IDENTIK
baris terbaca setelah container dimatikan lalu dinyalakan: 22
prioritas teratas: Manggarai (skor 162.58)
```
Kalimat pengiring: "Compute mati total, data identik byte-per-byte. Itulah lakehouse."
Bila gagal: bacakan `bukti/M4_E6_pemisahan_storage_compute.txt`.

---

## Urutan demo yang disarankan

1. Slide 6 → Demo 2 (cepat, aman).
2. Slide 13 → Demo 1 (inti cerita audit).
3. Slide 15 → Demo 4 bila waktu ≥ 4 menit tersisa; lewati bila tidak.
4. Demo 3 hanya bila penguji bertanya soal file kecil.

## Bila Docker tidak hidup sama sekali

Jangan mencoba menyalakannya di depan penguji (pull/start bisa makan menit). Katakan:
"Lingkungan demo kami rekam semalam; seluruh keluarannya ada di folder bukti dan bisa
diperiksa sekarang." Lalu buka berkas bukti terkait dengan editor teks.

## Berkas bukti yang dipakai slide

| Slide | Berkas |
|---|---|
| 2 | `bukti/` ringkasan USGS (lihat `RENCANA_PRAKTIKUM.md` §M2) |
| 6 | `bukti/M4_E1_bedah_delta_log.txt` |
| 10 | `bukti/M3_bronze_ringkasan.json` |
| 11 | `bukti/M3_silver_ringkasan.json` |
| 12 | `bukti/M3_gold_top5.tsv`, `bukti/M3_gold_ringkasan.json` |
| 13 | `bukti/M4_E2_time_travel.txt` |
| 14 | `bukti/M4_E4_optimize.txt`, `bukti/M4_E5_vacuum_vs_audit.txt` |
| 15 | `bukti/M3_verifikasi.json`, `bukti/M4_ringkasan.json`, `bukti/M4_E6_pemisahan_storage_compute.txt` |