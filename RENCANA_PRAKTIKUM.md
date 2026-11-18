# Rencana Praktikum Data Storage — Databricks Lakehouse (Delta Lake)
## Studi kasus: Kerangka Kerja Hybrid-Distributed untuk Mitigasi Gempa NTT

Mata kuliah: Big Data — Program S2 · **Kelompok 2**
Tugas: Mengeksplorasi platform Big Data, mengetahui cara kerjanya, dan menyajikan cara operasionalnya dalam bentuk PPT.
Disusun: 2026-11-11 · **Revisi 3** (Databricks; jadwal dikompres karena presentasi 12 Nov 2026)

**Anggota Kelompok 2:** Alif Ilman Nafian · Beatrix Datu Sarira · Kevin Pratama · Sinta Siti Nuriah · Raoul Habonaran Purba

**Tanggal presentasi: 12 November 2026** (besok). Rencana awal menargetkan 13 Nov — tidak berlaku.

### Jadwal kompres (disetujui 2026-11-11 18:05)
| Waktu | Pekerjaan | Penanggung |
|---|---|---|
| 18:00–18:30 | M0 fondasi + M1 image diunduh & container hidup | agen |
| 18:00–18:15 | Daftar Databricks Free Edition (paralel) | Sinta |
| 18:30–19:15 | M2 data + M3 pipeline medallion | agen |
| 19:15–20:00 | M4 eksperimen storage (inti: E1, E2, E3, E6) | agen |
| 20:00–21:00 | M6 PPT 18 slide + narasi | agen |
| 21:00–21:30 | M5 screenshot cloud bila akun siap | Sinta + agen |
| sisa malam | Latihan presentasi | kelompok |

Pemotongan bila waktu mepet: E4 (`OPTIMIZE`) dan E5 (`VACUUM`) jadi opsional; empat eksperimen inti (bedah `_delta_log`, time travel, schema enforcement, pemisahan storage–compute) wajib karena menopang slide 6, 7, 11, 13, 15.

---

## 1. Keputusan yang sudah dikunci (2026-11-11)

| Aspek | Keputusan |
|---|---|
| Platform inti | **Databricks** (Data Lakehouse), fokus praktikum pada lapisan penyimpanan **Delta Lake** |
| Arsitektur | **Medallion Bronze → Silver → Gold** + jalur **Quarantine**, sesuai diagram kelompok |
| Jalur bukti | **Hybrid** — replika lokal (Spark + Delta Lake di Docker) lalu notebook yang sama di **Databricks Free Edition** |
| Dataset | **Gempa asli (BMKG + USGS)** + populasi/wilayah batch + **laporan Edge Node sintetis berlabel** |
| Durasi | **15 menit → 18 slide** |
| Identitas | **Kelompok 2** (nama anggota menyusul → placeholder) |

**Mengapa rencana HDFS dibatalkan:** dokumen kelompok sudah menetapkan Databricks sebagai platform yang dibahas. HDFS tidak dibuang percuma — fakta yang sudah terverifikasi dipakai untuk **slide pembanding** "Hadoop tradisional vs Lakehouse terkelola", yang justru memperkuat argumen di bagian 4 dokumen Anda. Tidak ada cluster Hadoop yang akan dijalankan.

## 2. Sumber materi kelompok (diperlakukan sebagai baca-saja)

`Databricks_Presentasi_Gempa_NTT.docx` sudah saya baca seluruhnya (45 blok). Isinya kuat secara konsep: pengenalan platform, pemisahan storage–compute, medallion, manajemen revisi, keunggulan operasional.

**Yang belum ada dan diminta tugas ("cara kerjanya dan bagaimana operasionalnya"):**

| Klaim di dokumen | Bukti yang akan dibuat |
|---|---|
| "menyimpan payload mentah immutable + checksum + timestamp" | tabel Bronze nyata, Delta history hanya berisi operasi APPEND |
| "validasi skema & resolusi identifier" | percobaan tulis skema salah **ditolak** Delta + baris quarantine beserta alasannya |
| "laporan terlambat direkonsiliasi tanpa merusak data" | `MERGE` laporan revisi + `DESCRIBE HISTORY` + baca versi lama |
| "Time-Travel: melihat data pukul 10:00" | `SELECT ... VERSION AS OF n` benar-benar dijalankan, hasilnya dibandingkan |
| "data disimpan format Delta Lake di cloud storage murah" | file **Parquet + `_delta_log/*.json`** dibuka dan dibedah isinya |
| "compute hanya menyala saat perlu" | container Spark **dimatikan**, data tetap ada, dinyalakan lagi → query jalan |
| "Zero-maintenance, tak perlu setel Hadoop manual" | slide pembanding + pengalaman setup nyata (angka waktu & ukuran) |

**Catatan:** `.docx` Anda tidak akan saya ubah. Semua keluaran baru dibuat sebagai file terpisah (PPTX, notebook, log). Kalau ada kalimat dokumen yang perlu diperbaiki, saya berikan lembar keputusan per kalimat untuk Anda ketik sendiri.

## 3. Fakta lingkungan yang sudah diverifikasi

| Item | Hasil | Implikasi |
|---|---|---|
| Docker | 29.7.2 + Compose v5.5.1 aktif | replika lokal bisa jalan |
| Memori VM Docker | 12,38 GB | cukup (Spark dibatasi ~3 GB) |
| Container aktif | hanya `typed-evidence-postgres` (5433) | **tidak disentuh**; lokal pakai port 8888 (Jupyter) & 4040 (Spark UI) |
| Java di Windows | **tidak ada** | semua Spark jalan di dalam container |
| Python | 3.12.4 | skrip unduh data + render diagram |
| Disk | D: 824 GB, C: 120 GB bebas | aman (image ±1,9 GB terkompresi) |
| `deltaio/delta-docker:4.4.0` | tersedia, 1.883 MB | Spark + Delta Lake + Jupyter resmi, satu image |
| **BMKG API** | **hidup** — `autogempa.json` & `gempaterkini.json` (15 event, data 11 Nov 2026) | sumber streaming nyata |
| **USGS API** | **hidup** — 55.312 event Indonesia 2000–2026 M2.5+ | sumber batch historis nyata |
| Databricks Community Edition | **sudah ditutup 2025**, diganti **Free Edition** | serverless, gratis, tanpa kartu kredit, **non-commercial**, ada kuota (1 SQL warehouse 2X-Small, maks 5 job bersamaan) |
| MinIO (object storage lokal) | **tidak tersedia** di Docker Hub / quay.io | lapisan S3 asli ditunjukkan di Databricks cloud, bukan lewat mock |
| `PPT BIG DATA.pdf` | terproteksi password | materi dosen belum bisa dibaca (butir 8) |

## 4. Risiko dan mitigasi

1. **Free Edition butuh pendaftaran & bisa kena kuota/verifikasi.** → Semua bukti inti dibuat **lokal lebih dulu** (M1–M4). Bagian cloud (M5) adalah pengaya; kalau gagal, slide 16 diganti menjadi penjelasan arsitektur + batasan Free Edition berdasarkan dokumentasi resmi, dan presentasi tetap utuh.
2. **Data populasi BPS NTT / inaRISK butuh unduhan manual.** → Saya cek dulu apakah tabelnya sudah ada di paper/artefak kelompok. Kalau tidak ada, kolom populasi diisi nilai **sintetis berlabel jelas** di manifest dan slide, bukan diklaim sebagai data BPS. Nama kabupaten/kota NTT tetap faktual.
3. **Laporan Edge Node tidak mungkin nyata.** → Sintetis dengan seed tetap, diberi kolom `_synthetic=true`, dan disebut terbuka di slide dataset. Justru ini dipakai untuk menguji kasus revisi & konflik yang jadi inti klaim dokumen.
4. **Klaim "immutable evidence" bisa menyesatkan.** → `VACUUM` menghapus file lama sehingga time travel punya batas retensi. Ini akan diuji dan disajikan sebagai **temuan jujur**, bukan disembunyikan — penting untuk konteks audit trail BPBD.
5. **Demo live saat presentasi bisa gagal.** → Semua bukti disimpan sebagai log + screenshot; demo live jadi bonus dengan cheatsheet dan rencana cadangan.
6. **Jaringan kampus/laptop memblokir API.** → Data diunduh sekali ke `data/` beserta hash; pipeline bisa jalan ulang sepenuhnya offline.

## 5. Struktur folder

```
Praktikum Data Storage/
├── RENCANA_PRAKTIKUM.md              ← dokumen ini (catatan keputusan kelompok)
├── lokal/
│   ├── docker-compose.yml            ← deltaio/delta-docker:4.4.0, port 8888 & 4040
│   └── notebooks/
│       ├── 00_setup_dan_sumber.py
│       ├── 01_bronze_ingest.py
│       ├── 02_silver_validasi.py
│       ├── 03_gold_agregasi.py
│       └── 04_eksperimen_storage.py
├── cloud/
│   ├── 01_bronze_databricks.py       ← versi notebook Databricks (siap unggah)
│   ├── 02_silver_databricks.py
│   ├── 03_gold_databricks.py
│   └── PANDUAN_FREE_EDITION.md       ← langkah daftar + unggah + jalankan (Anda yang login)
├── scripts/
│   ├── ambil_bmkg.py
│   ├── ambil_usgs.py
│   ├── buat_edge_sintetis.py
│   ├── render_diagram.py
│   └── jalankan_semua.ps1
├── data/                             ← CSV/JSON + MANIFEST.json (hash, tidak di-commit)
├── bukti/                            ← log .txt + screenshot .png (masuk commit)
├── diagram/                          ← PNG hasil render berlabel Indonesia
└── presentasi/
    ├── Databricks_Kelompok2_Gempa_NTT.pptx
    ├── NARASI_PRESENTASI.md          ← naskah bicara + timing 15 menit
    └── DEMO_CHEATSHEET.md            ← perintah demo live + rencana cadangan
```

## 6. Milestone dan gerbang verifikasi

Saya tidak lanjut ke milestone berikutnya sebelum gerbangnya lolos. Setiap gerbang punya perintah yang bisa Anda jalankan sendiri.

### M0 — Fondasi (±10 menit)
Struktur folder, `.gitignore` (kecualikan `data/`, `*.pptx` sementara), `git init` + commit baseline.
**Cek:** `git log --oneline` → 1 commit; `git status` → bersih.

### M1 — Lingkungan Delta lokal hidup (±25 menit, termasuk unduh image 1,9 GB)
**Cek:**
```powershell
docker compose -f "D:\Documents\S2\Kuliah\261\Big Data\Praktikum Data Storage\lokal\docker-compose.yml" ps
docker exec delta-lake-lab python -c "import delta, pyspark; print('delta', delta.__version__, '| spark', pyspark.__version__)"
```
**Diharapkan:** container `Up`; versi Delta 4.x & Spark 4.x tercetak; Jupyter terbuka di `http://localhost:8888`.
Bukti: `bukti/M1_versi.txt` + screenshot Spark UI.

### M2 — Data nyata + provenance (±20 menit)
BMKG ( polling `gempaterkini` + `autogempa`), USGS (per tahun, wilayah Indonesia), tabel wilayah NTT, laporan Edge sintetis ber-seed. Semua dicatat di `data/MANIFEST.json`: sumber, URL, waktu ambil, jumlah baris, SHA-256, dan status asli/sintetis.
**Cek:** `python scripts\ambil_usgs.py --verify`
**Diharapkan:** jumlah baris & hash cocok manifest; setiap file punya label `asli` atau `sintetis`.

### M3 — Pipeline medallion lokal (±45 menit)
| Lapisan | Tabel | Isi |
|---|---|---|
| Bronze | `gempa_bmkg`, `gempa_usgs`, `iot_observasi`, `laporan_edge`, `ref_wilayah` | payload mentah + `_ingested_at`, `_source`, `_checksum`, `_batch_id`; append-only |
| Silver | `gempa_tervalidasi`, `laporan_edge_aktif`, `quarantine` | validasi skema & koordinat, dedup identifier, `MERGE` revisi terlambat, konflik → quarantine beralasan |
| Gold | `prioritas_area`, `status_akses_fasilitas`, `rekap_bantuan` | spatial join gempa × wilayah × laporan → skor prioritas |

**Cek:** `docker exec delta-lake-lab python /workspace/notebooks/03_gold_agregasi.py --verify`
**Diharapkan:** jumlah baris tiap lapisan tercetak; `quarantine > 0` beserta alasan; `prioritas_area` tidak kosong dan terurut skor; tidak ada baris Silver yang gagal validasi koordinat.

### M4 — Eksperimen storage Delta (±45 menit) ← inti praktikum Data Storage
| Kode | Eksperimen | Bukti yang dihasilkan |
|---|---|---|
| E1 | Bedah fisik tabel Delta | daftar file Parquet + isi `_delta_log/000...json` (aksi `add`, `stats`, `commitInfo`) |
| E2 | Time travel & revisi laporan | `DESCRIBE HISTORY` + `VERSION AS OF` sebelum/sesudah revisi, dibandingkan berdampingan |
| E3 | Schema enforcement | tulis skema salah → **ditolak**, pesan error disimpan |
| E4 | Masalah file kecil → `OPTIMIZE` | jumlah file sebelum vs sesudah (angka nyata) |
| E5 | `VACUUM` vs audit trail | time travel gagal setelah vacuum → batas retensi terukur |
| E6 | Pemisahan storage–compute | container dimatikan → data tetap ada → hidup lagi → query jalan |

**Cek:** `powershell -File scripts\jalankan_semua.ps1 -EksperimenSaja`
**Diharapkan:** E2 versi lama ≠ versi baru dan cocok dengan laporan yang dikirim; E3 error tertangkap; E4 jumlah file turun; E5 error time travel muncul; E6 SHA-256 tabel identik sebelum/sesudah restart.
Bukti: 6 log + screenshot.

### M5 — Databricks Free Edition (±45 menit, bergantung Anda)
Anda daftar & login (saya tidak menangani kredensial). Saya siapkan 3 notebook + data untuk diunggah ke Unity Catalog Volume, lalu pandu menjalankan; screenshot UI asli (notebook, catalog, `DESCRIBE HISTORY`, hasil Gold).
**Cek:** `DESCRIBE HISTORY gold.prioritas_area` di Databricks menampilkan versi hasil `MERGE`.
**Kalau terblokir:** ditandai terbuka di slide 16, presentasi tetap lengkap dari bukti lokal.

### M6 — PPT 18 slide + narasi (±60 menit)
Diagram dirender lewat matplotlib dengan label Indonesia yang deterministik (bukan gambar AI), termasuk versi rapi dari diagram Mermaid kelompok. Bukti nyata dari M3–M5 masuk ke slide.
**Cek:** buka PPTX → 18 slide, tidak ada gambar rusak, narasi total ±15 menit.

## 7. Rancangan 18 slide (15 menit)

| # | Slide | Isi inti |
|---|---|---|
| 1 | Judul | Databricks untuk Mitigasi Gempa NTT · Kelompok 2 · Big Data |
| 2 | Masalah NTT | Multi-otoritas, laporan lapangan offline, revisi susulan, keputusan harus cepat |
| 3 | Apa itu Databricks | Platform analitik terpadu di atas Spark; posisinya vs Hadoop/HDFS, warehouse, dan lake |
| 4 | Kenapa Lakehouse | Tabel perbandingan lake vs warehouse vs lakehouse (skema, ACID, biaya, BI) |
| 5 | Arsitektur platform | Control plane vs compute plane; **pemisahan storage–compute** (diagram) |
| 6 | **Cara kerja storage** | **Delta Lake = Parquet + `_delta_log`** — potongan JSON commit nyata dari praktikum |
| 7 | ACID & konkurensi | Optimistic concurrency; kenapa laporan terlambat tidak merusak data |
| 8 | Medallion | Diagram Bronze–Silver–Gold + Quarantine (versi rapi diagram kelompok) |
| 9 | Ingestion | Auto Loader / Structured Streaming vs batch; pemetaan BMKG, IoT, BPS/inaRISK, Edge |
| 10 | Bronze | Immutable evidence: checksum, timestamp, `_source`; bukti Delta history append-only |
| 11 | Silver | Validasi skema (percobaan gagal nyata), resolusi identifier, tabel quarantine + alasan |
| 12 | Gold | Tabel prioritas area, status akses fasilitas, rekap bantuan (hasil query nyata) |
| 13 | Time travel & revisi | `DESCRIBE HISTORY` + `VERSION AS OF` berdampingan — menjawab klaim "pukul 10:00" |
| 14 | Operasional harian | Notebook, jobs, `OPTIMIZE`/`VACUUM`, retensi vs audit trail (tabel + angka E4/E5) |
| 15 | Hasil praktikum lokal | Rekap angka: baris per lapisan, file sebelum/sesudah OPTIMIZE, bukti E6 |
| 16 | Databricks Free Edition | Screenshot UI asli + batasan kuota secara jujur (atau status terblokir) |
| 17 | Keunggulan & keterbatasan | Zero-maintenance & kolaborasi vs biaya, lock-in, ketergantungan internet di NTT, node offline |
| 18 | Kesimpulan + referensi | 3 poin; dokumen kelompok, paper IEEE Access NTT, dokumentasi Databricks/Delta, BMKG/USGS |

## 8. Yang masih perlu dari Anda

1. **Persetujuan rencana revisi ini** (dan `git init` di M0).
2. **Nama anggota Kelompok 2** untuk slide judul (sementara placeholder).
3. **Tanggal pasti presentasi** minggu depan — target materi siap Jumat 2026-11-13, latihan 14–15 Nov.
4. **Kesediaan mendaftar Databricks Free Edition** untuk M5 (gratis, tanpa kartu kredit). Kalau tidak mau, katakan sekarang — M5 saya hapus dan slide 16 disesuaikan.
5. *Opsional:* `PPT BIG DATA.pdf` tanpa password, dan tabel populasi/inaRISK kalau kelompok sudah punya — supaya angka di slide Gold benar-benar data resmi.

## 9. Catatan progres

### M0 — selesai 2026-11-11 18:08
Commit baseline `1b911b4`. Struktur folder dibuat, `.gitignore` mengecualikan `data/`.

### M2 — selesai 2026-11-11 18:10, gerbang LULUS
`python scripts\siapkan_data.py --verify` → **34 berkas, 0 bermasalah, semua hash cocok.**
- USGS: **55.194 event** asli (Indonesia, 2000–2026, M2.5+), 27 potongan tahunan, gabung jadi `usgs_indonesia.csv` (9,87 MB).
  Catatan: angka `count` API menyebut 55.312; hasil unduhan CSV 55.194. Selisih 118 event kemungkinan karena batas waktu/`updated` antara dua panggilan — yang dipakai di seluruh praktikum adalah **55.194** (angka dari berkas yang di-hash).
- BMKG: `autogempa`, `gempaterkini` (15 event), `gempadirasakan` — payload mentah disimpan utuh.
- Wilayah NTT: 22 kabupaten/kota, **nama faktual**, koordinat perkiraan, **populasi & indeks risiko sintetis berlabel**.
- Laporan edge: 166 baris = 140 dasar + 8 revisi terlambat + 5 konflik + 6 koordinat invalid + 4 tanpa identitas + 3 duplikat.
- Seed sintetis: `20261112`.

### Kendala jaringan dan jalan keluar (2026-11-11 18:12–18:43)
Ini penting dicatat karena mempengaruhi cara praktikum direproduksi:

| Gejala | Penyebab | Jalan keluar |
|---|---|---|
| `docker pull` tampak macet, log kosong | log di-pipe ke `tail` sehingga ter-buffer | tulis log langsung ke berkas, jangan di-pipe |
| diagnosis awal "Docker Hub diblokir" | **keliru** — `curl` ke `auth.docker.io` gagal, tetapi Docker daemon tetap berhasil menarik image | image `deltaio/delta-docker:4.4.0` (6,33 GB) berhasil masuk; pull yang dihentikan CLI-nya tetap diselesaikan daemon |
| pull quay.io gagal | `cdn01.quay.io` tidak bisa di-resolve | jalur quay.io dibatalkan, `Dockerfile` turunannya dihapus |
| Delta jar tidak ada di image | image mengunduh jar dari Maven saat start | perlu resolusi jar terpisah |
| `repo1.maven.org`, mirror Google, `plugins.gradle.org` semua gagal DNS | pembatasan jaringan | **mirror `maven.aliyun.com` terbukti melayani jar Delta (HTTP 200)** → `lokal/ivysettings.xml` mengarahkan Ivy hanya ke sana |
| `python` tidak ada di PATH container | image hanya punya `/usr/bin/python3.10` | semua perintah memakai path penuh; `PYSPARK_PYTHON` disetel di compose |

### M1 — lingkungan hidup 2026-11-11 18:35
Container `delta-lake-lab` Up, port 4040 (Spark UI) & 8888. **Spark 4.2.0, Delta 4.4.0, Java 21.**
Volume `lokal_delta_data` terpasang di `/delta` dan bisa ditulis; proyek ter-bind di `/workspace`.
Container tesis `typed-evidence-postgres` tetap berjalan, tidak disentuh.

### M3 — pipeline medallion LULUS 2026-11-11 19:18
Gerbang `M3_verifikasi.json`: **LULUS** (9 tabel, koordinat dalam bbox, tanpa duplikat event_id,
8 revisi direkonsiliasi, prioritas teratas Manggarai 162.58).
- Bronze: sumber_mentah 3, gempa_bmkg 31, gempa_usgs 55.194, ref_wilayah 22, laporan_edge 166.
- Silver: gempa_tervalidasi 55.219; laporan_edge_aktif 135 → 136 setelah MERGE (8 baris revisi > 1);
  quarantine 29 (konflik 10, koordinat luar NTT 6, identitas kosong 4, duplikat event 4, duplikat persis 3, koordinat luar cakupan 2).
- Gold: jendela 30 hari berisi 23 gempa NTT; prioritas_area 22, status_akses_fasilitas 77, rekap_bantuan 72.
- **Bug ditemukan & diperbaiki:** `countDistinct` di dalam window ditolak Spark
  (`DISTINCT_WINDOW_FUNCTION_UNSUPPORTED`); kegagalan sempat tertelan pesan "[LEWATI] Bronze tidak ada".
  Diganti groupBy+agg+join, dan pesan kegagalan dibuat jujur (`[GAGAL] ...`).

### M4 — enam eksperimen LULUS 2026-11-11 19:57
- E1: 2 Parquet (40.845 byte) + 2 commit JSON (6.483 byte); aksi versi 0 = commitInfo/metaData/protocol/add.
- E2: RPT-0008 versi 0 (revisi 1, lancar, 1.279) vs kini (revisi 2, putus, 2.046); event_time tetap 02:58.
- E3: append bertipe salah DITOLAK (`DELTA_FAILED_TO_MERGE_FIELDS`), baris tetap 136.
- E4: 12 file → 1 file setelah OPTIMIZE, baris tetap 12. **Koreksi pengukuran:** menghitung file fisik
  di disk menyesatkan (24 → 25) karena file lama baru hilang setelah VACUUM; kini memakai
  `DeltaTable.detail().numFiles`. Ditemukan pula 12 file kosong 376 byte sisa task Spark tanpa baris.
- E5: VACUUM RETAIN 0 HOURS menghapus 8 dari 9 file fisik; versi 0 gagal dibaca
  (`FAILED_READ_FILE.FILE_NOT_EXIST`). **Koreksi:** pengecekan memakai `collect()`, bukan `count()`,
  karena COUNT bisa terjawab dari metadata tanpa menyentuh berkas.
- E6: container dimatikan lalu dinyalakan; hash 6 berkas **IDENTIK**, 22 baris terbaca,
  prioritas teratas Manggarai 162.58.

### M6 — PPT 18 slide + narasi + cheatsheet SELESAI 2026-11-11 20:10
- `presentasi/Databricks_Kelompok2_Gempa_NTT.pptx`: 18 slide, tersimpan & dibuka ulang oleh engine
  (rev 30). Semua slide punya speaker notes berisi pembicara dan durasi; total narasi 900 detik.
- Pembagian: Alif 1–4 (3:00), Beatrix 5–8 (3:40), Kevin 9–12 (3:30), Sinta 13–15 (2:40), Raoul 16–18 (2:10).
- Tabel Gold slide 12 adalah tabel native PowerPoint (bukan gambar), bisa diedit langsung.
- **Tinjauan visual dilakukan** lewat ekspor PNG PowerPoint (`scripts/export_preview.ps1`,
  hasil di `presentasi/preview_png/`). Dua cacat nyata ditemukan lalu diperbaiki dan diverifikasi
  ulang pada ekspor kedua: (1) baris isi tabel Gold berlatar gelap sehingga teks gelap tidak terbaca
  → shading sel isi diputihkan; (2) baris `_checksum` di slide 10 terpotong tepi kotak kode
  → line spacing paragraf kode dirapatkan ke 140%.
- `presentasi/NARASI_PRESENTASI.md`: naskah ucap 18 slide + antisipasi 5 pertanyaan penguji.
- `presentasi/DEMO_CHEATSHEET.md`: 4 demo live beserta perintah PowerShell/docker, keluaran yang
  diharapkan, dan fallback ke berkas bukti. Demo 1 & 2 sudah diuji hidup terhadap container.
- M5 (screenshot UI Databricks Free Edition) masih MENUNGGU pendaftaran akun oleh Sinta;
  slide 16 menyatakan status ini secara jujur dan memakai diagram 05 sebagai pengganti sementara.