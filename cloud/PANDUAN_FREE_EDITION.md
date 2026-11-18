# Panduan Databricks Free Edition — Kelompok 2

Tujuan: mendapatkan **screenshot UI Databricks asli** untuk slide 16, dan membuktikan pipeline medallion yang sama berjalan di platform terkelola (bukan hanya replika lokal).

Perkiraan waktu: 20–30 menit. Gratis, tanpa kartu kredit. **Jangan kirimkan password ke siapa pun termasuk ke agen.**

---

## Langkah 1 — Daftar (5 menit, Anda sendiri)

1. Buka https://www.databricks.com/learn/free-edition
2. Pilih **Sign up for Free Edition**. Bisa pakai akun Google/GitHub atau email.
3. Pilih region terdekat (mis. Singapore/Australia) bila diminta.
4. Tunggu workspace selesai dibuat, lalu masuk ke workspace.

Kalau muncul verifikasi email, selesaikan dulu. Kalau pendaftaran ditolak atau kuota penuh, **kabari saya** — M5 ditandai terblokir dan slide 16 disesuaikan. Bukti lokal sudah cukup untuk presentasi.

## Langkah 2 — Buat schema dan Volume (3 menit)

1. Menu kiri: **Catalog**.
2. Pilih catalog **workspace** → **Create schema** → nama: `kelompok2`.
3. Di dalam schema `kelompok2` → **Create** → **Volume** → nama: `data_praktikum`.

Hasil akhirnya harus ada path: `/Volumes/workspace/kelompok2/data_praktikum`

## Langkah 3 — Unggah 4 berkas data (5 menit)

Buka Volume `data_praktikum` → **Upload to this volume**, unggah dari folder
`D:\Documents\S2\Kuliah\261\Big Data\Praktikum Data Storage\data\`:

| Berkas | Ukuran | Peran |
|---|---|---|
| `usgs_indonesia.csv` | ±9,9 MB | gempa asli (batch historis, 55.194 event) |
| `laporan_edge.csv` | ±20 KB | laporan lapangan (sintetis berlabel) |
| `ref_wilayah_ntt.csv` | ±1,3 KB | 22 kabupaten/kota NTT |
| `raw\bmkg_gempaterkini.json` | kecil | payload mentah BMKG (asli) |

Nama berkas harus persis sama (huruf kecil, garis bawah).

## Langkah 4 — Impor notebook (2 menit)

1. Menu kiri: **Workspace** → folder Anda.
2. **Create** → **Notebook**, beri nama `Medallion_Gempa_NTT_Kelompok2`.
3. Buka berkas `cloud/NOTEBOOK_MEDALLION_DATABRICKS.py` dari folder praktikum dengan Notepad, **salin seluruh isinya**.
4. Di notebook Databricks: hapus sel kosong pertama, lalu tempel. Kalau Databricks menawarkan impor sebagai berkas, lebih baik: **Workspace → Import → File**, pilih `NOTEBOOK_MEDALLION_DATABRICKS.py` (format dikenali otomatis karena ada penanda `# Databricks notebook source`).
5. Pastikan notebook terhubung ke **Serverless** (di Free Edition compute sudah otomatis).

## Langkah 5 — Jalankan (5 menit)

Klik **Run all**. Notebook mencetak jumlah baris tiap lapisan dan alasan quarantine.

Kalau ada sel gagal, **jangan diperbaiki sendiri** — screenshot pesan errornya dan kirim ke saya.

## Langkah 6 — Screenshot yang dibutuhkan (5 menit)

Simpan ke folder `bukti\` dengan nama berikut:

| Nama berkas | Isi screenshot |
|---|---|
| `M5_ui_notebook.png` | notebook dengan sel Gold berhasil + tabel prioritas terlihat |
| `M5_ui_catalog.png` | Catalog menampilkan schema `kelompok2` dan tabel bronze/silver/gold |
| `M5_ui_history.png` | hasil `DESCRIBE HISTORY` (ada operasi WRITE dan MERGE) |
| `M5_ui_timetravel.png` | hasil query `VERSION AS OF 0` berdampingan dengan versi kini |
| `M5_ui_volume.png` | Volume `data_praktikum` berisi 4 berkas terunggah |

Cara cepat: `Windows + Shift + S`, lalu tempel ke Paint dan simpan dengan nama di atas.

## Langkah 7 — Kabari saya

Cukup tulis "screenshot sudah ada di bukti/". Saya lanjut menyusun slide 16 dan membandingkan angka cloud vs lokal untuk slide 15.

---

## Batasan Free Edition yang perlu disebut jujur di presentasi

Berdasarkan dokumentasi resmi Databricks (diakses 2026-11-11):

- **Serverless saja** — tidak bisa memilih ukuran/jumlah cluster sendiri.
- **1 SQL warehouse** ukuran 2X-Small, **maksimal 5 job berjalan bersamaan**.
- **Non-commercial use only** — untuk kuliah boleh, untuk operasional BPBD nyata harus lisensi berbayar.
- Ada kuota compute per akun; kalau habis, notebook tidak bisa dijalankan sampai kuota pulih.
- Penyimpanan di bawah kap dikelola Databricks — kita bisa melihat `DESCRIBE HISTORY`, tetapi **tidak bisa membuka file Parquet/`_delta_log` secara langsung**. Justru karena itu bukti fisik storage diambil dari replika lokal (E1).

Sumber: https://docs.databricks.com/aws/en/getting-started/free-edition-limitations