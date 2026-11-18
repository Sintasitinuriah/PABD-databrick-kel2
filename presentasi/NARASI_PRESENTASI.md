# Narasi Presentasi — Databricks Lakehouse untuk Mitigasi Gempa NTT

Kelompok 2 · Big Data · Program S2 · Presentasi 12 November 2026
Durasi target **15 menit** (900 detik) untuk 18 slide. Angka di narasi ini sama persis dengan
angka pada slide dan berasal dari folder `bukti/`. Bila waktu mepet, slide 6 dan 14 boleh
dipersingkat masing-masing 10 detik tanpa kehilangan inti.

Pembagian pembicara:

| Pembicara | Slide | Alokasi |
|---|---|---|
| Alif Ilman Nafian | 1–4 | 3:00 |
| Beatrix Datu Sarira | 5–8 | 3:40 |
| Kevin Pratama | 9–12 | 3:30 |
| Sinta Siti Nuriah | 13–15 | 2:40 |
| Raoul Habonaran Purba | 16–18 | 2:10 |

---

## Slide 1 — Pembuka (20 dtk, Alif)

"Selamat pagi. Kami Kelompok 2. Pertanyaan yang kami jawab hari ini sederhana: bagaimana
membuat data gempa dari banyak sumber bisa dipercaya untuk keputusan evakuasi. Jawaban kami
satu platform — Databricks Lakehouse — dan satu format tabel — Delta Lake. Semua angka yang
kami tampilkan nanti berasal dari praktikum yang kami jalankan sendiri, bukan ilustrasi."

Transisi: "Mulai dari masalahnya dulu."

## Slide 2 — Masalah NTT (60 dtk, Alif)

"Di kotak NTT yang kami unduh dari katalog USGS ada 3.929 gempa sejak tahun 2000. Yang terbaru
besar: M7.8 pada 14 Agustus 2026, 66 km barat laut Ende, kedalaman 10 km — dangkal, jadi
guncangannya kuat di permukaan. Ada 27 gempa M≥6 dan 410 M≥5 di kotak ini.

Tapi masalah operasionalnya bukan volume. Pertama, data tersebar: BMKG untuk peringatan, BPS
untuk penduduk, inaRISK untuk risiko, dan laporan lapangan dari petugas. Kedua, laporan
lapangan datang terlambat dan direvisi — angka jiwa terdampak bisa berubah setelah keputusan
diambil. Ketiga, keputusan harus diambil dalam hitungan jam. Tiga masalah ini yang menuntut
storage yang transaksional dan bisa diaudit."

Transisi: "Platform apa yang menjawab itu?"

## Slide 3 — Apa itu Databricks (50 dtk, Alif)

"Databricks adalah platform analitik terpadu di atas Apache Spark, didirikan oleh pembuat
Spark sendiri. Ia memelopori istilah lakehouse. Poin pentingnya: Databricks bukan storage
baru. Ia lapisan yang membuat storage murah diperlakukan seperti database.

Bandingkan dengan Hadoop/HDFS tradisional: di sana kita merawat NameNode, DataNode, dan YARN
sendiri, dan mengubah satu baris berarti menulis ulang seluruh partisi. Di Databricks compute
menyala saat ada kerja, dan tabelnya transaksional. Versi yang kami pakai di praktikum:
Spark 4.2.0 dan Delta Lake 4.4.0 — versi open-source yang sama dengan engine di baliknya."

Transisi: "Kenapa harus lakehouse, bukan lake atau warehouse biasa?"

## Slide 4 — Kenapa Lakehouse (50 dtk, Alif)

"Data lake murah dan formatnya terbuka, tapi tanpa transaksi: dua penulis bersamaan bisa
merusak data. Data warehouse punya ACID dan skema ketat, tapi mahal dan menyalin data ke
sistem terpisah.

Kasus gempa NTT butuh dua hal sekaligus: arsip bukti mentah yang murah, dan tabel keputusan
yang transaksional. Lakehouse mengambil keduanya: file tetap di object storage murah, tetapi
Delta Lake menambahkan lapisan transaksi di atasnya. Jadi tidak ada salinan data antar sistem
— SQL, Python, dan ML membaca tabel yang sama."

Transisi: "Beatrix akan membedah cara kerjanya."

## Slide 5 — Arsitektur tiga bidang (50 dtk, Beatrix)

"Ada tiga bidang yang terpisah. Control plane dikelola Databricks: notebook, job, keamanan.
Compute plane adalah cluster atau serverless yang menyala saat ada pekerjaan. Storage plane
adalah object storage milik pelanggan sendiri — S3 atau ADLS — tempat file Delta berada.

Konsekuensi praktisnya penting untuk anggaran BPBD: mematikan compute tidak menghapus data.
Nanti di slide 15 kami buktikan dengan uji hash: container Spark kami matikan total, lalu
nyalakan lagi, dan datanya identik byte-per-byte."

Transisi: "Sekarang bagian paling teknis: tabel Delta itu sebenarnya apa?"

## Slide 6 — Parquet + buku transaksi (60 dtk, Beatrix)

"Secara fisik, tabel Delta hanyalah folder berisi berkas Parquet plus folder `_delta_log`
berisi commit JSON. Ini foto nyata tabel laporan aktif kami: 2 berkas Parquet total 40.845
byte, dirujuk oleh 2 commit JSON total 6.483 byte.

Commit versi 0 berisi empat aksi: commitInfo, metaData, protocol, dan add. Perhatikan dua hal.
Skema tersimpan di log, bukan di file data — karena itu Delta bisa menolak tulisan bertipe
salah. Dan aksi `add` membawa statistik min/max per kolom untuk data skipping.

Kunci desainnya: commit versi 1 tidak menimpa file lama. Ia menambah file baru dan menandai
file lama dengan aksi `remove`. File lama tetap ada di disk sampai dibersihkan VACUUM."

Transisi: "Dari desain itu lahir sifat ACID-nya."

## Slide 7 — ACID & konkurensi (60 dtk, Beatrix)

"Satu transaksi Delta berjalan empat langkah. Penulis membaca snapshot versi N. Ia menulis
file Parquet baru, bukan menimpa. Saat commit, ia memeriksa konflik: apakah ada penulis lain
yang mengubah file yang sama sejak versi N? Jika ya, commit ditolak dan penulis mengulang
dengan snapshot terbaru — ini optimistic concurrency, tanpa lock yang menahan pembaca.
Jika tidak, commit JSON ditulis atomik dan jadilah versi N+1.

Pembaca selalu melihat satu versi yang konsisten, tidak pernah setengah jadi. Untuk kasus
kami ini berarti: puluhan petugas mengirim laporan bersamaan, dan laporan revisi yang datang
belakangan tidak merusak angka yang sudah dipakai operasi pagi."

Transisi: "Bagaimana lapisan-lapisannya disusun?"

## Slide 8 — Medallion (50 dtk, Beatrix)

"Kami memakai pola medallion. Bronze menyimpan bukti sumber apa adanya, append-only, lengkap
dengan metadata asal: kapan masuk, dari sumber mana, batch mana, dan checksum. Silver adalah
data yang lolos validasi: koordinat masuk akal, identitas lengkap, duplikat dibuang, revisi
direkonsiliasi. Gold adalah tampilan operasional untuk keputusan.

Dan yang tidak lolos tidak dibuang diam-diam: ia masuk Quarantine beserta alasannya, lalu
direview petugas. Ini penting untuk audit bantuan bencana — setiap penolakan bisa dijelaskan."

Transisi: "Kevin menunjukkan angka nyatanya."

## Slide 9 — Ingestion (50 dtk, Kevin)

"Empat sumber, dua moda masuk. BMKG TEWS dan gateway IoT bersifat streaming atau micro-batch;
di Databricks pola ini dilayani Auto Loader atau Structured Streaming. BPS NTT dan inaRISK
bersifat batch. Laporan Edge Node bersifat asinkron: petugas offline di lapangan, lalu
sinkronisasi ulang saat ada sinyal — justru pola ini yang menghasilkan revisi terlambat.

Kolom kanan slide menunjukkan status jujur data kami: gempa BMKG dan USGS asli, referensi
wilayah nama faktual dengan populasi sintetis berlabel, dan laporan edge sintetis berlabel
dengan seed tetap supaya bisa diulang."

Transisi: "Masuk ke Bronze."

## Slide 10 — Bronze (40 dtk, Kevin)

"Bronze kami berisi lima tabel Delta. Yang terbesar: 55.194 baris gempa USGS hasil unduhan
sendiri, 31 event BMKG hari ini, 22 wilayah referensi, dan 166 laporan edge sintetis. Tiga
berkas payload mentah disimpan utuh tanpa diubah.

Setiap baris membawa empat kolom metadata: `_ingested_at`, `_source`, `_batch_id`, dan
`_checksum`. Checksum inilah yang nanti dipakai Silver untuk mendeteksi duplikat persis dan
konflik isi. Bronze tidak pernah di-update — ia adalah bukti sumber."

Transisi: "Silver memilah yang layak dipercaya."

## Slide 11 — Silver (60 dtk, Kevin)

"Gabungan gempa BMKG dan USGS menghasilkan 55.219 baris tervalidasi di Silver. Untuk laporan
edge: dari 166 baris, 23 dikarantina dan 143 lolos; setelah MERGE revisi terlambat, tabel aktif
berisi 136 baris dengan 8 baris kini berevisi lebih dari 1.

Lihat batang alasan quarantine: 10 konflik revisi dengan isi berbeda, 6 koordinat di luar NTT,
4 identitas pelapor kosong, 4 duplikat event id, 3 duplikat persis, 2 koordinat di luar
cakupan. Konflik revisi sengaja kami karantina semua versinya supaya petugas yang memutuskan,
bukan pipeline.

MERGE-nya bersyarat: baris lama hanya diperbarui bila revisi baru lebih besar. Laporan lama
tidak hilang — ia menjadi versi 0 yang tetap bisa dibaca."

Transisi: "Gold mengubahnya menjadi keputusan."

## Slide 12 — Gold (60 dtk, Kevin)

"Gold bukan satu tabel, tapi tiga tampilan operasional: prioritas area 22 baris, status akses
fasilitas 77 baris, dan rekap bantuan 72 baris. Tabel prioritas menilai tiap kabupaten/kota
dalam jendela 30 hari yang berisi 23 gempa NTT.

Lima teratas: Manggarai 162,58; Ende 162,42; Lembata 159,32; Kota Kupang 152,29; Manggarai
Barat 152,21. Skornya gabungan magnitudo maksimum, indeks risiko, populasi, jalan putus dan
terbatas, serta jiwa terdampak — bobotnya kami pilih sendiri dan kami sebut terbuka di slide,
bukan standar BNPB. Populasi dan jiwa terdampak sintetis berlabel, jadi angka ini demonstrasi
metode, bukan klaim kondisi nyata."

Transisi: "Sinta menunjukkan bagian auditnya."

## Slide 13 — Time travel (60 dtk, Sinta)

"Ini jawaban atas pertanyaan operasional: bagaimana kondisi data sebelum laporan susulan
masuk? Ambil RPT-0008. Pada versi 0: revisi 1, jalan lancar, 1.279 jiwa. Itulah yang dilihat
petugas saat Gold dihitung untuk operasi pagi. Lalu revisi 2 tiba pukul 11:46 WITA untuk
kejadian pukul 02:58 — MERGE menerapkannya karena revisi baru lebih besar. Kini: jalan putus,
2.046 jiwa.

Perhatikan `event_time` tidak berubah; yang maju adalah `received_at`. Dan baris lama tidak
ditimpa: `SELECT ... VERSION AS OF 0` masih mengembalikan kondisi pagi tadi. DESCRIBE HISTORY
mencatat versi 0 WRITE dan versi 1 MERGE. Untuk audit bantuan, ini berarti setiap keputusan
bisa direkonstruksi dengan data yang benar-benar terlihat saat keputusan itu diambil."

Transisi: "Tapi ada harga yang harus diurus setiap hari."

## Slide 14 — Operasional harian (50 dtk, Sinta)

"Empat pekerjaan. Pertama, menjalankan dan menjadwalkan pipeline — di Databricks lewat
Workflows; di praktikum kami lewat satu skrip yang menjalankan Bronze sampai gerbang
verifikasi. Kedua, merawat ukuran file: 12 tulis kecil menghasilkan 12 file Parquet; OPTIMIZE
memadatkannya menjadi 1 file tanpa kehilangan baris.

Ketiga, mengatur retensi. VACUUM RETAIN 0 HOURS menghapus 8 dari 9 file fisik di tabel uji
kami, dan setelah itu versi 0 gagal dibaca dengan error FILE_NOT_EXIST. Jadi klaim 'bukti
historis abadi' hanya berlaku selama berkas belum di-VACUUM — retensi audit harus disetel
lebih panjang dari kebutuhan penelusuran. Keempat, mengawasi biaya compute, karena compute
dan storage dibayar terpisah."

Transisi: "Ringkasan angka yang bisa diperiksa."

## Slide 15 — Hasil praktikum (50 dtk, Sinta)

"Semua angka di slide ini ada berkasnya di folder bukti. Gerbang M3 LULUS: sembilan tabel,
koordinat dalam bbox NTT, tanpa duplikat event id, delapan revisi terlambat direkonsiliasi.
Eksperimen E1 sampai E6 LULUS semua.

Dua angka yang paling kami banggakan: 12 menjadi 1 file setelah OPTIMIZE, dan 6 dari 6 berkas
tabel prioritas hash-nya identik setelah container Spark dimatikan lalu dinyalakan. Yang
kedua adalah bukti langsung pemisahan storage dan compute: compute mati total, data tetap
utuh, dan query pertama setelah hidup kembali langsung mengembalikan prioritas teratas
Manggarai 162,58."

Transisi: "Raoul menutup dengan platform aslinya dan penilaian jujur."

## Slide 16 — Free Edition (40 dtk, Raoul)

"Praktikum kami berjalan di replika lokal: Spark dan Delta open-source di Docker, engine dan
format tabel yang sama dengan Databricks. Status jujur per malam 11 November: akun Databricks
Free Edition belum kami buat, jadi screenshot UI asli belum ada; slide ini memakai topologi
replika kami.

Batas Free Edition menurut dokumentasi resmi: serverless saja, satu SQL warehouse 2X-Small,
maksimal lima job bersamaan, non-commercial only, dan berkas fisik dikelola platform sehingga
folder `_delta_log` tidak bisa dibuka langsung oleh pengguna. Notebook untuk versi cloud sudah
kami siapkan bila akun siap sebelum presentasi."

Transisi: "Penilaian kami terhadap platform ini."

## Slide 17 — Keunggulan & keterbatasan (50 dtk, Raoul)

"Keunggulan: tanpa perawatan cluster; satu format untuk SQL, Python, dan ML; audit bawaan
karena time travel adalah akibat langsung dari desain log, bukan fitur tambahan; dan
kolaborasi notebook real-time.

Keterbatasan, termasuk yang kami alami sendiri: biaya lisensi untuk pemakaian nyata di luar
Free Edition; ketergantungan koneksi internet — justru masalah di NTT saat bencana; vendor
lock-in pada control plane; dan retensi VACUUM yang membatasi klaim bukti abadi, seperti kami
buktikan di E5. Kami menyebut ini terbuka karena BPBD/BNPB harus menimbangnya sebelum adopsi."

Transisi: "Penutup."

## Slide 18 — Kesimpulan (40 dtk, Raoul)

"Tiga poin. Lakehouse membuat data mentah bisa dipercaya tanpa memindahkan storage. Delta
Lake memberi ACID dan time travel di atas object storage murah lewat mekanisme yang sederhana:
buku transaksi JSON yang mencatat file mana yang berlaku pada setiap versi. Dan pipeline
medallion mengubah laporan lapangan yang berantakan menjadi prioritas bantuan yang bisa
diaudit: 166 laporan masuk, 23 dikarantina dengan alasan, 8 revisi terlambat diterapkan tanpa
menghapus bukti lama.

Terima kasih. Semua kode, data, dan bukti praktikum tersedia di folder kelompok dan siap
diperiksa. Kami buka pertanyaan."

---

## Antisipasi pertanyaan penguji

- **"Kenapa tidak pakai database biasa?"** Karena volume arsip mentah (55 ribu event dan
  growing) murah di object storage, dan kita butuh schema-on-read untuk payload mentah;
  warehouse biasa mahal untuk arsip dan tidak fleksibel untuk payload mentah yang beragam.
  Delta memberi transaksi di atas storage murah itu.
- **"Apakah angka prioritas bisa dipakai nyata?"** Belum. Populasi dan jiwa terdampak
  sintetis berlabel; bobot skor pilihan kelompok. Yang nyata adalah katalog gempa dan
  mekanisme pipeline-nya.
- **"Apa bedanya replika lokal dengan Databricks asli?"** Engine dan format tabel sama
  (Spark 4.2.0, Delta 4.4.0). Yang berbeda: control plane, Unity Catalog, serverless, dan
  fakta bahwa di Free Edition berkas fisik tidak bisa dibuka pengguna.
- **"Kenapa quarantine bukan dibuang?"** Audit bantuan bencana menuntut setiap penolakan
  bisa dijelaskan dan direview manusia; konflik revisi adalah keputusan petugas, bukan mesin.
- **"VACUUM menghapus bukti, bukankah itu berbahaya?"** Ya, bila retensi salah setel. Karena
  itu E5 kami jadikan temuan, bukan kegagalan: retensi audit harus lebih panjang dari
  kebutuhan penelusuran, atau versi diarsipkan terpisah.