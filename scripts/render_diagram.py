"""Merender diagram presentasi berlabel Indonesia secara deterministik (bukan gambar AI)."""
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

OUT = Path(__file__).resolve().parent.parent / "diagram"
BIRU = "#00519E"
BIRU_TUA = "#154360"
BRONZE = "#CD7F32"
SILVER = "#8C9196"
GOLD = "#C9971A"
MERAH = "#C0392B"
ABU = "#5D6D7E"
TEKS = "#1B2631"
LATAR = "#FFFFFF"


def kanvas(judul=None, lebar=13.33, tinggi=7.2):
    """Judul sengaja opsional: judul halaman dipegang PPTX, bukan gambar."""
    fig, ax = plt.subplots(figsize=(lebar, tinggi), dpi=200)
    fig.patch.set_facecolor(LATAR)
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.set_aspect("auto")
    ax.axis("off")
    if judul:
        ax.text(50, 97, judul, ha="center", va="top", fontsize=17, fontweight="bold", color=TEKS)
    else:
        ax.set_ylim(0, 90)
    return fig, ax


def kotak(ax, x, y, w, h, teks, warna=BIRU, fs=10.5, tebal=False, isi_putih=False):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0,rounding_size=1.2",
                                linewidth=1.6, edgecolor=warna,
                                facecolor="white" if isi_putih else warna, zorder=2))
    if teks:
        ax.text(x + w / 2, y + h / 2, teks, ha="center", va="center", zorder=3,
                fontsize=fs, fontweight="bold" if tebal else "normal",
                color=TEKS if isi_putih else "white", linespacing=1.45)


def panah(ax, p1, p2, warna=ABU, label="", fs=9, lw=1.8, putus=False, rad=0.0, offset=(0, 3)):
    ax.add_patch(FancyArrowPatch(p1, p2, arrowstyle="-|>", mutation_scale=15,
                                 linewidth=lw, color=warna, zorder=1,
                                 linestyle=(0, (4, 3)) if putus else "solid",
                                 connectionstyle=f"arc3,rad={rad}"))
    if label:
        ax.text((p1[0] + p2[0]) / 2 + offset[0], (p1[1] + p2[1]) / 2 + offset[1], label,
                ha="center", va="center", fontsize=fs, color=warna, zorder=4,
                bbox=dict(boxstyle="round,pad=0.25", facecolor="white", edgecolor="none", alpha=0.95))


def simpan(fig, nama):
    OUT.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT / nama, bbox_inches="tight", facecolor=LATAR, pad_inches=0.18)
    plt.close(fig)
    print(f"  [OK] diagram/{nama}")


def d1_arsitektur():
    fig, ax = kanvas()
    kolom = [
        (2, BIRU, "CONTROL PLANE", "dikelola Databricks",
         ["Antarmuka web & notebook", "Penjadwal job & orkestrasi", "Unity Catalog: metadata & izin", "Riwayat versi tabel Delta"]),
        (35.5, BIRU_TUA, "COMPUTE PLANE", "menyala hanya saat ada kerja",
         ["Apache Spark: cluster / serverless", "SQL Warehouse untuk dasbor", "Auto Loader & Structured Streaming", "Dimatikan saat idle → hemat biaya"]),
        (69, "#7D6608", "STORAGE", "object storage murah milik pelanggan",
         ["AWS S3 / Azure ADLS / GCS", "Berkas Parquet = data sebenarnya", "_delta_log = buku transaksi", "Tetap ada walau compute mati"]),
    ]
    for x, warna, judul, sub, items in kolom:
        kotak(ax, x, 24, 29, 64, "", warna, isi_putih=True)
        ax.text(x + 14.5, 83, judul, ha="center", fontsize=12.5, fontweight="bold", color=warna)
        ax.text(x + 14.5, 78.5, sub, ha="center", fontsize=9, color=ABU, style="italic")
        for i, t in enumerate(items):
            kotak(ax, x + 2, 66 - i * 11, 25, 9, t, warna, fs=9.5)
    panah(ax, (31, 56), (35.5, 56), BIRU, "perintah", fs=8.5, offset=(0, 4))
    panah(ax, (64.5, 56), (69, 56), "#7D6608", "baca / tulis", fs=8.5, offset=(0, 4))
    ax.plot([16.5, 16.5], [24, 18], color=ABU, lw=1.5, linestyle=(0, (4, 3)), zorder=1)
    ax.plot([83.5, 83.5], [24, 18], color=ABU, lw=1.5, linestyle=(0, (4, 3)), zorder=1)
    panah(ax, (16.5, 18), (83.5, 18), ABU, "metadata & izin dikelola terpusat (Unity Catalog)",
          fs=9, putus=True, offset=(0, 3.2))
    ax.text(50, 7, "Data disimpan sekali di object storage terbuka; banyak mesin komputasi bisa membacanya "
            "tanpa memindahkan data.", ha="center", fontsize=10.5, color=TEKS, style="italic")
    simpan(fig, "01_arsitektur_platform.png")


def d2_delta_fisik():
    """Angka dan potongan JSON diambil dari bukti/M4_E1_bedah_delta_log.txt (E1 LULUS)."""
    fig, ax = kanvas()
    kotak(ax, 3, 48, 44, 38, "", SILVER, isi_putih=True)
    ax.text(25, 82, "DATA — 2 berkas Parquet", ha="center", fontsize=12.5, fontweight="bold", color=ABU)
    for i, t in enumerate(["part-00000-be447583-…-c000.snappy.parquet\n20.383 byte · 135 baris (versi 0)",
                           "part-00001-…-c000.snappy.parquet\n20.462 byte · 136 baris (versi 1)"]):
        kotak(ax, 6, 68 - i * 11, 38, 9.5, t, SILVER, fs=8.5)
    ax.text(25, 51, "total 40.845 byte · tidak diubah di tempat", ha="center", fontsize=8.5, color=ABU, style="italic")

    kotak(ax, 53, 48, 44, 38, "", BIRU, isi_putih=True)
    ax.text(75, 82, "_delta_log/ — 2 berkas JSON", ha="center", fontsize=12.5, fontweight="bold", color=BIRU)
    for i, t in enumerate(["00000000000000000000.json\ncommit versi 0 — WRITE",
                           "00000000000000000001.json\ncommit versi 1 — MERGE"]):
        kotak(ax, 56, 68 - i * 11, 38, 9.5, t, BIRU, fs=8.5)
    ax.text(75, 51, "total 6.483 byte · sumber kebenaran versi", ha="center", fontsize=8.5, color=ABU, style="italic")
    panah(ax, (47, 67), (53, 67), ABU, "dirujuk", fs=8.5, offset=(0, 4))

    kotak(ax, 6, 10, 88, 32, "", TEKS, isi_putih=True)
    ax.text(50, 38, "Isi commit versi 0 — dipotong dari bukti/M4_E1_bedah_delta_log.txt", ha="center",
            fontsize=11, fontweight="bold", color=TEKS)
    isi = ('{"commitInfo":{"operation":"WRITE","numOutputRows":"135","numOutputBytes":"20383",\n'
           '               "engineInfo":"Apache-Spark/4.2.0 Delta-Lake/4.4.0"}}\n'
           '{"metaData":{"format":{"provider":"parquet"},"schemaString":"…16 kolom, jiwa_terdampak = long…"}}\n'
           '{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}\n'
           '{"add":{"path":"part-00000-be447583-…-c000.snappy.parquet","size":20383,"stats":"{numRecords:135,…}"}}')
    ax.text(50, 24, isi, ha="center", va="center", fontsize=8, family="monospace", color=TEKS, linespacing=1.75)
    ax.text(50, 13, "Commit versi 1 (MERGE) menambah file baru dan menandai file lama dengan aksi remove,\n"
            "sehingga file lama tidak ditimpa dan versi 0 tetap bisa dibaca.",
            ha="center", va="center", fontsize=9, color=TEKS, style="italic", linespacing=1.6)
    ax.text(50, 4, "Skema tersimpan di log, bukan di file data — karena itu Delta bisa menolak tulisan bertipe salah (E3).",
            ha="center", fontsize=9.5, color=ABU, style="italic")
    simpan(fig, "02_delta_fisik.png")


def d3_medallion():
    fig, ax = kanvas(tinggi=7.6)
    sumber = [("BMKG API\nperingatan gempa", "streaming", 78),
              ("Gateway IoT\nobservasi sensor", "streaming", 62),
              ("BPS NTT & inaRISK\nkependudukan & risiko", "batch", 46),
              ("Edge Node lapangan\nlaporan logistik", "asinkron", 30)]
    for t, jenis, cy in sumber:
        kotak(ax, 1.5, cy - 6, 19, 12, t, BIRU, fs=9)
        panah(ax, (20.5, cy), (24, 58), ABU, "", rad=0.04)
        ax.text(22.3, cy + 4.6, jenis, fontsize=7.5, color=ABU, ha="center", style="italic")

    kotak(ax, 24, 44, 12, 28, "INGESTION\n\nAuto Loader /\nStructured\nStreaming", BIRU, fs=9)
    panah(ax, (36, 60), (40, 60), ABU, "payload\nmentah", fs=8, offset=(0, 6))
    kotak(ax, 40, 44, 15, 32, "BRONZE\n\nbukti sumber\nimmutable\n\n+ checksum\n+ timestamp\n+ batch id", BRONZE, fs=9.5)
    panah(ax, (55, 60), (59, 60), ABU, "validasi skema\nresolusi identifier", fs=8, offset=(0, 7))
    kotak(ax, 59, 44, 15, 32, "SILVER\n\nrecord\ntervalidasi\n\n+ dedup\n+ MERGE\nrevisi terlambat", SILVER, fs=9.5)
    panah(ax, (74, 60), (78, 60), ABU, "agregasi\nspatial join", fs=8, offset=(0, 6))
    kotak(ax, 78, 44, 17, 32, "GOLD\n\ntampilan\noperasional\n\nsiap dipakai\npengambil\nkeputusan", GOLD, fs=9.5)

    kotak(ax, 59, 26, 15, 11, "QUARANTINE\nkonflik / invalid\n→ review petugas", MERAH, fs=8.5)
    panah(ax, (64, 44), (64, 37), MERAH, "", putus=True)
    panah(ax, (74, 31), (76.5, 45), MERAH, "verifikasi manual", fs=7.5, putus=True, rad=-0.25, offset=(7.5, -1))

    layanan = ["Tabel prioritas area\n& kebutuhan", "Peta status akses\nfasilitas", "Otorisasi bantuan\n& peringatan"]
    for i, t in enumerate(layanan):
        kotak(ax, 40 + i * 19, 10, 17, 11, t, BIRU_TUA, fs=9)
    panah(ax, (86.5, 44), (86.5, 21), GOLD, "", lw=2)
    ax.text(50, 4, "Setiap lapisan adalah tabel Delta tersendiri — bisa diaudit dan dibaca versi sebelumnya.",
            ha="center", fontsize=10.5, color=TEKS, style="italic")
    simpan(fig, "03_medallion.png")


def d4_revisi():
    """Nilai versi 0 vs kini diambil dari bukti/M4_E2_time_travel.txt (RPT-0008, E2 LULUS)."""
    fig, ax = kanvas()
    y = 74
    panah(ax, (6, y), (96, y), ABU, "", lw=2.2)
    titik = [(14, "t1", "RPT-0008 revisi 1 masuk\njalan: lancar · jiwa: 1.279"),
             (40, "t2", "Gold dihitung untuk operasi pagi\npetugas melihat kondisi ini"),
             (66, "t3", "Revisi 2 tiba 11:46 WITA\nkejadian 02:58, baru terkirim kini"),
             (90, "t4", "MERGE diterapkan\nhanya jika revisi baru > lama")]
    for x, lab, t in titik:
        ax.plot([x], [y], marker="o", markersize=9, color=BIRU, zorder=5)
        ax.text(x, y + 3, lab, ha="center", fontsize=11.5, fontweight="bold", color=BIRU)
        ax.text(x, y - 3.5, t, ha="center", va="top", fontsize=8.5, color=TEKS, linespacing=1.6)

    kotak(ax, 8, 24, 38, 28, "", SILVER, isi_putih=True)
    ax.text(27, 47, "VERSION AS OF 0  (kondisi t2)", ha="center", fontsize=11.5, fontweight="bold", color=ABU)
    ax.text(27, 35, "report_id = RPT-0008\nrevision = 1\nstatus_jalan = lancar\njiwa_terdampak = 1279",
            ha="center", va="center", fontsize=9.5, family="monospace", color=TEKS, linespacing=1.9)
    kotak(ax, 54, 24, 38, 28, "", GOLD, isi_putih=True)
    ax.text(73, 47, "VERSI 1 — SETELAH MERGE", ha="center", fontsize=11.5, fontweight="bold", color="#7D6608")
    ax.text(73, 35, "report_id = RPT-0008\nrevision = 2\nstatus_jalan = putus\njiwa_terdampak = 2046",
            ha="center", va="center", fontsize=9.5, family="monospace", color=TEKS, linespacing=1.9)
    panah(ax, (46, 38), (54, 38), GOLD, "MERGE", fs=9.5, offset=(0, 4))
    ax.text(50, 17, "event_time tetap 2026-11-11 02:58 WITA; hanya received_at yang maju ke 11:46 WITA.",
            ha="center", fontsize=10.5, fontweight="bold", color=TEKS)
    ax.text(50, 10, "Baris lama tidak ditimpa: ia menjadi versi 0 yang tetap bisa dibaca lewat VERSION AS OF 0.",
            ha="center", fontsize=10, color=ABU, style="italic")
    ax.text(50, 4, "Jawaban operasional: kondisi data sebelum laporan susulan masuk masih bisa direkonstruksi untuk audit.",
            ha="center", fontsize=10, color=ABU, style="italic")
    simpan(fig, "04_alur_revisi.png")


def d5_topologi():
    fig, ax = kanvas()
    panel = [(3, BIRU, "REPLIKA LOKAL (laptop)",
              ["Docker: Spark 4.2.0 + Delta Lake 4.4.0", "Data: /workspace/data (bind mount)",
               "Tabel Delta: volume /delta", "Parquet & _delta_log bisa dibuka langsung",
               "Bukti: E1, E4, E5, E6"]),
             (55, "#7D6608", "DATABRICKS FREE EDITION",
              ["Serverless notebook + SQL warehouse", "Data: Unity Catalog Volume",
               "Tabel Delta: penyimpanan terkelola", "File fisik tidak dibuka pengguna",
               "Bukti: UI asli, DESCRIBE HISTORY, time travel"])]
    for x, warna, judul, items in panel:
        kotak(ax, x, 34, 42, 54, "", warna, isi_putih=True)
        ax.text(x + 21, 83, judul, ha="center", fontsize=12.5, fontweight="bold", color=warna)
        for i, t in enumerate(items):
            kotak(ax, x + 2.5, 71 - i * 9, 37, 7.5, t, warna, fs=9)
    kotak(ax, 20, 14, 60, 12, "Format tabel identik: Delta Lake (Parquet + _delta_log)\nLogika pipeline per lapisan juga identik",
          TEKS, fs=10.5, tebal=True)
    panah(ax, (24, 34), (34, 26), ABU, "", putus=True)
    panah(ax, (76, 34), (66, 26), ABU, "", putus=True)
    ax.text(50, 6, "Lokal untuk membedah mekanisme penyimpanan; cloud untuk membuktikan platform terkelolanya nyata.",
            ha="center", fontsize=10.5, color=TEKS, style="italic")
    simpan(fig, "05_topologi_praktikum.png")


def main():
    print("Merender diagram presentasi:")
    d1_arsitektur()
    d2_delta_fisik()
    d3_medallion()
    d4_revisi()
    d5_topologi()
    print(f"Selesai. Semua diagram di {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())