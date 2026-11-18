"""Menyiapkan dataset praktikum: gempa asli (BMKG/USGS) + wilayah & laporan edge sintetis."""
import argparse
import csv
import hashlib
import io
import json
import random
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
RAW = DATA / "raw"

BMKG_ENDPOINTS = {
    "autogempa": "https://data.bmkg.go.id/DataMKG/TEWS/autogempa.json",
    "gempaterkini": "https://data.bmkg.go.id/DataMKG/TEWS/gempaterkini.json",
    "gempadirasakan": "https://data.bmkg.go.id/DataMKG/TEWS/gempadirasakan.json",
}
USGS_BASE = "https://earthquake.usgs.gov/fdsnws/event/1/query"
USGS_BBOX = "minlatitude=-12&maxlatitude=6&minlongitude=94&maxlongitude=141"

# Kabupaten/kota NTT faktual; koordinat adalah perkiraan pusat administrasi,
# populasi dan indeks risiko SINTETIS karena tabel BPS/inaRISK tidak tersedia.
WILAYAH = [
    ("NTT-01", "Kota Kupang", -10.1772, 123.6070),
    ("NTT-02", "Kabupaten Kupang", -10.1500, 123.6500),
    ("NTT-03", "Timor Tengah Selatan", -9.8500, 124.4000),
    ("NTT-04", "Timor Tengah Utara", -9.4500, 124.5500),
    ("NTT-05", "Belu", -9.1500, 124.9500),
    ("NTT-06", "Malaka", -9.5500, 124.9000),
    ("NTT-07", "Rote Ndao", -10.7300, 123.1200),
    ("NTT-08", "Sabu Raijua", -10.5000, 121.8500),
    ("NTT-09", "Alor", -8.2200, 124.5500),
    ("NTT-10", "Lembata", -8.4000, 123.5000),
    ("NTT-11", "Flores Timur", -8.3500, 122.9000),
    ("NTT-12", "Sikka", -8.6200, 122.2200),
    ("NTT-13", "Ende", -8.8500, 121.6500),
    ("NTT-14", "Nagekeo", -8.7000, 121.2000),
    ("NTT-15", "Ngada", -8.8000, 121.0000),
    ("NTT-16", "Manggarai", -8.6100, 120.4600),
    ("NTT-17", "Manggarai Timur", -8.6500, 120.6000),
    ("NTT-18", "Manggarai Barat", -8.5000, 119.8900),
    ("NTT-19", "Sumba Timur", -9.6500, 120.2600),
    ("NTT-20", "Sumba Barat", -9.5500, 119.4500),
    ("NTT-21", "Sumba Tengah", -9.6000, 119.7000),
    ("NTT-22", "Sumba Barat Daya", -9.4000, 119.2000),
]


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def unduh(url, timeout=60, coba=3):
    terakhir = None
    for i in range(coba):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "praktikum-bigdata-kelompok2/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read()
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            terakhir = e
            time.sleep(2 * (i + 1))
    raise terakhir


def catat(manifest, path, sumber, status, url="", baris=None, catatan=""):
    manifest["berkas"].append({
        "path": str(path.relative_to(ROOT)).replace("\\", "/"),
        "sumber": sumber,
        "status": status,
        "url": url,
        "diambil_pada": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "baris": baris,
        "bytes": path.stat().st_size,
        "sha256": sha256(path),
        "catatan": catatan,
    })


def ambil_bmkg(manifest, gagal):
    hasil = {}
    for nama, url in BMKG_ENDPOINTS.items():
        path = RAW / f"bmkg_{nama}.json"
        try:
            raw = unduh(url)
            payload = json.loads(raw)
            path.write_bytes(raw)
            gempa = payload["Infogempa"]["gempa"]
            n = len(gempa) if isinstance(gempa, list) else 1
            hasil[nama] = n
            catat(manifest, path, "BMKG TEWS", "asli", url, n,
                  "payload mentah disimpan apa adanya untuk lapisan Bronze")
            print(f"  [OK] {nama}: {n} event")
        except Exception as e:
            gagal.append(f"BMKG {nama}: {e}")
            print(f"  [GAGAL] {nama}: {e}")
    return hasil


def ambil_usgs(manifest, gagal, mulai=2000, sampai=2026):
    path_gab = DATA / "usgs_indonesia.csv"
    header = None
    total = 0
    potongan = []
    for tahun in range(mulai, sampai + 1):
        url = (f"{USGS_BASE}?format=csv&starttime={tahun}-01-01"
               f"&endtime={tahun}-12-31&minmagnitude=2.5&{USGS_BBOX}&orderby=time-asc")
        try:
            raw = unduh(url, timeout=90)
            teks = raw.decode("utf-8")
            if not teks.strip().startswith("time,"):
                raise ValueError(f"respons bukan CSV: {teks[:80]!r}")
            baris = list(csv.reader(io.StringIO(teks)))
            if header is None:
                header = baris[0]
            isi = baris[1:]
            pth = RAW / f"usgs_{tahun}.csv"
            pth.write_bytes(raw)
            potongan.append((pth, url, len(isi)))
            total += len(isi)
            print(f"  [OK] {tahun}: {len(isi)} event")
        except Exception as e:
            gagal.append(f"USGS {tahun}: {e}")
            print(f"  [GAGAL] {tahun}: {e}")
    if header is None:
        return 0
    with open(path_gab, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        for pth, _, _ in potongan:
            with open(pth, newline="", encoding="utf-8") as g:
                r = csv.reader(g)
                next(r, None)
                w.writerows(r)
    for pth, url, n in potongan:
        catat(manifest, pth, "USGS FDSN event", "asli", url, n, "katalog per tahun, wilayah Indonesia M2.5+")
    catat(manifest, path_gab, "USGS FDSN event", "turunan", USGS_BASE, total,
          f"gabungan {len(potongan)} potongan tahunan, tanpa perubahan isi baris")
    return total


def buat_wilayah(manifest, rng):
    path = DATA / "ref_wilayah_ntt.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["wilayah_id", "nama_wilayah", "lat_pusat", "lon_pusat",
                    "populasi", "indeks_risiko", "populasi_sintetis", "koordinat_perkiraan"])
        for wid, nama, lat, lon in WILAYAH:
            w.writerow([wid, nama, lat, lon, rng.randint(45_000, 430_000),
                        round(rng.uniform(0.2, 0.95), 3), True, True])
    catat(manifest, path, "sintetis ber-seed", "sintetis", "", len(WILAYAH),
          "nama kabupaten/kota NTT faktual; koordinat perkiraan; populasi & indeks risiko SINTETIS "
          "(tabel BPS NTT dan inaRISK BNPB tidak tersedia offline)")
    print(f"  [OK] wilayah NTT: {len(WILAYAH)} baris (populasi sintetis berlabel)")
    return len(WILAYAH)


def buat_edge(manifest, rng, jumlah=140):
    path = DATA / "laporan_edge.csv"
    kolom = ["report_id", "wilayah_id", "reporter_id", "event_time", "received_at", "revision",
             "status_jalan", "jenis_fasilitas", "kebutuhan_utama", "jiwa_terdampak", "lat", "lon"]
    now = datetime(2026, 11, 11, 9, 0, tzinfo=timezone.utc)
    rows = []
    for i in range(jumlah):
        wid, nama, lat, lon = WILAYAH[i % len(WILAYAH)]
        t_event = now - timedelta(minutes=rng.randint(5, 600))
        rows.append({
            "report_id": f"RPT-{i:04d}", "wilayah_id": wid, "reporter_id": f"PTG-{rng.randint(1, 40):03d}",
            "event_time": t_event.isoformat(), "received_at": (t_event + timedelta(minutes=rng.randint(1, 90))).isoformat(),
            "revision": 1, "status_jalan": rng.choice(["lancar", "terbatas", "putus"]),
            "jenis_fasilitas": rng.choice(["puskesmas", "sekolah", "jembatan", "gudang"]),
            "kebutuhan_utama": rng.choice(["air bersih", "pangan", "medis", "tenda"]),
            "jiwa_terdampak": rng.randint(20, 4000),
            "lat": round(lat + rng.uniform(-0.2, 0.2), 4), "lon": round(lon + rng.uniform(-0.2, 0.2), 4),
        })

    # Revisi terlambat: event_time lebih tua, received_at lebih baru, revision naik.
    revisi = []
    for idx in rng.sample(range(jumlah), 8):
        asal = rows[idx]
        r = dict(asal)
        r["revision"] = 2
        r["received_at"] = (now + timedelta(minutes=rng.randint(30, 180))).isoformat()
        r["status_jalan"] = "putus" if asal["status_jalan"] != "putus" else "terbatas"
        r["jiwa_terdampak"] = int(asal["jiwa_terdampak"] * 1.6)
        revisi.append(r)

    # Konflik: report_id dan revision sama, isi berbeda -> harus masuk quarantine.
    konflik = []
    for idx in rng.sample(range(jumlah), 5):
        r = dict(rows[idx])
        r["jiwa_terdampak"] = int(r["jiwa_terdampak"]) + 999
        r["status_jalan"] = "lancar"
        konflik.append(r)

    # Koordinat di luar wilayah NTT -> quarantine.
    invalid = []
    for j in range(6):
        r = dict(rows[j])
        r["report_id"] = f"RPT-INV-{j:02d}"
        r["lat"], r["lon"] = (0.0, 0.0) if j % 2 == 0 else (-6.2, 106.8)
        invalid.append(r)

    # Identitas pelapor hilang -> quarantine.
    tanpa_id = []
    for j in range(4):
        r = dict(rows[100 + j])
        r["report_id"] = f"RPT-NOID-{j:02d}"
        r["reporter_id"] = ""
        tanpa_id.append(r)

    duplikat = [dict(rows[k]) for k in range(3)]

    semua = rows + revisi + konflik + invalid + tanpa_id + duplikat
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=kolom)
        w.writeheader()
        w.writerows(semua)
    catat(manifest, path, "sintetis ber-seed", "sintetis", "", len(semua),
          f"{jumlah} laporan dasar + 8 revisi terlambat + 5 konflik + 6 koordinat invalid "
          "+ 4 tanpa identitas + 3 duplikat; dibuat untuk menguji validasi & rekonsiliasi")

    salah = DATA / "laporan_edge_skema_salah.csv"
    with open(salah, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=kolom)
        w.writeheader()
        r = dict(rows[0])
        r["report_id"] = "RPT-SCHEMA-01"
        r["jiwa_terdampak"] = "banyak sekali"
        w.writerow(r)
    catat(manifest, salah, "sintetis ber-seed", "sintetis", "", 1,
          "jiwa_terdampak bertipe teks; dipakai menguji penolakan skema oleh Delta Lake")

    print(f"  [OK] laporan edge: {len(semua)} baris "
          f"(8 revisi terlambat, 5 konflik, 6 koordinat invalid, 4 tanpa identitas, 3 duplikat)")
    return len(semua)


def verifikasi():
    path = DATA / "MANIFEST.json"
    if not path.exists():
        print("MANIFEST.json belum ada. Jalankan tanpa --verify lebih dulu.")
        return 1
    m = json.loads(path.read_text(encoding="utf-8"))
    rusak = 0
    print(f"{'berkas':<42} {'status':<10} {'baris':>8}  verifikasi")
    for b in m["berkas"]:
        p = ROOT / b["path"]
        if not p.exists():
            print(f"{b['path']:<42} {b['status']:<10} {'-':>8}  HILANG")
            rusak += 1
            continue
        ok = sha256(p) == b["sha256"]
        if not ok:
            rusak += 1
        print(f"{Path(b['path']).name:<42} {b['status']:<10} {str(b['baris']):>8}  "
              f"{'hash cocok' if ok else 'HASH BERBEDA'}")
    print(f"\nTotal berkas: {len(m['berkas'])} | bermasalah: {rusak}")
    return 1 if rusak else 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--verify", action="store_true", help="cek ulang hash & keberadaan berkas")
    ap.add_argument("--seed", type=int, default=20261112)
    ap.add_argument("--usgs-mulai", type=int, default=2000)
    ap.add_argument("--usgs-sampai", type=int, default=2026)
    args = ap.parse_args()

    if args.verify:
        return verifikasi()

    DATA.mkdir(exist_ok=True)
    RAW.mkdir(exist_ok=True)
    rng = random.Random(args.seed)
    manifest = {
        "praktikum": "Data Storage - Databricks Lakehouse",
        "kelompok": "Kelompok 2 - Big Data S2",
        "seed_sintetis": args.seed,
        "dibuat_pada": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "berkas": [],
    }
    gagal = []
    print("[1/4] BMKG (asli, streaming)")
    ambil_bmkg(manifest, gagal)
    print("[2/4] USGS (asli, batch historis)")
    n_usgs = ambil_usgs(manifest, gagal, args.usgs_mulai, args.usgs_sampai)
    print("[3/4] Wilayah NTT (nama faktual, populasi sintetis)")
    buat_wilayah(manifest, rng)
    print("[4/4] Laporan Edge Node (sintetis ber-seed)")
    buat_edge(manifest, rng)

    manifest["gagal"] = gagal
    (DATA / "MANIFEST.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nManifest: data/MANIFEST.json | berkas: {len(manifest['berkas'])} | USGS total: {n_usgs}")
    if gagal:
        print(f"\n{len(gagal)} unduhan GAGAL (data sintetis tetap dibuat):")
        for g in gagal:
            print("  -", g)
        return 1
    print("Semua sumber berhasil.")
    return 0


if __name__ == "__main__":
    sys.exit(main())