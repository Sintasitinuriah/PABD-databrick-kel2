param(
    [ValidateSet('status', 'bronze', 'silver', 'gold', 'verifikasi', 'eksperimen', 'e6', 'semua', 'matikan', 'nyalakan')]
    [string]$Langkah = 'status'
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$compose = Join-Path $root 'lokal\docker-compose.yml'
$py = '/usr/bin/python3.10'
$container = 'delta-lake-lab'

function Tulis-Judul($t) { Write-Host "`n=== $t ===" -ForegroundColor Cyan }

function Jalankan-Python($argumen) {
    docker exec $container bash -lc "cd /workspace && $py $argumen"
    if ($LASTEXITCODE -ne 0) { throw "Gagal: $argumen (exit $LASTEXITCODE)" }
}

function Pastikan-Hidup {
    $ada = docker ps --filter "name=$container" --format '{{.Names}}'
    if (-not $ada) {
        Tulis-Judul 'Menyalakan container'
        docker compose -f $compose up -d
        Start-Sleep -Seconds 5
    }
}

switch ($Langkah) {
    'nyalakan' {
        Tulis-Judul 'Menyalakan lingkungan Delta'
        docker compose -f $compose up -d
        Start-Sleep -Seconds 5
        docker ps --filter "name=$container" --format '{{.Names}} | {{.Status}}'
    }
    'status' {
        Tulis-Judul 'Status container'
        docker ps --filter "name=$container" --format '{{.Names}} | {{.Status}} | {{.Ports}}'
        Tulis-Judul 'Versi (diharapkan: pyspark 4.2.0, delta 4.4.0)'
        docker exec $container bash -lc "$py -c `"import pyspark, delta; print('pyspark', pyspark.__version__); print('delta', delta.__version__)`""
        Tulis-Judul 'Tabel Delta yang sudah ada'
        docker exec $container bash -lc "ls -1 /delta/bronze /delta/silver /delta/gold 2>/dev/null"
    }
    'bronze' { Pastikan-Hidup; Tulis-Judul 'BRONZE'; Jalankan-Python 'lokal/pipeline_medallion.py --lapisan bronze' }
    'silver' { Pastikan-Hidup; Tulis-Judul 'SILVER'; Jalankan-Python 'lokal/pipeline_medallion.py --lapisan silver' }
    'gold'   { Pastikan-Hidup; Tulis-Judul 'GOLD';   Jalankan-Python 'lokal/pipeline_medallion.py --lapisan gold' }
    'verifikasi' { Pastikan-Hidup; Tulis-Judul 'Gerbang M3'; Jalankan-Python 'lokal/pipeline_medallion.py --verify' }
    'eksperimen' {
        Pastikan-Hidup
        Tulis-Judul 'E1-E3 (inti storage)'
        Jalankan-Python 'lokal/eksperimen_storage.py --eksperimen inti'
        Tulis-Judul 'E4-E5 (file kecil & retensi)'
        Jalankan-Python 'lokal/eksperimen_storage.py --eksperimen e4'
        Jalankan-Python 'lokal/eksperimen_storage.py --eksperimen e5'
    }
    'e6' {
        Pastikan-Hidup
        Tulis-Judul 'E6 fase sebelum: catat hash tabel'
        Jalankan-Python 'lokal/eksperimen_storage.py --eksperimen e6 --e6-fase sebelum'
        Tulis-Judul 'Mematikan compute (container Spark)'
        docker stop $container | Out-Null
        Write-Host 'Container mati. Data di volume tetap ada.'
        Start-Sleep -Seconds 3
        Tulis-Judul 'Menyalakan kembali compute'
        docker start $container | Out-Null
        Start-Sleep -Seconds 8
        Tulis-Judul 'E6 fase sesudah: bandingkan hash (diharapkan IDENTIK)'
        Jalankan-Python 'lokal/eksperimen_storage.py --eksperimen e6 --e6-fase sesudah'
    }
    'semua' {
        Pastikan-Hidup
        Tulis-Judul 'Pipeline lengkap Bronze -> Silver -> Gold + gerbang'
        Jalankan-Python 'lokal/pipeline_medallion.py --lapisan semua'
        Tulis-Judul 'Eksperimen storage E1-E5'
        Jalankan-Python 'lokal/eksperimen_storage.py --eksperimen semua'
        Tulis-Judul 'E6 pemisahan storage-compute'
        & (Join-Path $PSScriptRoot 'jalankan_semua.ps1') -Langkah e6
    }
    'matikan' {
        Tulis-Judul 'Mematikan lingkungan (data di volume tetap tersimpan)'
        docker compose -f $compose down
        Write-Host 'Volume lokal_delta_data tidak dihapus. Container tesis tidak disentuh.'
    }
}