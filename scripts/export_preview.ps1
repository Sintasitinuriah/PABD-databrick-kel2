# Ekspor seluruh slide PPTX ke PNG lewat PowerPoint COM untuk tinjauan visual.
# Pakai: powershell -NoProfile -ExecutionPolicy Bypass -File scripts\export_preview.ps1
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$pptx = Join-Path $root 'presentasi\Databricks_Kelompok2_Gempa_NTT.pptx'
$out  = Join-Path $root 'presentasi\preview_png'

$app = New-Object -ComObject PowerPoint.Application
try {
    $deck = $app.Presentations.Open($pptx, $true, $false, $false)
    $deck.SaveAs($out, 18)  # 18 = ppSaveAsPNG
    $deck.Close()
} finally {
    $app.Quit()
    [System.Runtime.Interopservices.Marshal]::ReleaseComObject($app) | Out-Null
}
Write-Output "EXPORT_OK -> $out"