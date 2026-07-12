# Generates the CI smoke-test fixture consumed by .github/workflows/release.yml.
#
# Drives a RUNNING gateway (your normal local instance) over its HTTP API:
# copies a small sample of the dataset into a fixture folder, creates a fresh
# index DB, scans the folder, runs data extraction (tagging) on it, and packs
# the resulting DB + media files + manifest into fixture.tar.gz.
#
# Upload the result as a release asset on the dedicated fixtures tag:
#   gh release create ci-fixture --title "CI smoke-test fixture" --notes "See .github/scripts/generate_ci_fixture.ps1" fixture.tar.gz
# or to replace an existing one:
#   gh release upload ci-fixture fixture.tar.gz --clobber
#
# The DB this creates (index/<DbName>/ under your data folder) is left in
# place; delete that directory while the gateway is stopped if you don't
# want it showing up in your DB list.

param(
    [string]$Gateway = "http://127.0.0.1:6342",
    [string]$DbName = "cifixture",
    [string]$ImageSource = "Z:\var\datasets\animesfw",
    [string]$VideoSource = "Z:\var\datasets\videosample",
    [int]$ImageCount = 48,
    [string]$WorkDir = (Join-Path $PSScriptRoot "..\..\ci-fixture"),
    [string]$DataFolder = (Join-Path $PSScriptRoot "..\..\data"),
    [string[]]$InferenceIds = @("tags/wd-swinv2-tagger-v3"),
    [string[]]$TagCandidates = @("1girl", "solo", "long_hair", "1boy", "outdoors"),
    [int]$PollIntervalSec = 3,
    [int]$PollTimeoutSec = 3600
)

$ErrorActionPreference = "Stop"

function Wait-QueueIdle([string]$Phase) {
    $deadline = (Get-Date).AddSeconds($PollTimeoutSec)
    while ($true) {
        $queue = (Invoke-RestMethod "$Gateway/api/jobs/queue").queue
        $ours = @($queue | Where-Object { $_.index_db -eq $DbName })
        if ($ours.Count -eq 0) { return }
        if ((Get-Date) -gt $deadline) { throw "$Phase did not finish within ${PollTimeoutSec}s" }
        Write-Host "  [$Phase] $($ours.Count) job(s) pending for '$DbName'..."
        Start-Sleep -Seconds $PollIntervalSec
    }
}

function Get-TagCount([string]$Tag) {
    $body = @{
        query   = @{ match_tags = @{ tags = @($Tag); match_any = $true } }
        count   = $true
        results = $false
    } | ConvertTo-Json -Depth 10
    $resp = Invoke-RestMethod -Method Post -ContentType "application/json" -Body $body `
        "$Gateway/api/search/pql?index_db=$DbName"
    return [int]$resp.count
}

# --- 1. Select and copy sample files ---------------------------------------
$filesDir = Join-Path $WorkDir "files"
New-Item -ItemType Directory -Force $filesDir | Out-Null
Remove-Item (Join-Path $filesDir "*") -Force -ErrorAction SilentlyContinue

$imageExts = ".jpg", ".jpeg", ".png", ".webp", ".gif"
$images = Get-ChildItem $ImageSource -File -Recurse |
    Where-Object { $imageExts -contains $_.Extension.ToLower() } |
    Sort-Object FullName
if ($images.Count -lt $ImageCount) {
    throw "Only $($images.Count) images found under $ImageSource, need $ImageCount"
}
# Deterministic, evenly spaced selection across the dataset.
$step = [math]::Floor($images.Count / $ImageCount)
for ($i = 0; $i -lt $ImageCount; $i++) {
    $img = $images[$i * $step]
    Copy-Item $img.FullName (Join-Path $filesDir ("{0:d4}_{1}" -f $i, $img.Name))
}
$videos = @(Get-ChildItem $VideoSource -File -ErrorAction SilentlyContinue)
foreach ($v in $videos) { Copy-Item $v.FullName (Join-Path $filesDir $v.Name) }
Write-Host "Copied $ImageCount images and $($videos.Count) videos to $filesDir"

# --- 2. Create the fixture DB (idempotent, runs migrations) ----------------
$null = Invoke-RestMethod -Method Post "$Gateway/api/db/create?new_index_db=$DbName&new_user_data_db=$DbName"
Write-Host "Created index/user-data DB '$DbName'"

# --- 3. Point its folder config at the fixture folder ----------------------
$conf = Invoke-RestMethod "$Gateway/api/jobs/config?index_db=$DbName"
$conf.included_folders = @((Resolve-Path $filesDir).Path)
$null = Invoke-RestMethod -Method Put -ContentType "application/json" `
    -Body ($conf | ConvertTo-Json -Depth 32) "$Gateway/api/jobs/config?index_db=$DbName"
Wait-QueueIdle "folder-update"

# --- 4. Scan ----------------------------------------------------------------
$null = Invoke-RestMethod -Method Post "$Gateway/api/jobs/folders/rescan?index_db=$DbName"
Wait-QueueIdle "scan"
Write-Host "Scan complete"

# --- 5. Data extraction (tagging) -------------------------------------------
$idsQuery = ($InferenceIds | ForEach-Object { "inference_ids=" + [uri]::EscapeDataString($_) }) -join "&"
$null = Invoke-RestMethod -Method Post "$Gateway/api/jobs/data/extraction?index_db=$DbName&$idsQuery"
Wait-QueueIdle "extraction"
Write-Host "Extraction complete: $($InferenceIds -join ', ')"

# --- 6. Pick the manifest assertion tag -------------------------------------
$bestTag = $null
$bestCount = 0
foreach ($t in $TagCandidates) {
    $c = Get-TagCount $t
    Write-Host "  tag '$t': $c files"
    if ($c -gt $bestCount) { $bestTag = $t; $bestCount = $c }
}
if (-not $bestTag) {
    throw "None of the candidate tags matched any files; extraction likely failed"
}
Write-Host "Manifest tag: '$bestTag' ($bestCount files)"

# --- 7. Stage and pack -------------------------------------------------------
# The queue is idle for this DB, so its WAL sidecars (if present) are quiescent
# and safe to copy alongside the main files; SQLite recovers them on open.
$stage = Join-Path $WorkDir "stage"
if (Test-Path $stage) { Remove-Item $stage -Recurse -Force }
$stageDb = Join-Path $stage "index\$DbName"
New-Item -ItemType Directory -Force $stageDb | Out-Null

$srcDb = Join-Path $DataFolder "index\$DbName"
foreach ($pattern in @("index.db*", "storage.db*", "config.toml")) {
    Copy-Item (Join-Path $srcDb $pattern) $stageDb -ErrorAction SilentlyContinue
}
if (-not (Test-Path (Join-Path $stageDb "index.db"))) {
    throw "index.db not found under $srcDb - is -DataFolder pointing at the gateway's data folder?"
}
Copy-Item $filesDir (Join-Path $stage "files") -Recurse

@{
    db            = $DbName
    tag           = $bestTag
    count         = $bestCount
    inference_ids = $InferenceIds
    generated     = (Get-Date -Format "yyyy-MM-dd")
} | ConvertTo-Json | Set-Content (Join-Path $stage "manifest.json")

$outFile = Join-Path $WorkDir "fixture.tar.gz"
if (Test-Path $outFile) { Remove-Item $outFile -Force }
tar -czf $outFile -C $stage .
if ($LASTEXITCODE -ne 0) { throw "tar failed with exit code $LASTEXITCODE" }

Write-Host ""
Write-Host "Fixture written to $outFile"
Write-Host "Upload it with:"
Write-Host "  gh release create ci-fixture --title `"CI smoke-test fixture`" --notes `"Generated by .github/scripts/generate_ci_fixture.ps1`" `"$outFile`""
Write-Host "or replace the existing asset:"
Write-Host "  gh release upload ci-fixture `"$outFile`" --clobber"
