[CmdletBinding()]
param(
    [switch]$SkipNpmCi,
    [switch]$ReleaseDesktop
)

$ErrorActionPreference = "Stop"
$repoRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$uiRoot = Join-Path $repoRoot "ui"
$standalone = Join-Path $uiRoot ".next\standalone"
$desktopRoot = Join-Path $repoRoot "panoptikon-desktop"
$sidecar = Join-Path $repoRoot "target\release\panoptikon.exe"
$stagedSidecar = Join-Path $desktopRoot "src-tauri\binaries\panoptikon-x86_64-pc-windows-msvc.exe"

function Invoke-Checked {
    param([string]$FilePath, [string[]]$Arguments)
    & $FilePath @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed ($LASTEXITCODE): $FilePath $($Arguments -join ' ')"
    }
}

Write-Host "Building standalone Panoptikon UI..."
Push-Location $uiRoot
$oldStandalone = $env:BUILD_STANDALONE
try {
    $env:BUILD_STANDALONE = "true"
    if (-not $SkipNpmCi) {
        Invoke-Checked "npm.cmd" @("ci")
    }
    Invoke-Checked "npm.cmd" @("run", "build")

    $staticSource = Join-Path $uiRoot ".next\static"
    $staticTarget = Join-Path $standalone ".next\static"
    if (Test-Path -LiteralPath $staticTarget) {
        Remove-Item -LiteralPath $staticTarget -Recurse -Force
    }
    New-Item -ItemType Directory -Force -Path (Split-Path $staticTarget -Parent) | Out-Null
    Copy-Item -LiteralPath $staticSource -Destination $staticTarget -Recurse

    $publicSource = Join-Path $uiRoot "public"
    $publicTarget = Join-Path $standalone "public"
    if (Test-Path -LiteralPath $publicSource) {
        if (Test-Path -LiteralPath $publicTarget) {
            Remove-Item -LiteralPath $publicTarget -Recurse -Force
        }
        Copy-Item -LiteralPath $publicSource -Destination $publicTarget -Recurse
    }
    if (-not (Test-Path -LiteralPath (Join-Path $standalone "server.js") -PathType Leaf)) {
        throw "The standalone UI build did not produce server.js"
    }
}
finally {
    $env:BUILD_STANDALONE = $oldStandalone
    Pop-Location
}

Write-Host "Building the bundled Panoptikon Server sidecar..."
$oldBundle = $env:PANOPTIKON_UI_BUNDLE
try {
    $env:PANOPTIKON_UI_BUNDLE = $standalone
    Push-Location $repoRoot
    try {
        Invoke-Checked "cargo.exe" @("build", "--release", "-p", "panoptikon", "--features", "bundled,bundled-ui")
    }
    finally {
        Pop-Location
    }
}
finally {
    $env:PANOPTIKON_UI_BUNDLE = $oldBundle
}

if (-not (Test-Path -LiteralPath $sidecar -PathType Leaf)) {
    throw "The Server build did not produce '$sidecar'"
}
New-Item -ItemType Directory -Force -Path (Split-Path $stagedSidecar -Parent) | Out-Null
Copy-Item -LiteralPath $sidecar -Destination $stagedSidecar -Force

Write-Host "Building Panoptikon Desktop Dev NSIS installer..."
$tauriArgs = @(
    "@tauri-apps/cli@2.11.4", "build", "--bundles", "nsis",
    "--config", "src-tauri/tauri.dev.conf.json"
)
if (-not $ReleaseDesktop) {
    $tauriArgs += "--debug"
}
Push-Location $desktopRoot
try {
    Invoke-Checked "npx.cmd" $tauriArgs
}
finally {
    Pop-Location
}

$profile = if ($ReleaseDesktop) { "release" } else { "debug" }
$bundleDir = Join-Path $repoRoot "target\$profile\bundle\nsis"
$installer = Get-ChildItem -LiteralPath $bundleDir -Filter "Panoptikon Desktop Dev*_x64-setup.exe" -File |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 1
if (-not $installer) {
    throw "No Panoptikon Desktop Dev installer was found under '$bundleDir'"
}
Write-Host "Desktop Dev installer: $($installer.FullName)"
