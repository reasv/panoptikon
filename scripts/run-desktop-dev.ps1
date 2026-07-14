[CmdletBinding()]
param(
    [switch]$SkipUiBuild
)

$ErrorActionPreference = "Stop"
$repoRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$uiRoot = Join-Path $repoRoot "ui"
$standalone = Join-Path $uiRoot ".next\standalone"
$desktopRoot = Join-Path $repoRoot "panoptikon-desktop"
$sidecar = Join-Path $repoRoot "target\debug\panoptikon.exe"
$stagedSidecar = Join-Path $desktopRoot "src-tauri\binaries\panoptikon-x86_64-pc-windows-msvc.exe"

function Invoke-Checked {
    param([string]$FilePath, [string[]]$Arguments)
    & $FilePath @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed ($LASTEXITCODE): $FilePath $($Arguments -join ' ')"
    }
}

if (-not $SkipUiBuild) {
    Write-Host "Building the current Panoptikon UI for Desktop Dev..."
    Push-Location $uiRoot
    $oldStandalone = $env:BUILD_STANDALONE
    try {
        $env:BUILD_STANDALONE = "true"
        if (-not (Test-Path -LiteralPath (Join-Path $uiRoot "node_modules") -PathType Container)) {
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
    }
    finally {
        $env:BUILD_STANDALONE = $oldStandalone
        Pop-Location
    }
}

if (-not (Test-Path -LiteralPath (Join-Path $standalone "server.js") -PathType Leaf)) {
    throw "No standalone UI exists at '$standalone'. Run again without -SkipUiBuild."
}

Write-Host "Building and staging the current debug Server sidecar..."
$oldBundle = $env:PANOPTIKON_UI_BUNDLE
try {
    $env:PANOPTIKON_UI_BUNDLE = $standalone
    Push-Location $repoRoot
    try {
        Invoke-Checked "cargo.exe" @("build", "-p", "panoptikon", "--features", "bundled,bundled-ui")
    }
    finally {
        Pop-Location
    }
}
finally {
    $env:PANOPTIKON_UI_BUNDLE = $oldBundle
}

if (-not (Test-Path -LiteralPath $sidecar -PathType Leaf)) {
    throw "The debug Server build did not produce '$sidecar'."
}
New-Item -ItemType Directory -Force -Path (Split-Path $stagedSidecar -Parent) | Out-Null
Copy-Item -LiteralPath $sidecar -Destination $stagedSidecar -Force

Write-Host "Starting unpackaged Panoptikon Desktop Dev (Ctrl+C to stop)..."
Push-Location $desktopRoot
try {
    Invoke-Checked "npx.cmd" @(
        "@tauri-apps/cli@2.11.4", "dev",
        "--config", "src-tauri/tauri.dev.conf.json"
    )
}
finally {
    Pop-Location
}
