[CmdletBinding()]
param(
    [switch]$Force,
    [switch]$WhatIf
)

$ErrorActionPreference = "Stop"
$identifier = "app.panoptikon.desktop.dev"
$configBase = [System.IO.Path]::GetFullPath([Environment]::GetFolderPath("ApplicationData")).TrimEnd('\')
$localBase = [System.IO.Path]::GetFullPath([Environment]::GetFolderPath("LocalApplicationData")).TrimEnd('\')
$targets = @(
    [System.IO.Path]::GetFullPath((Join-Path $configBase $identifier)),
    [System.IO.Path]::GetFullPath((Join-Path $localBase $identifier))
)

foreach ($target in $targets) {
    $parent = [System.IO.Path]::GetDirectoryName($target).TrimEnd('\')
    $leaf = [System.IO.Path]::GetFileName($target)
    if (($parent -ne $configBase -and $parent -ne $localBase) -or $leaf -ne $identifier) {
        throw "Refusing unsafe Desktop Dev reset target '$target'"
    }
}

$installRoot = [System.IO.Path]::GetFullPath((Join-Path $localBase "Panoptikon Desktop Dev")).TrimEnd('\')
$running = Get-Process -Name "panoptikon-desktop", "panoptikon" -ErrorAction SilentlyContinue |
    Where-Object {
        try {
            $_.Path -and [System.IO.Path]::GetFullPath($_.Path).StartsWith(
                "$installRoot\",
                [System.StringComparison]::OrdinalIgnoreCase
            )
        }
        catch {
            $false
        }
    }
if ($running) {
    $names = ($running | Select-Object -ExpandProperty ProcessName -Unique) -join ", "
    throw "Quit Panoptikon Desktop and Panoptikon Server before resetting state (running: $names)."
}

Write-Host "Panoptikon Desktop Dev state to remove:"
$targets | ForEach-Object { Write-Host "  $_" }
if ($WhatIf) {
    Write-Host "WhatIf: no files were removed."
    exit 0
}
if (-not $Force) {
    $answer = Read-Host "Type RESET to permanently remove this development state"
    if ($answer -cne "RESET") {
        Write-Host "Reset cancelled."
        exit 1
    }
}

foreach ($target in $targets) {
    if (Test-Path -LiteralPath $target) {
        Remove-Item -LiteralPath $target -Recurse -Force
        Write-Host "Removed $target"
    }
    else {
        Write-Host "Already absent: $target"
    }
}
Write-Host "Panoptikon Desktop Dev will run first-time setup on its next launch."
