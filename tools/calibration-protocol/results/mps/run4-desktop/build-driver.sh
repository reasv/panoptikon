#!/bin/bash
export PATH=$HOME/.cargo/bin:$HOME/.local/bin:/opt/homebrew/bin:$PATH
cd ~/projects/panoptikon-pr27
mkdir -p .mac/desktop
date -u +"start %Y-%m-%dT%H:%M:%SZ" > .mac/desktop/build.status
bash scripts/build-desktop-dev.sh --release-desktop > .mac/desktop/build-desktop.log 2>&1
rc=$?
echo "BUILD_RC=$rc" >> .mac/desktop/build.status
date -u +"end %Y-%m-%dT%H:%M:%SZ" >> .mac/desktop/build.status
