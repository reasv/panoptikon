#!/bin/bash
# Wait for the box to be quiet: load < 4 and no foreign cargo/rustc/panoptikon/
# inferio_worker/pytest. Polls every 60 s for at most 20 min. Prints the wait.
waited=0
while true; do
  load=$(awk '{print $1}' /proc/loadavg)
  foreign=$(pgrep -af 'cargo|rustc|target/release/panoptikon|inferio_worker|pytest' \
            | grep -v 'zsh -c' | grep -v 'quiet.sh' | grep -v 'bash /home/admin/tmp-n1' | grep -vE '[0-9]+ ssh ' || true)
  ok_load=$(awk -v l="$load" 'BEGIN{print (l<4)?1:0}')
  if [ "$ok_load" = "1" ] && [ -z "$foreign" ]; then
    echo "QUIET after ${waited}s  load=${load}"
    exit 0
  fi
  if [ "$waited" -ge 1200 ]; then
    echo "NOT QUIET after ${waited}s  load=${load}"
    echo "$foreign"
    exit 1
  fi
  echo "waiting (${waited}s) load=${load}"
  [ -n "$foreign" ] && echo "$foreign"
  sleep 60
  waited=$((waited+60))
done
