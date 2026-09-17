#!/bin/bash
# Wait for the box to be quiet. VERIFY deviation from final-n1's quiet.sh:
# this box now also runs an SGLang server (docker `dsv4-flash-sglang-vision`,
# started 2026-09-17T05:34Z) whose two `sglang::schedul` threads busy-wait at
# ~85 % of a core each, permanently, on 48 cores. That steady spin is part of
# the host and is excluded; anything ELSE over 50 % of a core is not, and
# neither is SGLang *loading* a model, which shows up in the load average
# (leg f2 passed the N1 gate, had the vision model loaded alongside it, and
# came back at half this host's per-batch throughput).
waited=0
while true; do
  load=$(awk '{print $1}' /proc/loadavg)
  foreign=$(pgrep -af 'cargo|rustc|target/release/panoptikon|inferio_worker|pytest' \
            | grep -v 'zsh -c' | grep -v 'quiet.sh' | grep -v 'bash /home/admin/tmp-vkg' | grep -vE '[0-9]+ ssh ' || true)
  busy=$(ps -eo pcpu,comm --sort=-pcpu --no-headers \
         | awk '$1 > 50 && $2 != "ps" && $2 !~ /^sglang::sched/ {print}' || true)
  ok_load=$(awk -v l="$load" 'BEGIN{print (l<4)?1:0}')
  if [ "$ok_load" = "1" ] && [ -z "$foreign" ] && [ -z "$busy" ]; then
    echo "QUIET after ${waited}s  load=${load}  (sglang steady: $(ps -eo pcpu,comm --no-headers | awk '$2 ~ /^sglang::sched/ {printf "%s ", $1}'))"
    exit 0
  fi
  if [ "$waited" -ge 1200 ]; then
    echo "NOT QUIET after ${waited}s  load=${load}"; echo "$foreign"; echo "$busy"; exit 1
  fi
  echo "waiting (${waited}s) load=${load}"
  [ -n "$foreign" ] && echo "  foreign: $foreign"
  [ -n "$busy" ] && echo "  busy: $busy"
  sleep 60
  waited=$((waited+60))
done
