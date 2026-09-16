PY=$HOME/tmp-run4-proxy/h2venv/bin/python
H2=$HOME/tmp-run4-proxy/h2req.py
OUT=$HOME/tmp-run4-proxy/out
BODY=$HOME/tmp-run4-proxy/.body

# parse: echo "<status> <policy|snippet>"
parse() {
  local st=$1
  local pol
  pol=$($PY - "$BODY" <<'PYEOF'
import json,sys
try:
    d=json.load(open(sys.argv[1]))
    print(d.get("policy") or d.get("detail") or "-")
except Exception:
    try:
        t=open(sys.argv[1]).read().strip().replace("\n"," ")
        print((t[:40] or "-"))
    except Exception:
        print("-")
PYEOF
)
  echo "$st|$pol"
}

# cprobe <curl args...>  -> "status|policy"
cprobe() {
  local st
  st=$(curl -sS -o "$BODY" -w '%{http_code}' --max-time 15 "$@" 2>/dev/null || echo 000)
  parse "$st"
}

# hprobe <host> <port> <path> [opts...] -> "status|policy"
hprobe() {
  local r
  r=$($PY $H2 "$@" 2>/dev/null) || r="000 err"
  echo "${r%% *}|${r#* }"
}

# row <id> <shape> <route> <binary> <result>
row() { printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" "${5%%|*}" "${5#*|}"; }
