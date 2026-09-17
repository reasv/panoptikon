#!/bin/bash
source $HOME/tmp-run4-proxy/lib.sh
ALLOW=panoptikon.example.com
DENY=denied.example.net
ROUTES="/api/client-config /api/db /api/search/stats"

pp() { # id shape gateway-label port
  local ID=$1 SH=$2 L=$3 P=$4
  for R in $ROUTES; do
    row "$ID" "$SH; client Host=allowed" "$R" "$L" "$(cprobe --http1.1 -H "Host: $ALLOW" http://127.0.0.1:$P$R)"
    row "$ID" "$SH; client Host=denied"  "$R" "$L" "$(cprobe --http1.1 -H "Host: $DENY"  http://127.0.0.1:$P$R)"
  done
}

printf 'id\tshape\troute\tgateway\tstatus\tpolicy\n'
pp N1 "nginx h1.1 up, Host=\$host"                    master-trustfalse 6861
pp N1 "nginx h1.1 up, Host=\$host"                    branch-trustfalse 6862
pp N2 "nginx h1.1 up, Host=upstream-name + XFH"       master-trustfalse 6863
pp N2 "nginx h1.1 up, Host=upstream-name + XFH"       master-trusttrue  6864
pp N2 "nginx h1.1 up, Host=upstream-name + XFH"       branch-trustfalse 6865
pp N2 "nginx h1.1 up, Host=upstream-name + XFH"       branch-trusttrue  6866
pp N3 "nginx h1.1 up to legacy_ui port, Host=\$host"  master-legacy     6867
pp N3 "nginx h1.1 up to legacy_ui port, Host=\$host"  branch-legacy     6868
pp N5a "nginx h2c up, Host=\$host"                    master-trustfalse 6877
pp N5a "nginx h2c up, Host=\$host"                    branch-trustfalse 6878
pp N5b "nginx h2c up, Host=upstream-name + XFH"       master-trustfalse 6879
pp N5b "nginx h2c up, Host=upstream-name + XFH"       branch-trustfalse 6880
pp N5c "nginx h2c up, Host=upstream-name + XFH"       master-trusttrue  6881
pp N5c "nginx h2c up, Host=upstream-name + XFH"       branch-trusttrue  6882
pp C1 "caddy h2c up, Host preserved"                  master-trustfalse 6871
pp C1 "caddy h2c up, Host preserved"                  branch-trustfalse 6872
pp C2 "caddy h2c up, Host=upstream-hostport + XFH"    master-trustfalse 6873
pp C2 "caddy h2c up, Host=upstream-hostport + XFH"    branch-trustfalse 6874
pp C3 "caddy h2c up, Host=upstream-hostport + XFH"    master-trusttrue  6875
pp C3 "caddy h2c up, Host=upstream-hostport + XFH"    branch-trusttrue  6876
