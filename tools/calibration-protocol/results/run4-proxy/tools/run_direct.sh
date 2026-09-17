#!/bin/bash
source $HOME/tmp-run4-proxy/lib.sh
ALLOW=panoptikon.example.com
DENY=denied.example.net
ROUTES="/api/client-config /api/db /api/search/stats"

# $1 label  $2 port
direct() {
  L=$1; P=$2
  for R in $ROUTES; do
    row D1 "h1.1 Host=allowed"              "$R" "$L" "$(cprobe --http1.1 -H "Host: $ALLOW" http://127.0.0.1:$P$R)"
    row D2 "h1.1 Host=denied"               "$R" "$L" "$(cprobe --http1.1 -H "Host: $DENY"  http://127.0.0.1:$P$R)"
    row D3 "h1.0 no Host"                   "$R" "$L" "$(cprobe --http1.0 -H 'Host:' http://127.0.0.1:$P$R)"
    row D4 "h1.1 abs-form auth=allowed"     "$R" "$L" "$(cprobe --http1.1 --proxy http://127.0.0.1:$P --resolve $ALLOW:80:127.0.0.1 http://$ALLOW$R)"
    row D5 "h1.1 abs-form auth=allowed Host=denied" "$R" "$L" "$(cprobe --http1.1 --proxy http://127.0.0.1:$P -H "Host: $DENY" http://$ALLOW$R)"
    row D6 "h1.1 abs-form auth=denied Host=allowed" "$R" "$L" "$(cprobe --http1.1 --proxy http://127.0.0.1:$P -H "Host: $ALLOW" http://$DENY$R)"
    row D7 "h2c :authority=allowed"         "$R" "$L" "$(hprobe 127.0.0.1 $P $R --authority $ALLOW:$P)"
    row D8 "h2c :authority=denied"          "$R" "$L" "$(hprobe 127.0.0.1 $P $R --authority $DENY:$P)"
    row D9 "h2c :authority=allowed host=denied"  "$R" "$L" "$(hprobe 127.0.0.1 $P $R --authority $ALLOW --host $DENY)"
    row D10 "h2c :authority=denied host=allowed" "$R" "$L" "$(hprobe 127.0.0.1 $P $R --authority $DENY --host $ALLOW)"
    row D11 "h2c no :authority host=allowed"     "$R" "$L" "$(hprobe 127.0.0.1 $P $R --host $ALLOW)"
    row D12 "h2c no :authority no host"          "$R" "$L" "$(hprobe 127.0.0.1 $P $R)"
  done
}

# forwarded-header shapes (trust flag varies by gateway)
fwd() {
  L=$1; P=$2
  R=/api/client-config
  row F1 "h1.1 Host=denied XFH=allowed"    "$R" "$L" "$(cprobe --http1.1 -H "Host: $DENY" -H "X-Forwarded-Host: $ALLOW" http://127.0.0.1:$P$R)"
  row F2 "h1.1 Host=allowed XFH=denied"    "$R" "$L" "$(cprobe --http1.1 -H "Host: $ALLOW" -H "X-Forwarded-Host: $DENY" http://127.0.0.1:$P$R)"
  row F3 "h1.1 Host=denied Forwarded=host=allowed" "$R" "$L" "$(cprobe --http1.1 -H "Host: $DENY" -H "Forwarded: for=203.0.113.9;host=$ALLOW;proto=https" http://127.0.0.1:$P$R)"
  row F4 "h2c :authority=denied XFH=allowed"  "$R" "$L" "$(hprobe 127.0.0.1 $P $R --authority $DENY --xfh $ALLOW)"
  row F5 "h2c :authority=allowed XFH=denied"  "$R" "$L" "$(hprobe 127.0.0.1 $P $R --authority $ALLOW --xfh $DENY)"
  row F6 "h2c no :authority XFH=allowed"      "$R" "$L" "$(hprobe 127.0.0.1 $P $R --xfh $ALLOW)"
}

# legacy [[server.endpoints]] listener
legacy() {
  L=$1; P=$2
  for R in $ROUTES; do
    row L1 "legacy port h1.1 Host=allowed" "$R" "$L" "$(cprobe --http1.1 -H "Host: $ALLOW" http://127.0.0.1:$P$R)"
    row L2 "legacy port h1.1 Host=denied"  "$R" "$L" "$(cprobe --http1.1 -H "Host: $DENY"  http://127.0.0.1:$P$R)"
    row L3 "legacy port h2c :authority=denied" "$R" "$L" "$(hprobe 127.0.0.1 $P $R --authority $DENY)"
    row L4 "legacy port h2c no authority no host" "$R" "$L" "$(hprobe 127.0.0.1 $P $R)"
  done
}

printf 'id\tshape\troute\tgateway\tstatus\tpolicy\n'
direct master-trustfalse 6842
direct branch-trustfalse 6852
direct master-strict     6846
direct branch-strict     6856
fwd master-trustfalse 6842
fwd branch-trustfalse 6852
fwd master-trusttrue  6844
fwd branch-trusttrue  6854
legacy master-trustfalse-legacy 6843
legacy branch-trustfalse-legacy 6853
