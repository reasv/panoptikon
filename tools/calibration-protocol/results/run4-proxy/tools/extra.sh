#!/bin/bash
source $HOME/tmp-run4-proxy/lib.sh
ALLOW=panoptikon.example.com
DENY=denied.example.net
printf 'id\tshape\troute\tgateway\tstatus\tpolicy\n'
for g in master-trustfalse:6842 branch-trustfalse:6852 master-strict:6846 branch-strict:6856; do
  L=${g%%:*}; P=${g##*:}
  R=/api/client-config
  row U1 "h1.1 Host=evil.example.net@allowed"      "$R" "$L" "$(cprobe --http1.1 -H "Host: evil.example.net@$ALLOW" http://127.0.0.1:$P$R)"
  row U2 "h1.1 Host=allowed@denied"                "$R" "$L" "$(cprobe --http1.1 -H "Host: $ALLOW@$DENY" http://127.0.0.1:$P$R)"
  row U3 "h2c :authority=evil.example.net@allowed" "$R" "$L" "$(hprobe 127.0.0.1 $P $R --authority "evil.example.net@$ALLOW")"
  row U4 "h1.1 Host=PANOPTIKON.EXAMPLE.COM"        "$R" "$L" "$(cprobe --http1.1 -H "Host: PANOPTIKON.EXAMPLE.COM" http://127.0.0.1:$P$R)"
  row U5 "h1.1 Host=allowed:8443"                  "$R" "$L" "$(cprobe --http1.1 -H "Host: $ALLOW:8443" http://127.0.0.1:$P$R)"
done
