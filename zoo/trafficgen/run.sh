#!/bin/bash
# Traffic generator: queries all five servers with varying names, QTYPEs
# and source IPs (secondary addresses in other /24s → exercises the
# agent's prefix aggregation). Needs NET_ADMIN.
set -u

SERVERS="knot nsd bind pdns unbound"
QTYPES="A AAAA TXT MX SOA NS"
NAMES="zoo.test www.zoo.test txt.zoo.test mx.zoo.test mail.zoo.test"

# Secondary IPs on the same L2 but in other /24s (the compose subnet is a /16).
DEV=eth0
BASE=$(ip -4 -o addr show "$DEV" | awk '{print $4}' | cut -d/ -f1)
PFX=$(echo "$BASE" | cut -d. -f1-2)
EXTRA_IPS="$PFX.201.10 $PFX.202.10 $PFX.203.10"
for ip in $EXTRA_IPS; do
    ip addr add "$ip/16" dev "$DEV" 2>/dev/null || true
done
SRC_IPS="$BASE $EXTRA_IPS"

echo "trafficgen: sources: $SRC_IPS"
sleep 3  # let the servers come up

i=0
while true; do
    for srv in $SERVERS; do
        name=$(echo $NAMES | tr ' ' '\n' | shuf -n1)
        qt=$(echo $QTYPES | tr ' ' '\n' | shuf -n1)
        src=$(echo $SRC_IPS | tr ' ' '\n' | shuf -n1)
        proto=""
        [ $((RANDOM % 5)) -eq 0 ] && proto="+tcp"
        dig +time=1 +tries=1 $proto -b "$src" @"$srv" "$name" "$qt" >/dev/null 2>&1
        i=$((i+1))
    done
    [ $((i % 200)) -lt 5 ] && echo "trafficgen: $i queries"
    sleep 0.05
done
