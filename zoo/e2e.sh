#!/bin/bash
# End-to-end test of the docker zoo: builds everything, checks answers from
# all five servers, dnstap flow of all five dialects and reconnect after an
# agent restart. Takes ~5 minutes (two full minute flushes). Exit 0 = green.
set -u
cd "$(dirname "$0")"
fail() { echo "FAIL: $*" >&2; exit 1; }

echo "== build + up"
docker compose up -d --build --quiet-pull 2>&1 | tail -1 || fail "compose up"

echo "== answers from all five servers"
sleep 8
for s in knot nsd bind pdns unbound; do
    out=$(docker compose exec trafficgen dig +short +time=3 @$s www.zoo.test A 2>/dev/null)
    [ "$out" = "192.0.2.11" ] || fail "$s does not answer correctly: '$out'"
    echo "   $s: $out"
done

echo "== dnstap flow of all five dialects (waiting for minute flush)"
sleep 80
for n in knot-zoo nsd-zoo bind-zoo pdns-zoo unbound-zoo; do
    c=$(docker compose logs --since 90s agent 2>/dev/null | grep -c "\"node\":\"$n\"")
    [ "$c" -gt 0 ] || fail "no dnstap from $n"
    echo "   $n: $c records"
done

echo "== reconnect: restart the agent, all producers must reconnect"
docker compose restart agent >/dev/null 2>&1
sleep 140
for n in knot-zoo nsd-zoo bind-zoo pdns-zoo unbound-zoo; do
    c=$(docker compose logs --since 80s agent 2>/dev/null | grep -c "\"node\":\"$n\"")
    [ "$c" -gt 0 ] || fail "$n did not reconnect after the agent restart"
    echo "   $n: $c records after restart"
done

drops=$(docker compose logs --since 40s agent 2>/dev/null | grep stats | tail -1)
echo "== $drops"
echo "E2E GREEN"
