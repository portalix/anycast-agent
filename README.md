# anycast-agent

Passive measurement agent for anycast DNS networks, built for
[anycast.dev](https://anycast.dev). It reads **dnstap** from the local
nameserver, aggregates metadata into per-minute sums and pushes them.

**Query names and full client IPs never leave the server.** Clients are
aggregated to /24 (IPv4) resp. /48 (IPv6) — or dropped entirely with
`granularity: none`. This repository is public precisely so operators can
verify that claim in the code before running the binary on an authoritative
nameserver: the whole data path is
[`internal/agg/agg.go`](internal/agg/agg.go), and
[`internal/agg/agg_test.go`](internal/agg/agg_test.go) asserts that neither
a query name nor a full client IP survives aggregation.

The agent is deliberately small: one static binary, one YAML file, no
plugins, no configurable pipeline you could accidentally export raw
queries through.

## Supported producers

**Knot** (mod-dnstap), **NSD** (`dnstap:`), **BIND** (`dnstap-output`),
**PowerDNS behind dnsdist** (`DnstapLogAction`), **Unbound** (`dnstap:`).

All five are exercised against real servers in [`zoo/`](zoo/) — a Docker
compose setup with one container per implementation, a traffic generator
and an end-to-end test (`zoo/e2e.sh`: answers, dnstap flow per dialect,
reconnect after an agent restart, zero drops). Note that Unbound, being a
resolver, logs `CLIENT_QUERY`/`CLIENT_RESPONSE` where authoritative
servers log `AUTH_*`; the agent counts the message type as its own
dimension.

## How it works

- The agent listens as a Frame Streams **server** on one or more Unix
  sockets; the nameserver connects as a client.
- Backpressure rule: full queue = drop the frame, never block (DNS comes
  first). Drops are counted and logged.
- Per minute: sums per (node, message type, address family, protocol,
  QTYPE, client prefix) as JSON — to an HTTP endpoint (`push.url`) or
  stdout (empty).
- Node ID = the producer's dnstap identity; can be overridden via
  `node_id`.

Example record:

```json
{"minute":"2026-08-30T22:49:00Z","node":"ns1","type":"AUTH_QUERY",
 "af":"v4","proto":"UDP","qtype":"A","client_prefix":"203.0.113.0/24","count":42}
```

## Install & run

Prebuilt static binaries (Linux amd64/arm64, FreeBSD amd64) with checksums
are on the [releases page](https://github.com/portalix/anycast-agent/releases)
— or build from source:

```sh
go build ./cmd/anycast-agent
./anycast-agent -config config.example.yaml
```

One binary, one YAML ([`config.example.yaml`](config.example.yaml)), a
systemd unit under [`systemd/`](systemd/). Try it without sending anything
anywhere: leave `push.url` empty and the minute sums go to stdout.

Socket permissions: the agent creates the socket (`mode`, default 0666);
tighten via a shared group + 0660.

## Test

```sh
go test ./...
zoo/e2e.sh   # ~5 minutes, needs Docker
```

## License

Apache-2.0 — see [LICENSE](LICENSE).
