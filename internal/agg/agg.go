// Package agg decodes dnstap frames into metadata and aggregates them into
// per-minute sums. ONLY sums leave the server: minute, node, message type,
// address family, protocol, QTYPE, client prefix (/24 resp. /48). Query
// names and full client IPs are never stored or sent.
package agg

import (
	"fmt"
	"net/netip"
	"sync"
	"time"

	dnstap "github.com/dnstap/golang-dnstap"
	"google.golang.org/protobuf/proto"
)

type Key struct {
	Minute    int64  // Unix seconds, floored to the minute
	Node      string // dnstap identity, or the node_id override
	MsgType   string // AUTH_QUERY, CLIENT_QUERY, …
	AF        string // v4 | v6
	Proto     string // UDP | TCP | DOT | DOH | …
	QType     string // A, AAAA, … (only set for *_QUERY)
	ClientPfx string // 203.0.113.0/24 resp. 2001:db8::/48; empty for granularity=none
}

type Record struct {
	Minute    string `json:"minute"`
	Node      string `json:"node"`
	MsgType   string `json:"type"`
	AF        string `json:"af,omitempty"`
	Proto     string `json:"proto,omitempty"`
	QType     string `json:"qtype,omitempty"`
	ClientPfx string `json:"client_prefix,omitempty"`
	Count     uint64 `json:"count"`
}

type Aggregator struct {
	mu       sync.Mutex
	buckets  map[Key]uint64
	nodeID   string // override; empty = identity from the dnstap frame
	prefixes bool   // granularity=prefix
	Decoded  uint64 // frames decoded successfully (under mu)
	BadFrame uint64 // frames that did not parse as dnstap
}

func New(nodeID string, prefixes bool) *Aggregator {
	return &Aggregator{buckets: make(map[Key]uint64), nodeID: nodeID, prefixes: prefixes}
}

// Consume decodes one frame and counts it into its minute bucket.
func (a *Aggregator) Consume(frame []byte) {
	tap := &dnstap.Dnstap{}
	if err := proto.Unmarshal(frame, tap); err != nil || tap.Message == nil {
		a.mu.Lock()
		a.BadFrame++
		a.mu.Unlock()
		return
	}
	m := tap.Message

	k := Key{MsgType: m.GetType().String()}

	k.Node = a.nodeID
	if k.Node == "" {
		k.Node = string(tap.GetIdentity())
	}
	if k.Node == "" {
		k.Node = "unknown"
	}

	switch m.GetSocketFamily() {
	case dnstap.SocketFamily_INET:
		k.AF = "v4"
	case dnstap.SocketFamily_INET6:
		k.AF = "v6"
	}
	if m.SocketProtocol != nil {
		k.Proto = m.GetSocketProtocol().String()
	}

	// Time source: the producer's query time, else response time, else clock.
	var ts int64
	switch {
	case m.QueryTimeSec != nil:
		ts = int64(m.GetQueryTimeSec())
	case m.ResponseTimeSec != nil:
		ts = int64(m.GetResponseTimeSec())
	default:
		ts = time.Now().Unix()
	}
	k.Minute = ts - ts%60

	// QTYPE only from query payloads; response types count without a QTYPE.
	if payload := m.GetQueryMessage(); payload != nil && isQueryType(m.GetType()) {
		k.QType = qtypeOf(payload)
	}

	// Client address: for all types with a client side, QueryAddress is the
	// client (for *_RESPONSE too — that is where the answer goes).
	if a.prefixes {
		if addr, ok := netip.AddrFromSlice(m.GetQueryAddress()); ok {
			k.ClientPfx = truncate(addr)
		}
	}

	a.mu.Lock()
	a.buckets[k]++
	a.Decoded++
	a.mu.Unlock()
}

// FlushBefore removes and returns all buckets with Minute < cutoff (Unix seconds).
func (a *Aggregator) FlushBefore(cutoff int64) []Record {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []Record
	for k, n := range a.buckets {
		if k.Minute >= cutoff {
			continue
		}
		out = append(out, Record{
			Minute:    time.Unix(k.Minute, 0).UTC().Format(time.RFC3339),
			Node:      k.Node,
			MsgType:   k.MsgType,
			AF:        k.AF,
			Proto:     k.Proto,
			QType:     k.QType,
			ClientPfx: k.ClientPfx,
			Count:     n,
		})
		delete(a.buckets, k)
	}
	return out
}

func isQueryType(t dnstap.Message_Type) bool {
	switch t {
	case dnstap.Message_AUTH_QUERY, dnstap.Message_CLIENT_QUERY,
		dnstap.Message_RESOLVER_QUERY, dnstap.Message_FORWARDER_QUERY,
		dnstap.Message_STUB_QUERY, dnstap.Message_TOOL_QUERY:
		return true
	}
	return false
}

// truncate reduces the client IP to /24 (v4) resp. /48 (v6).
func truncate(addr netip.Addr) string {
	bits := 48
	if addr.Is4() || addr.Is4In6() {
		addr = addr.Unmap()
		bits = 24
	}
	p, err := addr.Prefix(bits)
	if err != nil {
		return ""
	}
	return p.String()
}

// qtypeOf reads the QTYPE of the first question from a DNS wire payload.
// Deliberately minimal: labels are skipped, the QNAME is never materialized.
func qtypeOf(msg []byte) string {
	if len(msg) < 12 {
		return ""
	}
	qdcount := int(msg[4])<<8 | int(msg[5])
	if qdcount == 0 {
		return ""
	}
	i := 12
	for i < len(msg) {
		l := int(msg[i])
		if l == 0 {
			i++
			break
		}
		if l&0xC0 != 0 { // compression pointer: 2 bytes, end of name
			i += 2
			break
		}
		i += 1 + l
	}
	if i+2 > len(msg) {
		return ""
	}
	return qtypeName(uint16(msg[i])<<8 | uint16(msg[i+1]))
}

var qtypeNames = map[uint16]string{
	1: "A", 2: "NS", 5: "CNAME", 6: "SOA", 12: "PTR", 15: "MX", 16: "TXT",
	28: "AAAA", 33: "SRV", 35: "NAPTR", 43: "DS", 46: "RRSIG", 47: "NSEC",
	48: "DNSKEY", 50: "NSEC3", 64: "SVCB", 65: "HTTPS", 255: "ANY", 252: "AXFR",
	251: "IXFR", 257: "CAA",
}

func qtypeName(t uint16) string {
	if n, ok := qtypeNames[t]; ok {
		return n
	}
	return fmt.Sprintf("TYPE%d", t)
}
