package agg

import (
	"encoding/json"
	"net/netip"
	"strings"
	"testing"

	dnstap "github.com/dnstap/golang-dnstap"
	"google.golang.org/protobuf/proto"
)

// TestTruncate covers the privacy-critical prefix reduction: full client
// IPs must never survive aggregation.
func TestTruncate(t *testing.T) {
	cases := []struct{ in, want string }{
		{"203.0.113.77", "203.0.113.0/24"},
		{"198.51.100.1", "198.51.100.0/24"},
		{"::ffff:203.0.113.77", "203.0.113.0/24"}, // v4-mapped v6 → unmap, /24
		{"2001:db8:cafe:beef::1", "2001:db8:cafe::/48"},
		{"2001:db8::1", "2001:db8::/48"},
	}
	for _, c := range cases {
		got := truncate(netip.MustParseAddr(c.in))
		if got != c.want {
			t.Errorf("truncate(%s) = %q, want %q", c.in, got, c.want)
		}
	}
}

// wireQuery builds a minimal DNS query payload: header + one question.
func wireQuery(name string, qtype uint16) []byte {
	msg := []byte{0x12, 0x34, 0x01, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0}
	for _, label := range strings.Split(name, ".") {
		msg = append(msg, byte(len(label)))
		msg = append(msg, label...)
	}
	msg = append(msg, 0, byte(qtype>>8), byte(qtype), 0, 0x01)
	return msg
}

func TestQtypeOf(t *testing.T) {
	cases := []struct {
		name  string
		qtype uint16
		want  string
	}{
		{"www.example.com", 1, "A"},
		{"example.com", 28, "AAAA"},
		{"example.com", 6, "SOA"},
		{"example.com", 999, "TYPE999"},
	}
	for _, c := range cases {
		if got := qtypeOf(wireQuery(c.name, c.qtype)); got != c.want {
			t.Errorf("qtypeOf(%s %d) = %q, want %q", c.name, c.qtype, got, c.want)
		}
	}
	if got := qtypeOf([]byte{1, 2, 3}); got != "" {
		t.Errorf("qtypeOf(short) = %q, want empty", got)
	}
}

func frame(t *testing.T, identity string, mt dnstap.Message_Type, clientIP string, payload []byte, ts uint64) []byte {
	t.Helper()
	msg := &dnstap.Message{
		Type:         mt.Enum(),
		SocketFamily: dnstap.SocketFamily_INET.Enum(),
	}
	addr := netip.MustParseAddr(clientIP)
	if addr.Is6() {
		msg.SocketFamily = dnstap.SocketFamily_INET6.Enum()
	}
	msg.QueryAddress = addr.AsSlice()
	msg.QueryTimeSec = &ts
	proto_ := dnstap.SocketProtocol_UDP
	msg.SocketProtocol = &proto_
	if payload != nil {
		msg.QueryMessage = payload
	}
	tap := &dnstap.Dnstap{
		Type:     dnstap.Dnstap_MESSAGE.Enum(),
		Identity: []byte(identity),
		Message:  msg,
	}
	b, err := proto.Marshal(tap)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestConsumeNeverLeaksClientIP feeds a frame with a full client IP and a
// query name and asserts that neither appears in the flushed records.
func TestConsumeNeverLeaksClientIP(t *testing.T) {
	a := New("", true)
	a.Consume(frame(t, "node-1", dnstap.Message_AUTH_QUERY,
		"203.0.113.77", wireQuery("secret-host.example.com", 1), 1700000000))

	recs := a.FlushBefore(1 << 62)
	if len(recs) != 1 {
		t.Fatalf("got %d records, want 1", len(recs))
	}
	r := recs[0]
	if r.Node != "node-1" || r.MsgType != "AUTH_QUERY" || r.AF != "v4" ||
		r.Proto != "UDP" || r.QType != "A" || r.Count != 1 {
		t.Errorf("unexpected record: %+v", r)
	}
	if r.ClientPfx != "203.0.113.0/24" {
		t.Errorf("client prefix = %q, want 203.0.113.0/24", r.ClientPfx)
	}
	out, _ := json.Marshal(recs)
	for _, leak := range []string{"203.0.113.77", "secret-host", "example.com"} {
		if strings.Contains(string(out), leak) {
			t.Errorf("record output leaks %q: %s", leak, out)
		}
	}
}

// TestConsumeGranularityNone: without the prefix dimension no client data
// at all may appear.
func TestConsumeGranularityNone(t *testing.T) {
	a := New("override", false)
	a.Consume(frame(t, "node-1", dnstap.Message_CLIENT_QUERY,
		"2001:db8:cafe:beef::1", wireQuery("www.example.com", 28), 1700000000))

	recs := a.FlushBefore(1 << 62)
	if len(recs) != 1 {
		t.Fatalf("got %d records, want 1", len(recs))
	}
	if recs[0].ClientPfx != "" {
		t.Errorf("client prefix = %q, want empty", recs[0].ClientPfx)
	}
	if recs[0].Node != "override" {
		t.Errorf("node = %q, want override (node_id beats identity)", recs[0].Node)
	}
}

// TestFlushBeforeCutoff: only completed minutes leave the aggregator.
func TestFlushBeforeCutoff(t *testing.T) {
	a := New("", false)
	a.Consume(frame(t, "n", dnstap.Message_AUTH_QUERY, "203.0.113.1", wireQuery("a.example", 1), 100))
	a.Consume(frame(t, "n", dnstap.Message_AUTH_QUERY, "203.0.113.1", wireQuery("a.example", 1), 200))

	if got := len(a.FlushBefore(120)); got != 1 {
		t.Fatalf("flush(120) = %d records, want 1", got)
	}
	if got := len(a.FlushBefore(1 << 62)); got != 1 {
		t.Fatalf("second flush = %d records, want 1", got)
	}
	// Bad frames are counted, not records.
	a.Consume([]byte("not a dnstap frame"))
	if a.BadFrame != 1 {
		t.Errorf("BadFrame = %d, want 1", a.BadFrame)
	}
	if got := len(a.FlushBefore(1 << 62)); got != 0 {
		t.Errorf("flush after bad frame = %d records, want 0", got)
	}
}
