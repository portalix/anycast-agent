package push

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/portalix/anycast-agent/internal/agg"
)

// ingest is a test ingest endpoint that can be switched between 500 and 200
// and keeps the bodies it accepted, in order.
type ingest struct {
	mu     sync.Mutex
	down   bool
	bodies [][]byte
}

func (i *ingest) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.down {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	i.bodies = append(i.bodies, body)
}

func (i *ingest) setDown(down bool) {
	i.mu.Lock()
	i.down = down
	i.mu.Unlock()
}

// payloads returns the accepted payloads, decoded.
func (i *ingest) payloads(t *testing.T) []Payload {
	t.Helper()
	i.mu.Lock()
	defer i.mu.Unlock()
	out := make([]Payload, 0, len(i.bodies))
	for _, b := range i.bodies {
		var p Payload
		if err := json.Unmarshal(b, &p); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		out = append(out, p)
	}
	return out
}

// assertMinutes checks which minutes the ingest accepted, and in which order.
func assertMinutes(t *testing.T, in *ingest, want ...string) {
	t.Helper()
	got := in.payloads(t)
	if len(got) != len(want) {
		t.Fatalf("ingest got %d payloads, want %d", len(got), len(want))
	}
	for i, w := range want {
		if got[i].Records[0].Minute != w {
			t.Errorf("payload %d = %s, want %s", i, got[i].Records[0].Minute, w)
		}
	}
}

func rec(minute string) []agg.Record {
	return []agg.Record{{Minute: minute, Node: "ns1", MsgType: "AUTH_QUERY", Count: 1}}
}

func spoolNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read spool: %v", err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// newTestPusher shortens the retry delay and freezes the clock: with every
// payload landing in the same millisecond, only the sequence number can
// carry the spool order.
func newTestPusher(t *testing.T, url, dir string) *Pusher {
	t.Helper()
	oldDelay, oldNow := retryDelay, now
	retryDelay = time.Millisecond
	frozen := time.Date(2026, 9, 4, 10, 0, 0, 0, time.UTC)
	now = func() time.Time { return frozen }
	t.Cleanup(func() { retryDelay, now = oldDelay, oldNow })
	return New(url, "", 2*time.Second, dir, 50)
}

// A failing push lands in the spool and is replayed, in order and with the
// original sent_at, once the ingest is back.
func TestSpoolReplayOrder(t *testing.T) {
	in := &ingest{down: true}
	srv := httptest.NewServer(in)
	defer srv.Close()

	dir := t.TempDir()
	p := newTestPusher(t, srv.URL, dir)

	p.Push(rec("2026-09-04T10:00:00Z"))
	p.Push(rec("2026-09-04T10:01:00Z"))
	if got := len(spoolNames(t, dir)); got != 2 {
		t.Fatalf("spool holds %d files, want 2", got)
	}
	if p.Spooled != 2 {
		t.Fatalf("Spooled=%d, want 2", p.Spooled)
	}
	if p.Failed != 0 {
		t.Fatalf("Failed=%d, want 0 (nothing lost while spooling)", p.Failed)
	}

	// sent_at of the first spooled payload must survive the replay.
	names := spoolNames(t, dir)
	body, err := os.ReadFile(filepath.Join(dir, names[0]))
	if err != nil {
		t.Fatalf("read spool file: %v", err)
	}
	var spooled Payload
	if err := json.Unmarshal(body, &spooled); err != nil {
		t.Fatalf("decode spool file: %v", err)
	}

	in.setDown(false)
	p.Push(rec("2026-09-04T10:02:00Z"))

	assertMinutes(t, in, "2026-09-04T10:00:00Z", "2026-09-04T10:01:00Z", "2026-09-04T10:02:00Z")
	got := in.payloads(t)
	if got[0].SentAt != spooled.SentAt {
		t.Errorf("sent_at rewritten on replay: %s, want %s", got[0].SentAt, spooled.SentAt)
	}
	if n := len(spoolNames(t, dir)); n != 0 {
		t.Errorf("spool holds %d files after replay, want 0", n)
	}
	if p.Spooled != 0 {
		t.Errorf("Spooled=%d after replay, want 0", p.Spooled)
	}
}

// While the spool is not empty the current batch must not overtake it.
func TestCurrentBatchWaitsForSpool(t *testing.T) {
	in := &ingest{down: true}
	srv := httptest.NewServer(in)
	defer srv.Close()

	dir := t.TempDir()
	p := newTestPusher(t, srv.URL, dir)
	p.Push(rec("2026-09-04T10:00:00Z"))
	p.Push(rec("2026-09-04T10:01:00Z"))

	// One payload per tick: the ingest is back, but the spool is not empty
	// after the replay, so the current batch has to queue up behind it.
	old := maxReplayPerTick
	maxReplayPerTick = 1
	defer func() { maxReplayPerTick = old }()

	in.setDown(false)
	p.Push(rec("2026-09-04T10:02:00Z"))
	got := in.payloads(t)
	if len(got) != 1 || got[0].Records[0].Minute != "2026-09-04T10:00:00Z" {
		t.Fatalf("first tick delivered %d payloads, want only the oldest spooled one", len(got))
	}
	if p.Spooled != 2 {
		t.Fatalf("Spooled=%d, want 2 (current batch spooled behind)", p.Spooled)
	}

	p.Push(nil) // a quiet minute still drains the spool
	p.Push(nil)
	assertMinutes(t, in, "2026-09-04T10:00:00Z", "2026-09-04T10:01:00Z", "2026-09-04T10:02:00Z")
	if p.Spooled != 0 {
		t.Errorf("Spooled=%d, want 0", p.Spooled)
	}
}

// The spool is capped: the oldest payloads go first and are counted as lost.
func TestSpoolCapDropsOldest(t *testing.T) {
	in := &ingest{down: true}
	srv := httptest.NewServer(in)
	defer srv.Close()

	dir := t.TempDir()
	p := newTestPusher(t, srv.URL, dir)
	p.spoolMax = 400 // bytes; a payload here is ~150 bytes

	for i := 0; i < 6; i++ {
		p.Push(rec("2026-09-04T10:0" + string(rune('0'+i)) + ":00Z"))
	}
	names := spoolNames(t, dir)
	if len(names) == 0 || len(names) >= 6 {
		t.Fatalf("spool holds %d files, want a capped subset of 6", len(names))
	}
	var total int64
	for _, n := range names {
		fi, err := os.Stat(filepath.Join(dir, n))
		if err != nil {
			t.Fatal(err)
		}
		total += fi.Size()
	}
	if total > p.spoolMax {
		t.Errorf("spool holds %d bytes, cap is %d", total, p.spoolMax)
	}
	if p.Failed == 0 {
		t.Errorf("Failed=0, want the dropped records counted")
	}
	if p.Spooled != len(names) {
		t.Errorf("Spooled=%d, want %d", p.Spooled, len(names))
	}

	// The newest payload must be the one that survived.
	in.setDown(false)
	p.Push(nil)
	got := in.payloads(t)
	if len(got) == 0 {
		t.Fatal("nothing replayed")
	}
	if last := got[len(got)-1].Records[0].Minute; last != "2026-09-04T10:05:00Z" {
		t.Errorf("newest replayed payload = %s, want 10:05", last)
	}
}

// A restart picks up what an earlier run spooled.
func TestRestartReadsSpool(t *testing.T) {
	in := &ingest{down: true}
	srv := httptest.NewServer(in)
	defer srv.Close()

	dir := t.TempDir()
	first := newTestPusher(t, srv.URL, dir)
	first.Push(rec("2026-09-04T10:00:00Z"))
	first.Push(rec("2026-09-04T10:01:00Z"))

	second := newTestPusher(t, srv.URL, dir) // "restart"
	if second.Spooled != 2 {
		t.Fatalf("Spooled=%d after restart, want 2", second.Spooled)
	}
	// The new run must continue the sequence, not overwrite or overtake
	// what the old one left behind.
	second.Push(rec("2026-09-04T10:02:00Z"))
	if second.Spooled != 3 {
		t.Fatalf("Spooled=%d, want 3", second.Spooled)
	}
	in.setDown(false)
	second.Push(rec("2026-09-04T10:03:00Z"))
	assertMinutes(t, in, "2026-09-04T10:00:00Z", "2026-09-04T10:01:00Z",
		"2026-09-04T10:02:00Z", "2026-09-04T10:03:00Z")
}

// Spool file names are never reused: after a replay removed the oldest
// file, the next payload must still sort behind the remaining ones — even
// when everything happens within the same millisecond (the clock alone is
// not a valid order).
func TestSpoolNamesKeepWriteOrder(t *testing.T) {
	in := &ingest{down: true}
	srv := httptest.NewServer(in)
	defer srv.Close()

	dir := t.TempDir()
	p := newTestPusher(t, srv.URL, dir)
	p.Push(rec("2026-09-04T10:00:00Z"))
	p.Push(rec("2026-09-04T10:01:00Z"))
	p.Push(rec("2026-09-04T10:02:00Z"))

	old := maxReplayPerTick
	maxReplayPerTick = 1
	in.setDown(false)
	p.Push(nil) // frees the oldest name
	maxReplayPerTick = old

	in.setDown(true)
	p.Push(rec("2026-09-04T10:03:00Z"))
	in.setDown(false)
	p.Push(rec("2026-09-04T10:04:00Z"))

	assertMinutes(t, in, "2026-09-04T10:00:00Z", "2026-09-04T10:01:00Z",
		"2026-09-04T10:02:00Z", "2026-09-04T10:03:00Z", "2026-09-04T10:04:00Z")
	if p.Spooled != 0 {
		t.Errorf("Spooled=%d, want 0", p.Spooled)
	}
}

// Without spool_dir the old behaviour stands: drop and count.
func TestNoSpoolDropsAsBefore(t *testing.T) {
	in := &ingest{down: true}
	srv := httptest.NewServer(in)
	defer srv.Close()

	p := newTestPusher(t, srv.URL, "")
	p.Push(rec("2026-09-04T10:00:00Z"))
	if p.Failed != 1 {
		t.Errorf("Failed=%d, want 1", p.Failed)
	}
	if p.Spooled != 0 {
		t.Errorf("Spooled=%d, want 0", p.Spooled)
	}
}

// Half-written spool files (tmp) are never replayed.
func TestTmpFilesAreIgnored(t *testing.T) {
	in := &ingest{}
	srv := httptest.NewServer(in)
	defer srv.Close()

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "0000000000001.json.tmp"), []byte("{"), 0o640); err != nil {
		t.Fatal(err)
	}
	p := newTestPusher(t, srv.URL, dir)
	if p.Spooled != 0 {
		t.Fatalf("Spooled=%d, want 0 (tmp file counted)", p.Spooled)
	}
	p.Push(rec("2026-09-04T10:00:00Z"))
	if got := in.payloads(t); len(got) != 1 {
		t.Fatalf("ingest got %d payloads, want 1", len(got))
	}
	for _, n := range spoolNames(t, dir) {
		if !strings.HasSuffix(n, ".tmp") {
			t.Errorf("unexpected spool file %s", n)
		}
	}
}
