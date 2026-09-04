// Package push delivers the minute sums: HTTP POST (JSON) to an ingest
// endpoint, or JSON lines on stdout (empty url; debugging/zoo). Failed
// pushes are retried once and then written to an on-disk spool
// (push.spool_dir) if one is configured, so an ingest outage does not
// lose minutes. The spool is capped (push.spool_max_mb): when it is full
// the oldest payloads are dropped and counted — the agent does not buffer
// without bound (the backpressure rule applies here too).
package push

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/portalix/anycast-agent/internal/agg"
)

// Version is stamped by the release build (goreleaser, -X); source builds
// identify themselves as dev.
var Version = "0.0.0-dev"

var (
	// Delay between the two attempts of a regular push (var for tests).
	retryDelay = time.Second
	// Upper bound of spooled payloads replayed per tick, so a large spool
	// cannot stall a single tick; the rest follows on the next ticks.
	maxReplayPerTick = 200
	// Clock (var so tests can freeze it: the spool order must not depend
	// on it).
	now = time.Now
)

type Payload struct {
	Agent   string       `json:"agent"`
	SentAt  string       `json:"sent_at"`
	Records []agg.Record `json:"records"`
}

type Pusher struct {
	url      string
	token    string
	client   *http.Client
	spoolDir string // empty = spooling disabled
	spoolMax int64  // bytes
	seq      uint64 // next spool file number; monotonic, survives restarts
	// Failed counts records that were lost for good (no spool, or dropped
	// from a full spool). Spooled is the number of payloads currently
	// waiting in the spool.
	Failed  uint64
	Spooled int
}

func New(url, token string, timeout time.Duration, spoolDir string, spoolMaxMB int) *Pusher {
	p := &Pusher{url: url, token: token, client: &http.Client{Timeout: timeout}}
	if spoolDir == "" || url == "" {
		return p
	}
	if err := os.MkdirAll(spoolDir, 0o750); err != nil {
		log.Printf("push: spool disabled, %s: %v", spoolDir, err)
		return p
	}
	p.spoolDir = spoolDir
	p.spoolMax = int64(spoolMaxMB) * 1024 * 1024
	// A restart must not lose what an earlier run spooled, and must not
	// reuse its sequence numbers either.
	if names, err := p.spoolFiles(); err == nil {
		p.Spooled = len(names)
		if p.Spooled > 0 {
			for _, n := range names {
				if s := seqOf(n) + 1; s > p.seq {
					p.seq = s
				}
			}
			log.Printf("push: %d spooled payloads from an earlier run", p.Spooled)
		}
	}
	return p
}

func (p *Pusher) Push(records []agg.Record) {
	if p.url == "" {
		enc := json.NewEncoder(os.Stdout)
		for _, r := range records {
			enc.Encode(r)
		}
		return
	}
	// Spooled payloads go first, oldest first — the ingest sees the
	// minutes in order.
	drained := p.replaySpool()
	if len(records) == 0 {
		return
	}
	body, err := json.Marshal(Payload{
		Agent:   "anycast-agent/" + Version,
		SentAt:  time.Now().UTC().Format(time.RFC3339),
		Records: records,
	})
	if err != nil {
		log.Printf("push: marshal: %v", err)
		return
	}
	// Only try the current batch once the spool is empty; otherwise it
	// would overtake older minutes.
	if drained {
		for attempt := 1; attempt <= 2; attempt++ {
			if err = p.post(body); err == nil {
				return
			}
			time.Sleep(retryDelay)
		}
	}
	if p.spoolDir == "" {
		p.Failed += uint64(len(records))
		log.Printf("push: dropped after 2 attempts (%d records): %v", len(records), err)
		return
	}
	if serr := p.writeSpool(body); serr != nil {
		p.Failed += uint64(len(records))
		log.Printf("push: spool write failed (%d records lost): %v", len(records), serr)
		return
	}
	if drained { // replaySpool already reported the other case
		log.Printf("push: spooled %d records after 2 attempts (%d payloads waiting): %v",
			len(records), p.Spooled, err)
	}
}

// replaySpool re-sends spooled payloads oldest first, at most
// maxReplayPerTick per call. The stored body is posted verbatim so the
// ingest keeps seeing the original sent_at. It returns true when the
// spool is empty (or disabled) and the current batch may follow.
func (p *Pusher) replaySpool() bool {
	if p.spoolDir == "" {
		return true
	}
	names, err := p.spoolFiles()
	if err != nil {
		log.Printf("push: spool list: %v", err)
		return false
	}
	p.Spooled = len(names)
	if len(names) == 0 {
		return true
	}
	sent := 0
	for _, name := range names {
		if sent >= maxReplayPerTick {
			break
		}
		path := filepath.Join(p.spoolDir, name)
		body, err := os.ReadFile(path)
		if err != nil {
			// An unreadable file must not block the queue forever.
			log.Printf("push: spool read %s: %v", name, err)
			os.Remove(path)
			p.Spooled--
			continue
		}
		if err := p.post(body); err != nil {
			log.Printf("push: replay stopped at %s, %d payloads waiting: %v", name, p.Spooled, err)
			return false
		}
		os.Remove(path)
		p.Spooled--
		sent++
	}
	log.Printf("push: replayed %d spooled payloads (%d waiting)", sent, p.Spooled)
	return p.Spooled == 0
}

// writeSpool stores one payload as <seq>-<unix-ms>.json (tmp + rename, so
// a crash never leaves a half-written file behind) and enforces the cap.
// The sequence number — not the clock — orders the spool: it only ever
// counts up, so a name is never reused after a replay removed a file, and
// two payloads written in the same millisecond keep their order. The
// millisecond is there for the operator reading the directory.
func (p *Pusher) writeSpool(body []byte) error {
	path := filepath.Join(p.spoolDir, fmt.Sprintf("%020d-%d.json", p.seq, now().UnixMilli()))
	p.seq++
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, body, 0o640); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	p.Spooled++
	p.enforceSpoolCap()
	return nil
}

// enforceSpoolCap drops the oldest payloads until the spool fits into
// spool_max_mb. The newest payload is always kept, even if it alone
// exceeds the cap.
func (p *Pusher) enforceSpoolCap() {
	entries, err := os.ReadDir(p.spoolDir)
	if err != nil {
		return
	}
	type spooled struct {
		name string
		size int64
	}
	var files []spooled
	var total int64
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		files = append(files, spooled{e.Name(), info.Size()})
		total += info.Size()
	}
	if total <= p.spoolMax {
		return
	}
	sort.Slice(files, func(i, j int) bool { return files[i].name < files[j].name })
	dropped, lost := 0, uint64(0)
	for i := 0; i < len(files)-1 && total > p.spoolMax; i++ {
		path := filepath.Join(p.spoolDir, files[i].name)
		if body, err := os.ReadFile(path); err == nil {
			lost += recordCount(body)
		}
		if err := os.Remove(path); err != nil {
			continue
		}
		total -= files[i].size
		dropped++
		p.Spooled--
	}
	if dropped > 0 {
		p.Failed += lost
		log.Printf("push: spool over %d MB, dropped %d oldest payloads (%d records)",
			p.spoolMax/(1024*1024), dropped, lost)
	}
}

// spoolFiles returns the spooled payload names, oldest first. The names
// start with a zero-padded sequence number, so lexical order is write
// order.
func (p *Pusher) spoolFiles() ([]string, error) {
	entries, err := os.ReadDir(p.spoolDir)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".json") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names, nil
}

// seqOf reads the sequence number a spool file name starts with; an
// unparsable name counts as 0 and simply does not raise the counter.
func seqOf(name string) uint64 {
	digits := strings.TrimSuffix(name, ".json")
	if i := strings.IndexByte(digits, '-'); i >= 0 {
		digits = digits[:i]
	}
	n, err := strconv.ParseUint(digits, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// recordCount counts the records of a stored payload without decoding them.
func recordCount(body []byte) uint64 {
	var p struct {
		Records []json.RawMessage `json:"records"`
	}
	if err := json.Unmarshal(body, &p); err != nil {
		return 0
	}
	return uint64(len(p.Records))
}

func (p *Pusher) post(body []byte) error {
	req, err := http.NewRequest(http.MethodPost, p.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if p.token != "" {
		req.Header.Set("Authorization", "Bearer "+p.token)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("http %d", resp.StatusCode)
	}
	return nil
}
