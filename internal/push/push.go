// Package push delivers the minute sums: HTTP POST (JSON) to an ingest
// endpoint, or JSON lines on stdout (empty url; debugging/zoo). Failed
// pushes are retried once, then dropped and counted — the agent does not
// buffer without bound (the backpressure rule applies here too).
package push

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/portalix/anycast-agent/internal/agg"
)

// Version is stamped by the release build (goreleaser, -X); source builds
// identify themselves as dev.
var Version = "0.0.0-dev"

type Payload struct {
	Agent   string       `json:"agent"`
	SentAt  string       `json:"sent_at"`
	Records []agg.Record `json:"records"`
}

type Pusher struct {
	url    string
	token  string
	client *http.Client
	Failed uint64
}

func New(url, token string, timeout time.Duration) *Pusher {
	return &Pusher{url: url, token: token, client: &http.Client{Timeout: timeout}}
}

func (p *Pusher) Push(records []agg.Record) {
	if len(records) == 0 {
		return
	}
	if p.url == "" {
		enc := json.NewEncoder(os.Stdout)
		for _, r := range records {
			enc.Encode(r)
		}
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
	for attempt := 1; attempt <= 2; attempt++ {
		if err = p.post(body); err == nil {
			return
		}
		time.Sleep(time.Second)
	}
	p.Failed += uint64(len(records))
	log.Printf("push: dropped after 2 attempts (%d records): %v", len(records), err)
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
