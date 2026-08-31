// anycast-agent v0 — dnstap reader + per-minute aggregation + push.
//
// Listens as a Frame Streams server on Unix sockets (Knot, NSD, BIND,
// dnsdist/PowerDNS and Unbound connect as clients), aggregates metadata
// per minute and pushes the sums. Query names and full client IPs never
// leave the server.
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/portalix/anycast-agent/internal/agg"
	"github.com/portalix/anycast-agent/internal/config"
	"github.com/portalix/anycast-agent/internal/input"
	"github.com/portalix/anycast-agent/internal/push"
)

func main() {
	cfgPath := flag.String("config", "/etc/anycast-agent/config.yaml", "path to the YAML config")
	flag.Parse()
	log.SetPrefix("anycast-agent ")

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	frames := make(chan []byte, cfg.QueueSize)
	stats := &input.Stats{}
	for _, in := range cfg.Inputs {
		if err := input.Run(in.Socket, in.Mode, frames, stats); err != nil {
			log.Fatalf("input %s: %v", in.Socket, err)
		}
	}

	a := agg.New(cfg.NodeID, cfg.Granularity == "prefix")
	go func() {
		for f := range frames {
			a.Consume(f)
		}
	}()

	p := push.New(cfg.Push.URL, cfg.Push.Token,
		time.Duration(cfg.Push.TimeoutSeconds)*time.Second)

	interval := time.Duration(cfg.Push.IntervalSeconds) * time.Second
	ticker := time.NewTicker(interval)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	log.Printf("started: %d inputs, granularity=%s, push=%s",
		len(cfg.Inputs), cfg.Granularity, orStdout(cfg.Push.URL))

	for {
		select {
		case <-ticker.C:
			// Deliver everything up to the start of the current minute;
			// 5 s of grace for producer timestamps just before the edge.
			now := time.Now().Unix()
			cutoff := now - now%60
			if now%60 < 5 {
				cutoff -= 60
			}
			p.Push(a.FlushBefore(cutoff))
			log.Printf("stats: received=%d dropped=%d push_failed=%d",
				stats.Received.Load(), stats.Dropped.Load(), p.Failed)
		case s := <-sig:
			log.Printf("%v: flushing and shutting down", s)
			p.Push(a.FlushBefore(1 << 62))
			return
		}
	}
}

func orStdout(url string) string {
	if url == "" {
		return "stdout"
	}
	return url
}
