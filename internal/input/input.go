// Package input listens as a Frame Streams server on Unix sockets and
// delivers raw dnstap frames. All five dialects (Knot, NSD, BIND,
// dnsdist/PowerDNS, Unbound) connect to the socket as Frame Streams
// clients.
//
// Backpressure rule: the socket reader must NEVER block, or the stall
// propagates back into the nameserver. The drop-forwarder decouples:
// reader → drained at full speed → full queue = drop the frame.
package input

import (
	"log"
	"os"
	"strconv"
	"sync/atomic"

	dnstap "github.com/dnstap/golang-dnstap"
)

type Stats struct {
	Received atomic.Uint64
	Dropped  atomic.Uint64
}

// Run opens the socket (removing stale leftovers), sets the file mode and
// pumps frames into out. Runs until process exit; producer disconnects are
// handled by the Frame Streams library's accept loop.
func Run(socketPath, mode string, out chan<- []byte, stats *Stats) error {
	// Remove a stale socket from a previous run, or bind fails.
	if _, err := os.Stat(socketPath); err == nil {
		if err := os.Remove(socketPath); err != nil {
			return err
		}
	}
	in, err := dnstap.NewFrameStreamSockInputFromPath(socketPath)
	if err != nil {
		return err
	}
	if m, err := strconv.ParseUint(mode, 8, 32); err == nil {
		if err := os.Chmod(socketPath, os.FileMode(m)); err != nil {
			log.Printf("input %s: chmod %s: %v", socketPath, mode, err)
		}
	}

	// raw is filled by the library reader; the forwarder ALWAYS drains raw
	// immediately and only then decides about dropping.
	raw := make(chan []byte, 128)
	go func() {
		for frame := range raw {
			stats.Received.Add(1)
			select {
			case out <- frame:
			default:
				stats.Dropped.Add(1)
			}
		}
	}()
	go func() {
		in.ReadInto(raw)
		in.Wait()
		close(raw)
		log.Printf("input %s: reader stopped", socketPath)
	}()
	log.Printf("input: listening on %s (mode %s)", socketPath, mode)
	return nil
}
