// Package config loads the agent configuration (a single YAML file).
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Input struct {
	// Unix socket the agent listens on as a Frame Streams server. The
	// nameserver (Knot/NSD/BIND/dnsdist/Unbound) connects as a client.
	Socket string `yaml:"socket"`
	// File mode of the socket (octal string) so the nameserver user may
	// connect. Default "0666"; tighten via a shared group + "0660".
	Mode string `yaml:"mode"`
}

type Push struct {
	// Target URL for the minute push (HTTP POST, JSON). Empty = stdout.
	URL string `yaml:"url"`
	// Flush interval in seconds (default 60).
	IntervalSeconds int `yaml:"interval_seconds"`
	TimeoutSeconds  int `yaml:"timeout_seconds"`
	// Optional bearer token (header Authorization: Bearer …).
	Token string `yaml:"token"`
}

type Config struct {
	// Overrides the node ID. Empty = the producer's dnstap identity
	// (identity/server-id the nameserver includes in the dnstap frame).
	NodeID string `yaml:"node_id"`
	// Aggregation granularity: "prefix" (v4=/24, v6=/48) or "none" (no
	// client dimension). ASN/country are derived centrally during
	// enrichment, never on the monitored server.
	Granularity string  `yaml:"granularity"`
	Inputs      []Input `yaml:"inputs"`
	Push        Push    `yaml:"push"`
	// Buffer size between socket reader and aggregator. Full = drop,
	// never block (DNS comes first).
	QueueSize int `yaml:"queue_size"`
}

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	c := &Config{}
	if err := yaml.Unmarshal(b, c); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if len(c.Inputs) == 0 {
		return nil, fmt.Errorf("%s: no inputs configured", path)
	}
	if c.Granularity == "" {
		c.Granularity = "prefix"
	}
	if c.Granularity != "prefix" && c.Granularity != "none" {
		return nil, fmt.Errorf("unknown granularity %q (prefix|none)", c.Granularity)
	}
	if c.QueueSize <= 0 {
		c.QueueSize = 8192
	}
	if c.Push.IntervalSeconds <= 0 {
		c.Push.IntervalSeconds = 60
	}
	if c.Push.TimeoutSeconds <= 0 {
		c.Push.TimeoutSeconds = 5
	}
	for i := range c.Inputs {
		if c.Inputs[i].Mode == "" {
			c.Inputs[i].Mode = "0666"
		}
	}
	return c, nil
}
