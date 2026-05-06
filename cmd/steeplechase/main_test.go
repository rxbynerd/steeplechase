package main

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/rxbynerd/steeplechase/internal/metrics"
	"github.com/rxbynerd/steeplechase/internal/sink"
)

func TestParseStdoutFormat(t *testing.T) {
	tests := []struct {
		name      string
		mode      string
		timeout   time.Duration
		maxItems  int
		wantErr   bool
		errSubstr string
	}{
		{name: "line is the default and valid", mode: "line", timeout: 5 * time.Minute, maxItems: 10000},
		{name: "grouped is valid", mode: "grouped", timeout: 5 * time.Minute, maxItems: 10000},
		{name: "tree is valid", mode: "tree", timeout: 5 * time.Minute, maxItems: 10000},
		{name: "unknown mode is rejected loudly", mode: "fancy", timeout: time.Minute, maxItems: 100, wantErr: true, errSubstr: "invalid mode"},
		{name: "empty mode is rejected", mode: "", timeout: time.Minute, maxItems: 100, wantErr: true, errSubstr: "invalid mode"},
		{name: "zero timeout in grouped mode rejected", mode: "grouped", timeout: 0, maxItems: 100, wantErr: true, errSubstr: "stdout-run-timeout"},
		{name: "negative max items in tree mode rejected", mode: "tree", timeout: time.Minute, maxItems: -1, wantErr: true, errSubstr: "stdout-run-max-items"},
		// In line mode, values for the other two flags should not matter.
		{name: "line mode tolerates zero timeout", mode: "line", timeout: 0, maxItems: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseStdoutFormat(tt.mode, tt.timeout, tt.maxItems)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tt.errSubstr != "" && !strings.Contains(err.Error(), tt.errSubstr) {
					t.Errorf("error %q missing %q", err.Error(), tt.errSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestBuildPipeline_LineModeUsesPlainStdout(t *testing.T) {
	rec := metrics.NewRecorder(prometheus.NewRegistry())
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))

	cfg, err := parseStdoutFormat("line", 5*time.Minute, 10000)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	root, names, err := buildPipeline(nil, rec, logger, cfg)
	if err != nil {
		t.Fatalf("buildPipeline: %v", err)
	}
	defer root.Shutdown(context.Background())

	if len(names) != 1 || names[0] != "stdout" {
		t.Fatalf("expected single stdout leaf, got %v", names)
	}

	// Single sink should be the metered wrapper directly.
	metered, ok := root.(*sink.MeteredSink)
	if !ok {
		t.Fatalf("expected *MeteredSink, got %T", root)
	}
	// Inner should be the bare *StdoutSink, not a *RunBlockSink, in line mode.
	if _, isRB := metered.Inner().(*sink.RunBlockSink); isRB {
		t.Fatalf("line mode must not wrap stdout in RunBlockSink")
	}
	if _, isStdout := metered.Inner().(*sink.StdoutSink); !isStdout {
		t.Fatalf("expected inner *StdoutSink in line mode, got %T", metered.Inner())
	}
}

func TestBuildPipeline_GroupedModeWrapsStdout(t *testing.T) {
	rec := metrics.NewRecorder(prometheus.NewRegistry())
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))

	cfg, err := parseStdoutFormat("grouped", time.Minute, 100)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	root, _, err := buildPipeline(nil, rec, logger, cfg)
	if err != nil {
		t.Fatalf("buildPipeline: %v", err)
	}
	defer root.Shutdown(context.Background())

	metered, ok := root.(*sink.MeteredSink)
	if !ok {
		t.Fatalf("expected *MeteredSink, got %T", root)
	}
	if _, isRB := metered.Inner().(*sink.RunBlockSink); !isRB {
		t.Fatalf("grouped mode must wrap stdout in RunBlockSink, got %T", metered.Inner())
	}
}

func TestBuildPipeline_TreeModeWrapsStdout(t *testing.T) {
	rec := metrics.NewRecorder(prometheus.NewRegistry())
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))

	cfg, err := parseStdoutFormat("tree", time.Minute, 100)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	root, _, err := buildPipeline(nil, rec, logger, cfg)
	if err != nil {
		t.Fatalf("buildPipeline: %v", err)
	}
	defer root.Shutdown(context.Background())

	metered, ok := root.(*sink.MeteredSink)
	if !ok {
		t.Fatalf("expected *MeteredSink, got %T", root)
	}
	if _, isRB := metered.Inner().(*sink.RunBlockSink); !isRB {
		t.Fatalf("tree mode must wrap stdout in RunBlockSink, got %T", metered.Inner())
	}
}

func TestBuildPipeline_GroupedModeOnlyWrapsStdoutLeaves(t *testing.T) {
	rec := metrics.NewRecorder(prometheus.NewRegistry())
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))

	cfg, err := parseStdoutFormat("grouped", time.Minute, 100)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// Mix stdout with an OTLP HTTP sink. Only the stdout one should get
	// the RunBlockSink wrapper; OTLP forwarders must remain unwrapped so
	// downstream backends still receive raw OTLP.
	root, names, err := buildPipeline([]string{"stdout", "otlp+http://example.invalid:4318"}, rec, logger, cfg)
	if err != nil {
		t.Fatalf("buildPipeline: %v", err)
	}
	defer root.Shutdown(context.Background())

	if len(names) != 2 {
		t.Fatalf("expected 2 leaves, got %v", names)
	}

	fanout, ok := root.(*sink.FanoutSink)
	if !ok {
		t.Fatalf("expected *FanoutSink at root, got %T", root)
	}
	leaves := fanout.Sinks()
	if len(leaves) != 2 {
		t.Fatalf("expected 2 fanout leaves, got %d", len(leaves))
	}

	wrappedCount := 0
	for _, leaf := range leaves {
		metered, ok := leaf.(*sink.MeteredSink)
		if !ok {
			t.Fatalf("expected each leaf to be *MeteredSink, got %T", leaf)
		}
		if _, isRB := metered.Inner().(*sink.RunBlockSink); isRB {
			wrappedCount++
		}
	}
	if wrappedCount != 1 {
		t.Errorf("expected exactly one RunBlockSink-wrapped leaf, got %d", wrappedCount)
	}
}
