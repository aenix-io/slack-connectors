package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"testing"
	"time"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// The two containers start together, and signal-cli has an account to open
// and a native library to unpack before its socket exists. Failing on the
// first attempt turns every rollout into a CrashLoopBackOff that resolves
// itself and looks, meanwhile, exactly like a broken deployment.
func TestDialWaitsForTheSidecarSocket(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "socket")

	// The socket appears a moment after the bridge starts looking.
	go func() {
		time.Sleep(300 * time.Millisecond)
		ln, err := net.Listen("unix", socket)
		if err != nil {
			t.Errorf("listen: %v", err)
			return
		}
		go func() {
			conn, err := ln.Accept()
			if err == nil {
				_ = conn.Close()
			}
		}()
		t.Cleanup(func() { _ = ln.Close() })
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := dialWithRetry(ctx, socket, nil, testLogger())
	if err != nil {
		t.Fatalf("dialWithRetry(): %v", err)
	}
	_ = client.Close()
}

func TestDialGivesUpWhenTheContextEnds(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "never")
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	start := time.Now()
	if _, err := dialWithRetry(ctx, socket, nil, testLogger()); err == nil {
		t.Fatal("dialWithRetry() error = nil, want a failure")
	}
	// It must give up with the context rather than sitting out the whole
	// startup budget: a pod being shut down should not take two minutes.
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("took %s to give up, want it to follow the context", elapsed)
	}
}
