package signal

import (
	"context"
	"encoding/base64"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestAttachmentReaderFetchesFromTheSidecar(t *testing.T) {
	c, daemon := newFakeDaemon(t)
	serve(t, c)
	s, _ := newTestStream(t)
	s.SetClient(c)

	open := s.attachmentReader("att-1", groupID, true)
	type result struct {
		body []byte
		err  error
	}
	done := make(chan result, 1)
	go func() {
		r, err := open(context.Background())
		if err != nil {
			done <- result{err: err}
			return
		}
		defer func() { _ = r.Close() }()
		body, err := io.ReadAll(r)
		done <- result{body: body, err: err}
	}()

	req := daemon.nextRequest()
	if req.Method != "getAttachment" {
		t.Fatalf("method = %q, want getAttachment", req.Method)
	}
	encoded := base64.StdEncoding.EncodeToString([]byte("file contents"))
	daemon.write(`{"jsonrpc":"2.0","id":"` + req.ID + `","result":{"data":"` + encoded + `"}}`)

	got := <-done
	if got.err != nil {
		t.Fatalf("open: %v", got.err)
	}
	if string(got.body) != "file contents" {
		t.Fatalf("body = %q", got.body)
	}
}

// signal-cli writes every received file to disk and never cleans up. Left
// alone, a bridged conversation fills the volume that also holds the
// account's identity keys — and a full volume is an outage needing a human.
func TestDiscardAttachment(t *testing.T) {
	dir := t.TempDir()
	s, _ := newTestStream(t)
	s.SetAttachmentsDir(dir)

	path := filepath.Join(dir, "att-1")
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := s.DiscardAttachment("att-1"); err != nil {
		t.Fatalf("DiscardAttachment(): %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("file still exists: %v", err)
	}

	// Removing one that is already gone is the state the caller wanted.
	if err := s.DiscardAttachment("att-1"); err != nil {
		t.Fatalf("second DiscardAttachment(): %v", err)
	}
}

// The id comes from a remote party and is used as a filename, so a path
// separator in it would reach outside the directory.
func TestDiscardAttachmentRefusesToEscapeItsDirectory(t *testing.T) {
	dir := t.TempDir()
	outside := filepath.Join(dir, "keep-me")
	if err := os.WriteFile(outside, []byte("account keys"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	s, _ := newTestStream(t)
	s.SetAttachmentsDir(filepath.Join(dir, "attachments"))

	for _, id := range []string{"../keep-me", "..", ".", `sub\dir`, ""} {
		if err := s.DiscardAttachment(id); err == nil {
			t.Fatalf("DiscardAttachment(%q) error = nil, want a refusal", id)
		}
	}
	if _, err := os.Stat(outside); err != nil {
		t.Fatalf("the file outside the attachments directory was touched: %v", err)
	}
}

// Without the shared volume there is nothing to clean up, and saying so is
// better than failing every transfer.
func TestDiscardAttachmentWithoutADirectoryIsANoOp(t *testing.T) {
	s, _ := newTestStream(t)
	if err := s.DiscardAttachment("att-1"); err != nil {
		t.Fatalf("DiscardAttachment(): %v", err)
	}
}
