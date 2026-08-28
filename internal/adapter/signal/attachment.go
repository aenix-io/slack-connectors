package signal

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// maxAttachmentBytes bounds a transfer in either direction.
//
// The ceiling is a memory decision as much as a product one: JSON-RPC carries
// a file as one base64 string, so nothing streams and the encoded form is a
// third larger than the file. Two concurrent 50 MB transfers are already
// 130 MB of live heap in a pod sized for a bridge.
const maxAttachmentBytes = 50 << 20

// attachmentReader fetches a received attachment from the sidecar.
//
// signal-cli has already written the file to its data directory by the time
// we hear about it, so this reads a local copy over the socket rather than
// from the Signal servers.
func (s *Stream) attachmentReader(id, conversation string, isGroup bool) func(context.Context) (io.ReadCloser, error) {
	return func(ctx context.Context) (io.ReadCloser, error) {
		if s.client == nil {
			return nil, fmt.Errorf("no signal connection to fetch attachment %q", id)
		}
		params := map[string]any{"id": id}
		if isGroup {
			params["groupId"] = conversation
		} else {
			params["recipient"] = conversation
		}

		var result struct {
			Data string `json:"data"`
		}
		if err := s.client.Call(ctx, "getAttachment", params, &result); err != nil {
			return nil, fmt.Errorf("fetch attachment %q: %w", id, err)
		}
		raw, err := base64.StdEncoding.DecodeString(result.Data)
		if err != nil {
			return nil, fmt.Errorf("decode attachment %q: %w", id, err)
		}
		return io.NopCloser(bytes.NewReader(raw)), nil
	}
}

// DiscardAttachment removes a received file from the sidecar's data
// directory.
//
// signal-cli writes every attachment to disk and never cleans up: a bridged
// conversation would fill the volume that also holds the account's identity
// keys, and a full volume is an outage that needs a human. The bridge deletes
// what it has already transferred.
//
// The id is used as a filename, so it is checked rather than trusted: it
// arrives from a remote party, and a path separator in it would reach outside
// the directory.
func (s *Stream) DiscardAttachment(id string) error {
	if s.attachmentsDir == "" {
		return nil
	}
	if id == "" || strings.ContainsAny(id, `/\`) || id == "." || id == ".." {
		return fmt.Errorf("refusing to delete attachment with suspicious id %q", id)
	}
	path := filepath.Join(s.attachmentsDir, id)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove attachment %q: %w", path, err)
	}
	return nil
}

// SetAttachmentsDir tells the stream where the sidecar writes received files,
// so it can delete them once they have been transferred. An empty path
// disables cleanup, which is what a deployment without the shared volume gets.
func (s *Stream) SetAttachmentsDir(dir string) { s.attachmentsDir = dir }

// SetClient hands the stream the connection it fetches attachments over.
func (s *Stream) SetClient(c *Client) { s.client = c }
