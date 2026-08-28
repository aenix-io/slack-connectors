package teams

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"strings"
)

// BotClient posts channel messages through the Bot Framework connector.
//
// This is the only route an application has to a regular channel message:
// Graph's send is delegated-or-import-mode only, and resource-specific
// consent offers no application send permission at all. The bot travels in
// the same Teams app package, so installing the app makes the bot a member
// of the team's standard channels with nothing extra to consent to.
type BotClient struct {
	serviceURL string
	http       *http.Client
}

// NewBotClient builds a connector client. serviceURL is the Teams connector
// endpoint (regional); httpClient must already carry Bot Framework
// authentication — tokens come from the botframework.com authority, not the
// customer tenant.
func NewBotClient(serviceURL string, httpClient *http.Client) *BotClient {
	return &BotClient{serviceURL: strings.TrimSuffix(serviceURL, "/"), http: httpClient}
}

// activity is the subset of a Bot Framework activity the bridge sends.
// textFormat "xml" is the connector's name for the HTML subset Teams renders.
type activity struct {
	Type       string `json:"type"`
	TextFormat string `json:"textFormat"`
	Text       string `json:"text"`
}

// PostChannelMessage posts HTML into a channel and returns the id Teams
// assigned — the same identifier space Graph reports, so the mapping store
// can match later Graph notifications about the thread. When replyToID is
// non-empty the message lands under that root. The team id is unused (the
// connector addresses channels directly) and kept for signature parity with
// the read side.
func (c *BotClient) PostChannelMessage(
	ctx context.Context, _, channelID, replyToID, content string,
) (string, error) {
	act := activity{Type: "message", TextFormat: "xml", Text: content}

	if replyToID != "" {
		conversation := channelID + ";messageid=" + replyToID
		url := c.serviceURL + "/v3/conversations/" + neturl.PathEscape(conversation) + "/activities"
		var created struct {
			ID string `json:"id"`
		}
		if err := c.post(ctx, url, act, &created); err != nil {
			return "", err
		}
		return created.ID, nil
	}

	url := c.serviceURL + "/v3/conversations"
	payload := map[string]any{
		"isGroup":     true,
		"channelData": map[string]any{"channel": map[string]string{"id": channelID}},
		"activity":    act,
	}
	var created struct {
		ActivityID string `json:"activityId"`
	}
	if err := c.post(ctx, url, payload, &created); err != nil {
		return "", err
	}
	return created.ActivityID, nil
}

func (c *BotClient) post(ctx context.Context, url string, body, out any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("encode activity: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("build connector request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("call connector: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	payload, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("read connector response: %w", err)
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return &ThrottledError{RetryAfter: retryAfter(resp.Header.Get("Retry-After"))}
	}
	if resp.StatusCode >= 300 {
		var ge graphError
		if json.Unmarshal(payload, &ge) == nil && ge.Error.Message != "" {
			return fmt.Errorf("connector %s: %s: %s", resp.Status, ge.Error.Code, ge.Error.Message)
		}
		return fmt.Errorf("connector %s", resp.Status)
	}
	if err := json.Unmarshal(payload, out); err != nil {
		return fmt.Errorf("decode connector response: %w", err)
	}
	return nil
}
