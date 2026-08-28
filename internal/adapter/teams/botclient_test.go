package teams

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type capturedActivity struct {
	path string
	body map[string]any
}

func newConnectorServer(t *testing.T, status int, response string) (*httptest.Server, *capturedActivity) {
	t.Helper()
	got := &capturedActivity{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.path = r.URL.Path
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &got.body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, response)
	}))
	t.Cleanup(srv.Close)
	return srv, got
}

func TestBotReplyTargetsTheThreadConversation(t *testing.T) {
	srv, got := newConnectorServer(t, http.StatusCreated, `{"id":"1753600000001"}`)
	c := NewBotClient(srv.URL, srv.Client())

	id, err := c.PostChannelMessage(context.Background(),
		"t1", "19:chan@thread.tacv2", "1753500000000", "<b>Ivan</b><br>hi")
	if err != nil {
		t.Fatalf("PostChannelMessage(): %v", err)
	}
	if id != "1753600000001" {
		t.Fatalf("id = %q", id)
	}
	// The thread is addressed by the conversation id itself.
	want := "/v3/conversations/19:chan@thread.tacv2;messageid=1753500000000/activities"
	if got.path != want {
		t.Fatalf("path = %q, want %q", got.path, want)
	}
	if got.body["textFormat"] != "xml" || got.body["type"] != "message" {
		t.Fatalf("activity = %+v, want a message with textFormat xml", got.body)
	}
	if !strings.Contains(got.body["text"].(string), "<b>Ivan</b>") {
		t.Fatalf("text = %q, want the HTML body", got.body["text"])
	}
}

func TestBotNewMessageCreatesAConversationAndReturnsTheActivityID(t *testing.T) {
	srv, got := newConnectorServer(t, http.StatusCreated,
		`{"id":"19:chan@thread.tacv2;messageid=1753600000002","activityId":"1753600000002"}`)
	c := NewBotClient(srv.URL, srv.Client())

	id, err := c.PostChannelMessage(context.Background(),
		"t1", "19:chan@thread.tacv2", "", "<p>root</p>")
	if err != nil {
		t.Fatalf("PostChannelMessage(): %v", err)
	}
	// The activity id is what Graph will call this message in notifications;
	// the mapping store must receive it, not the composite conversation id.
	if id != "1753600000002" {
		t.Fatalf("id = %q, want the bare activity id", id)
	}
	if got.path != "/v3/conversations" {
		t.Fatalf("path = %q", got.path)
	}
	channelData := got.body["channelData"].(map[string]any)
	channel := channelData["channel"].(map[string]any)
	if channel["id"] != "19:chan@thread.tacv2" {
		t.Fatalf("channelData.channel.id = %v", channel["id"])
	}
}

func TestBotThrottleSurfacesRetryAfter(t *testing.T) {
	srv, _ := newConnectorServer(t, http.StatusTooManyRequests, `{}`)
	srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusTooManyRequests)
	})
	c := NewBotClient(srv.URL, srv.Client())

	_, err := c.PostChannelMessage(context.Background(), "t1", "19:c", "", "x")
	var throttled *ThrottledError
	if !errors.As(err, &throttled) {
		t.Fatalf("error = %v, want ThrottledError", err)
	}
	if throttled.RetryAfter != 7*time.Second {
		t.Fatalf("RetryAfter = %v, want 7s", throttled.RetryAfter)
	}
}

func TestBotErrorSurfacesTheConnectorMessage(t *testing.T) {
	srv, _ := newConnectorServer(t, http.StatusForbidden,
		`{"error":{"code":"BotNotInConversationRoster","message":"The bot is not part of the conversation roster."}}`)
	c := NewBotClient(srv.URL, srv.Client())

	_, err := c.PostChannelMessage(context.Background(), "t1", "19:c", "", "x")
	if err == nil || !strings.Contains(err.Error(), "BotNotInConversationRoster") {
		t.Fatalf("error = %v, want the connector code surfaced — it is the "+
			"'app not installed in this team' diagnostic", err)
	}
}
