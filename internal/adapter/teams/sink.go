package teams

import (
	"context"
	"errors"
	"fmt"
	"html"
	"strings"
	"time"

	"github.com/aenix-io/slack-connectors/internal/bridge"
	"github.com/aenix-io/slack-connectors/internal/config"
	"github.com/aenix-io/slack-connectors/internal/format/slackmrkdwn"
	"github.com/aenix-io/slack-connectors/internal/model"
)

// fallbackAuthorName is shown when the source message carries no author name.
const fallbackAuthorName = "Slack"

// channelPoster is what a Sink needs from the transport: post HTML into a
// channel, optionally under a thread root, and return the created message id.
// Implemented by BotClient — the Bot Framework connector is the only route an
// application has for posting regular channel messages.
type channelPoster interface {
	PostChannelMessage(ctx context.Context, teamID, channelID, replyToID, content string) (string, error)
}

// Sink posts canonical messages into Teams channels.
//
// The bot cannot impersonate people, so the author is written as the first
// line of the message body rather than as the sender. cfg is held because
// bridge.Sink passes only a channel id while the pair also names the team.
type Sink struct {
	client channelPoster
	cfg    config.Config
}

// NewSink builds a Sink. cfg must contain the pair serving every channel the
// bridge may deliver to.
func NewSink(client channelPoster, cfg config.Config) *Sink {
	return &Sink{client: client, cfg: cfg}
}

// Deliver posts msg into channelID. When threadTarget is non-empty the message
// is posted as a reply under that message.
func (s *Sink) Deliver(ctx context.Context, msg model.Message, channelID, threadTarget string) (string, error) {
	pair, ok := s.cfg.PairFor(model.PlatformTeams, channelID)
	if !ok {
		return "", fmt.Errorf("no configured pair for teams channel %q: %w",
			channelID, bridge.ErrNotSent)
	}

	body := renderBody(msg)
	var err error
	for attempt := 0; ; attempt++ {
		var id string
		id, err = s.client.PostChannelMessage(
			ctx, pair.Teams.TeamID, channelID, threadTarget, body)
		if err == nil {
			return id, nil
		}

		// Only a throttle is repeated. Every other refusal is permanent, and
		// an ambiguous failure must never be repeated at all: the post may
		// have landed, and a second attempt would duplicate it.
		var throttled *ThrottledError
		if !errors.As(err, &throttled) || attempt >= maxPostAttempts-1 {
			break
		}
		if waitErr := wait(ctx, throttled.RetryAfter); waitErr != nil {
			break
		}
	}
	return "", fmt.Errorf("post message to %s: %w", channelID, err)
}

// maxPostAttempts bounds how many times a throttled post is repeated. The
// delivery holds its pair's lock throughout, so retrying forever would stall
// the channel; whatever replays undelivered messages picks up from here.
const maxPostAttempts = 3

// wait sleeps for d, or returns early if ctx ends first.
func wait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// renderBody builds the HTML Teams shows: the author's name, then the message.
// The name is escaped because it comes from a remote profile and would
// otherwise be able to inject markup into the body.
func renderBody(msg model.Message) string {
	name := msg.Author.DisplayName
	if strings.TrimSpace(name) == "" {
		name = fallbackAuthorName
	}
	// <strong>, not <b>: Teams documents only <strong> in its supported set.
	return "<strong>" + html.EscapeString(name) + "</strong><br>" + slackmrkdwn.ToHTML(msg.Text)
}

// compile-time interface check against the bridge contract
var _ bridge.Sink = (*Sink)(nil)
