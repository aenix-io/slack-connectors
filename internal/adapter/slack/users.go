package slack

import (
	"context"
	"sync"

	goslack "github.com/slack-go/slack"
)

// UserDirectory resolves a Slack user id to a name fit to show a human.
//
// It returns a name rather than an error: an unresolvable author is worth a
// message attributed to a fallback, never a message not delivered.
type UserDirectory interface {
	DisplayName(ctx context.Context, userID string) string
}

// APIUserDirectory resolves names through users.info, caching what it learns.
//
// Display names change rarely and the alternative is one API call per
// message, which would spend the workspace's rate limit on data that hardly
// moves. The cache is never invalidated: a renamed user keeps their old name
// until the process restarts, which is the right trade for a bridge whose
// pods are restarted on every deploy.
type APIUserDirectory struct {
	client *goslack.Client

	mu    sync.RWMutex
	names map[string]string
}

// NewUserDirectory builds a directory around an existing Slack client. It
// needs the users:read scope.
func NewUserDirectory(client *goslack.Client) *APIUserDirectory {
	return &APIUserDirectory{client: client, names: make(map[string]string)}
}

// DisplayName returns the best available name for a user, or an empty string
// when there is none to be had. A failed lookup is not cached, so a transient
// outage does not leave a user anonymous for the life of the process.
func (d *APIUserDirectory) DisplayName(ctx context.Context, userID string) string {
	if userID == "" {
		return ""
	}

	d.mu.RLock()
	name, ok := d.names[userID]
	d.mu.RUnlock()
	if ok {
		return name
	}

	user, err := d.client.GetUserInfoContext(ctx, userID)
	if err != nil {
		return ""
	}
	name = bestName(user)
	if name == "" {
		return ""
	}

	d.mu.Lock()
	d.names[userID] = name
	d.mu.Unlock()
	return name
}

// bestName picks what Slack itself shows: the chosen display name, else the
// real name, else the handle.
func bestName(u *goslack.User) string {
	switch {
	case u.Profile.DisplayName != "":
		return u.Profile.DisplayName
	case u.Profile.RealName != "":
		return u.Profile.RealName
	case u.RealName != "":
		return u.RealName
	default:
		return u.Name
	}
}

// compile-time check that the API-backed directory satisfies the contract the
// webhook depends on
var _ UserDirectory = (*APIUserDirectory)(nil)
