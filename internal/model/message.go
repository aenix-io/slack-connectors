// Package model defines the canonical message exchanged between adapters.
// Adapters translate platform payloads into these types; the bridge core
// speaks only this vocabulary and never imports a platform SDK.
package model

import (
	"context"
	"io"
	"time"
)

// Platform identifies a chat system.
type Platform string

const (
	PlatformTeams  Platform = "teams"
	PlatformSlack  Platform = "slack"
	PlatformSignal Platform = "signal"
)

// Valid reports whether p is a platform the bridge knows how to route.
func (p Platform) Valid() bool {
	switch p {
	case PlatformTeams, PlatformSlack, PlatformSignal:
		return true
	default:
		return false
	}
}

// EventKind distinguishes what happened to a message.
type EventKind string

const (
	EventCreated EventKind = "created"
	EventEdited  EventKind = "edited"
	EventDeleted EventKind = "deleted"
)

// Author is the human who wrote the message.
type Author struct {
	ExternalID  string
	DisplayName string
	AvatarURL   string
}

// Attachment is a file carried by the message.
type Attachment struct {
	Name        string
	ContentType string
	SizeBytes   int64
	// SourceURL identifies the file on its own platform: a URL for Slack, a
	// signal-cli attachment id for Signal. It is never handed to the other
	// platform — a Slack url_private is unreadable without our token, and a
	// Signal id means nothing outside the sidecar.
	SourceURL string
	// Open reads the file's contents from the platform it came from.
	//
	// It is a function on the value rather than a lookup through some
	// registry because only the adapter that produced this attachment knows
	// how to fetch it, and the alternative — carrying the bytes in the
	// canonical message — would hold every transfer in memory from the moment
	// it is received to the moment it is delivered, whether or not anything
	// ends up wanting them.
	Open func(ctx context.Context) (io.ReadCloser, error)
}

// Mention is a person referenced in the message body.
//
// Start and Length locate the mention inside Text and are measured the way
// Signal measures them: in UTF-16 code units, over a body where the mention
// itself is a single U+FFFC placeholder. They are zero for platforms that
// carry mentions inline in the body, such as Slack.
type Mention struct {
	ExternalID  string
	DisplayName string
	Start       int
	Length      int
}

// TextStyleKind is a span style Signal expresses out of band, as a range over
// the body rather than as markup inside it.
type TextStyleKind string

const (
	StyleBold          TextStyleKind = "BOLD"
	StyleItalic        TextStyleKind = "ITALIC"
	StyleStrikethrough TextStyleKind = "STRIKETHROUGH"
	StyleMonospace     TextStyleKind = "MONOSPACE"
	StyleSpoiler       TextStyleKind = "SPOILER"
)

// TextStyle applies a style to a span of the message body.
//
// Start and Length are in UTF-16 code units, which is how Signal counts them
// and is not how Go counts anything: an emoji outside the Basic Multilingual
// Plane occupies two units and one Go rune, and Cyrillic occupies one unit and
// two bytes. Converting at the boundary is the adapter's job; everything above
// it works in these units.
type TextStyle struct {
	Style  TextStyleKind
	Start  int
	Length int
}

// Message is the canonical form crossing every adapter boundary.
type Message struct {
	Platform Platform
	ID       string
	ParentID string // empty when the message starts a thread
	// ThreadRootID names the thread this message belongs to, on its own
	// platform, and is set only by platforms that have threads.
	//
	// It is not the same question as ParentID. Slack addresses a thread by
	// its root and warns against naming a reply instead; Signal has no
	// threads at all and quotes whatever message a person tapped. So a reply
	// arriving from Signal to a message that was a reply in Slack has to
	// resolve back to that Slack thread's root, and only the Slack adapter
	// knows what it was.
	ThreadRootID string
	// EditsID names the message this one replaces, on the same platform.
	//
	// An edit needs two ids, not one: the id of the original, to find what to
	// change on the far side, and an id of its own, so that two corrections
	// to the same message are two events rather than one the idempotency gate
	// mistakes for a repeat. A delete carries only the original and leaves
	// this empty.
	EditsID     string
	PairID      string
	Author      Author
	Text        string
	Kind        EventKind
	Timestamp   time.Time
	Attachments []Attachment
	Mentions    []Mention
	// TextStyles carries formatting for platforms that keep it out of the
	// body. A platform whose body is already marked up — Teams HTML, Slack
	// mrkdwn — leaves this empty.
	TextStyles []TextStyle
	// FromBridge marks a message this service itself produced. Loop
	// prevention layer one.
	FromBridge bool
}

// IsReply reports whether the message belongs to an existing thread.
func (m Message) IsReply() bool { return m.ParentID != "" }

// Reaction is an emoji placed on a message. It travels separately from
// Message because it is not one: it has no body, no thread and no identity of
// its own on every platform — Slack names the message it sits on, and Signal
// names the message's author and send time.
type Reaction struct {
	Platform Platform
	PairID   string
	Author   Author
	// Emoji is a unicode grapheme cluster. Slack's shortcodes are translated
	// by the adapter, so the core never sees one.
	Emoji string
	// TargetID is the platform-native id of the message reacted to.
	TargetID string
	// Remove marks the withdrawal of a reaction rather than a new one.
	Remove    bool
	Timestamp time.Time
	// FromBridge marks a reaction this service itself placed. Loop
	// prevention layer one.
	FromBridge bool
}
