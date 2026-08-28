// Package config loads and validates the channel-pair configuration.
package config

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/aenix-io/slack-connectors/internal/model"
)

// SlackEndpoint is one side of a pair living in Slack.
type SlackEndpoint struct {
	ChannelID string `yaml:"channelId"`
}

// TeamsEndpoint is one side of a pair living in Microsoft Teams. The team id
// is carried alongside the channel id because Graph addresses a channel only
// through its team.
type TeamsEndpoint struct {
	TeamID    string `yaml:"teamId"`
	ChannelID string `yaml:"channelId"`
}

// SignalEndpoint is one side of a pair living in Signal: either a group or a
// single recipient, never both. A recipient is a phone number in
// international format or a service id (ACI).
type SignalEndpoint struct {
	GroupID   string `yaml:"groupId"`
	Recipient string `yaml:"recipient"`
}

// conversationID is the value the rest of the bridge uses to address this
// endpoint. Signal has no channel ids, so the group id stands in for one, and
// a 1:1 conversation is addressed by its recipient.
func (s SignalEndpoint) conversationID() string {
	if s.GroupID != "" {
		return s.GroupID
	}
	return s.Recipient
}

// Pair binds exactly two conversations on two platforms.
//
// Endpoints are pointers so that "absent" and "present but empty" are
// different states: a pair naming three endpoints, or one, is a configuration
// error rather than something the router has to guess about at delivery time.
type Pair struct {
	ID     string          `yaml:"id"`
	Slack  *SlackEndpoint  `yaml:"slack"`
	Teams  *TeamsEndpoint  `yaml:"teams"`
	Signal *SignalEndpoint `yaml:"signal"`
}

// UserMapping links one person's identity on both platforms.
//
// It is written by hand rather than discovered. The Teams bridge could match
// on email through a directory; Signal exposes none — a person is a phone
// number, a service id and a profile name, none of which a Slack workspace
// knows about. Guessing at a match would notify the wrong person at a
// customer, which is worse than not translating the mention at all.
type UserMapping struct {
	PairID      string `yaml:"pairId"`
	SlackUserID string `yaml:"slackUserId"`
	SignalUUID  string `yaml:"signalUuid"`
	DisplayName string `yaml:"displayName"`
}

// Config is the whole service configuration.
type Config struct {
	Pairs []Pair        `yaml:"pairs"`
	Users []UserMapping `yaml:"users"`
}

// SignalUserFor resolves a Slack user id to the service id of the same person
// in a pair's Signal conversation.
func (c Config) SignalUserFor(pairID, slackUserID string) (string, bool) {
	if slackUserID == "" {
		return "", false
	}
	for _, u := range c.Users {
		if u.PairID == pairID && u.SlackUserID == slackUserID && u.SignalUUID != "" {
			return u.SignalUUID, true
		}
	}
	return "", false
}

// SlackUserFor resolves a Signal service id to the same person's Slack user
// id in a pair.
func (c Config) SlackUserFor(pairID, signalUUID string) (string, bool) {
	if signalUUID == "" {
		return "", false
	}
	for _, u := range c.Users {
		if u.PairID == pairID && u.SignalUUID == signalUUID && u.SlackUserID != "" {
			return u.SlackUserID, true
		}
	}
	return "", false
}

// pairFields is every key a pair may carry, nested and legacy alike. yaml.v3
// applies KnownFields only to the default decoder path, and Pair takes over
// that path to accept the legacy layout — so the check is done here instead of
// being quietly lost.
var pairFields = map[string]struct{}{
	"id": {}, "slack": {}, "teams": {}, "signal": {},
	"teamsTeamId": {}, "teamsChannelId": {}, "slackChannelId": {},
}

// endpointFields is the same check one level down. Taking over UnmarshalYAML
// disables KnownFields for everything Pair contains, not just for Pair, so a
// typo inside an endpoint block would otherwise be accepted and then read as
// an empty id — a pair pointing at no channel at all.
var endpointFields = map[string]map[string]struct{}{
	"slack":  {"channelId": {}},
	"teams":  {"teamId": {}, "channelId": {}},
	"signal": {"groupId": {}, "recipient": {}},
}

// rejectUnknownFields reports the first key in a mapping node that is not in
// allowed, naming the line so an operator can find it.
func rejectUnknownFields(node *yaml.Node, allowed map[string]struct{}, where string) error {
	if node.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i < len(node.Content); i += 2 {
		key := node.Content[i].Value
		if _, known := allowed[key]; !known {
			return fmt.Errorf("line %d: unknown field %q in %s", node.Content[i].Line, key, where)
		}
	}
	return nil
}

// UnmarshalYAML accepts two layouts for a pair. The nested one names a block
// per platform ("slack:", "teams:", "signal:", each with its own ids); the
// flat one is what the Teams bridge shipped with, carrying teamsTeamId,
// teamsChannelId and slackChannelId as keys on the pair itself.
//
// The flat form is what is running in the pilot's ConfigMap. Requiring it to
// be rewritten in the same change that adds a third platform would couple a
// code rollout to a config edit on a cluster somebody else operates.
func (p *Pair) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("line %d: a pair must be a mapping", node.Line)
	}
	if err := rejectUnknownFields(node, pairFields, "pair"); err != nil {
		return err
	}
	for i := 0; i < len(node.Content); i += 2 {
		key := node.Content[i].Value
		allowed, isEndpoint := endpointFields[key]
		if !isEndpoint {
			continue
		}
		if err := rejectUnknownFields(node.Content[i+1], allowed, key); err != nil {
			return err
		}
	}

	var raw struct {
		ID     string          `yaml:"id"`
		Slack  *SlackEndpoint  `yaml:"slack"`
		Teams  *TeamsEndpoint  `yaml:"teams"`
		Signal *SignalEndpoint `yaml:"signal"`

		LegacyTeamsTeamID    string `yaml:"teamsTeamId"`
		LegacyTeamsChannelID string `yaml:"teamsChannelId"`
		LegacySlackChannelID string `yaml:"slackChannelId"`
	}
	if err := node.Decode(&raw); err != nil {
		return err
	}

	p.ID = raw.ID
	p.Slack = raw.Slack
	p.Teams = raw.Teams
	p.Signal = raw.Signal

	// A pair may use one layout or the other, not both: a pair carrying
	// slackChannelId and a slack block that disagree has no defensible
	// reading, and picking one silently would route a customer's messages
	// into whichever channel lost the coin toss.
	if raw.LegacySlackChannelID != "" {
		if p.Slack != nil {
			return fmt.Errorf("pair %q: slackChannelId and slack cannot both be set", raw.ID)
		}
		p.Slack = &SlackEndpoint{ChannelID: raw.LegacySlackChannelID}
	}
	if raw.LegacyTeamsTeamID != "" || raw.LegacyTeamsChannelID != "" {
		if p.Teams != nil {
			return fmt.Errorf("pair %q: teamsTeamId/teamsChannelId and teams cannot both be set", raw.ID)
		}
		p.Teams = &TeamsEndpoint{TeamID: raw.LegacyTeamsTeamID, ChannelID: raw.LegacyTeamsChannelID}
	}
	return nil
}

// Load parses and validates configuration from r.
func Load(r io.Reader) (Config, error) {
	var cfg Config
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil && !errors.Is(err, io.EOF) {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) validate() error {
	if len(c.Pairs) == 0 {
		return errors.New("config must declare at least one pair")
	}
	ids := make(map[string]struct{}, len(c.Pairs))
	// Conversations are unique within a platform, not across all of them: two
	// platforms are free to use the same opaque id, and mirroring one
	// conversation into two channels would double every message.
	conversations := make(map[model.Platform]map[string]struct{}, 3)
	for i, p := range c.Pairs {
		if p.ID == "" {
			return fmt.Errorf("pair %d: id is required", i)
		}
		if _, dup := ids[p.ID]; dup {
			return fmt.Errorf("duplicate pair id %q", p.ID)
		}
		ids[p.ID] = struct{}{}

		if err := p.validate(); err != nil {
			return err
		}

		for _, platform := range p.Platforms() {
			id, _ := p.ConversationID(platform)
			byID, ok := conversations[platform]
			if !ok {
				byID = make(map[string]struct{})
				conversations[platform] = byID
			}
			if _, dup := byID[id]; dup {
				return fmt.Errorf("duplicate %s conversation %q", platform, id)
			}
			byID[id] = struct{}{}
		}
	}
	return c.validateUsers(ids)
}

// validateUsers rejects a mapping that names a pair which does not exist, or
// that leaves out one of the two identities it exists to connect. Both are
// typos whose only symptom would be a mention that silently stays plain text.
func (c Config) validateUsers(pairIDs map[string]struct{}) error {
	for i, u := range c.Users {
		switch {
		case u.PairID == "":
			return fmt.Errorf("user %d: pairId is required", i)
		case u.SlackUserID == "":
			return fmt.Errorf("user %d in pair %q: slackUserId is required", i, u.PairID)
		case u.SignalUUID == "":
			return fmt.Errorf("user %d in pair %q: signalUuid is required", i, u.PairID)
		}
		if _, ok := pairIDs[u.PairID]; !ok {
			return fmt.Errorf("user %d names unknown pair %q", i, u.PairID)
		}
	}
	return nil
}

func (p Pair) validate() error {
	if p.Slack != nil && p.Slack.ChannelID == "" {
		return fmt.Errorf("pair %q: slack channelId is required", p.ID)
	}
	if p.Teams != nil {
		if p.Teams.TeamID == "" {
			return fmt.Errorf("pair %q: teams teamId is required", p.ID)
		}
		if p.Teams.ChannelID == "" {
			return fmt.Errorf("pair %q: teams channelId is required", p.ID)
		}
	}
	if p.Signal != nil {
		switch {
		case p.Signal.GroupID == "" && p.Signal.Recipient == "":
			return fmt.Errorf("pair %q: signal needs either groupId or recipient", p.ID)
		case p.Signal.GroupID != "" && p.Signal.Recipient != "":
			return fmt.Errorf("pair %q: signal groupId and recipient are mutually exclusive", p.ID)
		}
	}

	platforms := p.Platforms()
	if len(platforms) != 2 {
		names := make([]string, 0, len(platforms))
		for _, platform := range platforms {
			names = append(names, string(platform))
		}
		return fmt.Errorf("pair %q: a pair must name exactly two platforms, got %d (%s)",
			p.ID, len(platforms), strings.Join(names, ", "))
	}
	return nil
}

// Platforms returns the platforms this pair connects, in a stable order.
func (p Pair) Platforms() []model.Platform {
	var out []model.Platform
	if p.Slack != nil {
		out = append(out, model.PlatformSlack)
	}
	if p.Teams != nil {
		out = append(out, model.PlatformTeams)
	}
	if p.Signal != nil {
		out = append(out, model.PlatformSignal)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// ConversationID returns the id addressing this pair's conversation on a
// platform, or false when the pair does not reach that platform.
func (p Pair) ConversationID(platform model.Platform) (string, bool) {
	switch platform {
	case model.PlatformSlack:
		if p.Slack != nil {
			return p.Slack.ChannelID, true
		}
	case model.PlatformTeams:
		if p.Teams != nil {
			return p.Teams.ChannelID, true
		}
	case model.PlatformSignal:
		if p.Signal != nil {
			return p.Signal.conversationID(), true
		}
	}
	return "", false
}

// Counterpart returns the platform and conversation on the other side of the
// pair from source, or false when source is not part of this pair.
//
// This replaced Platform.Opposite, which answered the question without being
// asked about a pair: with two platforms it was merely redundant, and with
// three it resolved anything unrecognised to Teams — a wrong answer delivered
// with no error, into a customer's channel.
func (p Pair) Counterpart(source model.Platform) (model.Platform, string, bool) {
	platforms := p.Platforms()
	if len(platforms) != 2 {
		return "", "", false
	}
	var target model.Platform
	switch source {
	case platforms[0]:
		target = platforms[1]
	case platforms[1]:
		target = platforms[0]
	default:
		return "", "", false
	}
	id, ok := p.ConversationID(target)
	return target, id, ok
}

// PairFor finds the pair serving a conversation on a platform.
func (c Config) PairFor(platform model.Platform, conversationID string) (Pair, bool) {
	for _, p := range c.Pairs {
		if id, ok := p.ConversationID(platform); ok && id == conversationID {
			return p, true
		}
	}
	return Pair{}, false
}

// PairsFor returns the pairs that reach a platform, in configuration order.
//
// Adapters iterate this rather than Pairs: a Signal pair has no Graph
// subscription to create and no delta chain to sweep, and a loop over every
// pair would try anyway the moment one binary is handed a mixed config.
func (c Config) PairsFor(platform model.Platform) []Pair {
	var out []Pair
	for _, p := range c.Pairs {
		if _, ok := p.ConversationID(platform); ok {
			out = append(out, p)
		}
	}
	return out
}

// PairByID finds a pair by its configured id.
func (c Config) PairByID(id string) (Pair, bool) {
	for _, p := range c.Pairs {
		if p.ID == id {
			return p, true
		}
	}
	return Pair{}, false
}
