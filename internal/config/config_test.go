package config

import (
	"strings"
	"testing"

	"github.com/aenix-io/slack-connectors/internal/model"
)

// legacyYAML is the layout the Teams pilot's ConfigMap is written in. It is
// tested verbatim rather than paraphrased: this is the file running in a
// cluster, and a rollout that cannot parse it takes the bridge down.
const legacyYAML = `
pairs:
  - id: acme-general
    teamsTeamId: 00000000-0000-0000-0000-000000000000
    teamsChannelId: "19:xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx@thread.tacv2"
    slackChannelId: C01ABCDEF
`

const nestedYAML = `
pairs:
  - id: acme-general
    slack:
      channelId: C01ABCDEF
    teams:
      teamId: 00000000-0000-0000-0000-000000000000
      channelId: "19:xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx@thread.tacv2"
  - id: acme-signal-group
    slack:
      channelId: C02SIGNAL
    signal:
      groupId: "Ci8KIExhbXBz"
  - id: acme-signal-direct
    slack:
      channelId: C03SIGNAL
    signal:
      recipient: "+491701234567"
`

func TestLoadLegacyLayout(t *testing.T) {
	cfg, err := Load(strings.NewReader(legacyYAML))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(cfg.Pairs) != 1 {
		t.Fatalf("got %d pairs, want 1", len(cfg.Pairs))
	}
	p := cfg.Pairs[0]
	if p.Slack == nil || p.Slack.ChannelID != "C01ABCDEF" {
		t.Fatalf("Slack = %+v, want channel C01ABCDEF", p.Slack)
	}
	if p.Teams == nil || p.Teams.TeamID != "00000000-0000-0000-0000-000000000000" {
		t.Fatalf("Teams = %+v, want the pilot team id", p.Teams)
	}
	if p.Teams.ChannelID != "19:xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx@thread.tacv2" {
		t.Fatalf("Teams.ChannelID = %q", p.Teams.ChannelID)
	}
	if p.Signal != nil {
		t.Fatalf("Signal = %+v, want nil", p.Signal)
	}
}

func TestLoadNestedLayout(t *testing.T) {
	cfg, err := Load(strings.NewReader(nestedYAML))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(cfg.Pairs) != 3 {
		t.Fatalf("got %d pairs, want 3", len(cfg.Pairs))
	}

	group, ok := cfg.PairByID("acme-signal-group")
	if !ok {
		t.Fatal("PairByID(acme-signal-group) ok = false")
	}
	if id, ok := group.ConversationID(model.PlatformSignal); !ok || id != "Ci8KIExhbXBz" {
		t.Fatalf("signal conversation = %q, %v; want the group id", id, ok)
	}

	direct, ok := cfg.PairByID("acme-signal-direct")
	if !ok {
		t.Fatal("PairByID(acme-signal-direct) ok = false")
	}
	if id, ok := direct.ConversationID(model.PlatformSignal); !ok || id != "+491701234567" {
		t.Fatalf("signal conversation = %q, %v; want the recipient", id, ok)
	}
}

// The two layouts must describe the same pair. Anything else means the
// compatibility path quietly changed what the pilot is configured to do.
func TestLegacyAndNestedAgree(t *testing.T) {
	legacy, err := Load(strings.NewReader(legacyYAML))
	if err != nil {
		t.Fatalf("Load(legacy) error = %v", err)
	}
	nested, err := Load(strings.NewReader(nestedYAML))
	if err != nil {
		t.Fatalf("Load(nested) error = %v", err)
	}
	want, ok := nested.PairByID("acme-general")
	if !ok {
		t.Fatal("nested config has no acme-general pair")
	}
	got := legacy.Pairs[0]

	if got.ID != want.ID ||
		*got.Slack != *want.Slack ||
		*got.Teams != *want.Teams {
		t.Fatalf("legacy pair = %+v %+v %+v, nested = %+v %+v %+v",
			got.ID, got.Slack, got.Teams, want.ID, want.Slack, want.Teams)
	}
}

func TestLoadRejects(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "no pairs",
			yaml: "pairs: []\n",
			want: "at least one pair",
		},
		{
			name: "missing id",
			yaml: "pairs:\n  - teamsTeamId: t\n    teamsChannelId: c\n    slackChannelId: s\n",
			want: "id is required",
		},
		{
			name: "only one platform",
			yaml: "pairs:\n  - id: a\n    teamsTeamId: t\n    teamsChannelId: c\n",
			want: "exactly two platforms",
		},
		{
			name: "three platforms",
			yaml: "pairs:\n  - id: a\n    slack:\n      channelId: s\n" +
				"    teams:\n      teamId: t\n      channelId: c\n" +
				"    signal:\n      groupId: g\n",
			want: "exactly two platforms",
		},
		{
			name: "teams without team id",
			yaml: "pairs:\n  - id: a\n    slack:\n      channelId: s\n    teams:\n      channelId: c\n",
			want: "teams teamId is required",
		},
		{
			name: "signal with neither group nor recipient",
			yaml: "pairs:\n  - id: a\n    slack:\n      channelId: s\n    signal: {}\n",
			want: "either groupId or recipient",
		},
		{
			name: "signal with both group and recipient",
			yaml: "pairs:\n  - id: a\n    slack:\n      channelId: s\n" +
				"    signal:\n      groupId: g\n      recipient: \"+49170\"\n",
			want: "mutually exclusive",
		},
		{
			name: "duplicate slack channel",
			yaml: "pairs:\n" +
				"  - id: a\n    teamsTeamId: t1\n    teamsChannelId: c1\n    slackChannelId: S\n" +
				"  - id: b\n    teamsTeamId: t2\n    teamsChannelId: c2\n    slackChannelId: S\n",
			want: `duplicate slack conversation "S"`,
		},
		{
			name: "duplicate teams channel",
			yaml: "pairs:\n" +
				"  - id: a\n    teamsTeamId: t\n    teamsChannelId: C\n    slackChannelId: s1\n" +
				"  - id: b\n    teamsTeamId: t\n    teamsChannelId: C\n    slackChannelId: s2\n",
			want: `duplicate teams conversation "C"`,
		},
		{
			name: "duplicate signal group",
			yaml: "pairs:\n" +
				"  - id: a\n    slack:\n      channelId: s1\n    signal:\n      groupId: G\n" +
				"  - id: b\n    slack:\n      channelId: s2\n    signal:\n      groupId: G\n",
			want: `duplicate signal conversation "G"`,
		},
		{
			name: "duplicate pair id",
			yaml: "pairs:\n" +
				"  - id: a\n    teamsTeamId: t1\n    teamsChannelId: c1\n    slackChannelId: s1\n" +
				"  - id: a\n    teamsTeamId: t2\n    teamsChannelId: c2\n    slackChannelId: s2\n",
			want: "duplicate pair id",
		},
		{
			name: "both layouts for slack",
			yaml: "pairs:\n  - id: a\n    slackChannelId: s\n    slack:\n      channelId: s\n" +
				"    teams:\n      teamId: t\n      channelId: c\n",
			want: "cannot both be set",
		},
		{
			name: "both layouts for teams",
			yaml: "pairs:\n  - id: a\n    slack:\n      channelId: s\n" +
				"    teamsTeamId: t\n    teams:\n      teamId: t\n      channelId: c\n",
			want: "cannot both be set",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(strings.NewReader(tc.yaml))
			if err == nil {
				t.Fatal("Load() error = nil, want error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want it to contain %q", err, tc.want)
			}
		})
	}
}

func TestLoadRejectsUnknownFields(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "unknown field on a pair",
			yaml: "pairs:\n  - id: a\n    teamsTeamId: t\n    teamsChannelId: c\n" +
				"    slackChannelId: s\n    bogusField: surprise\n",
			want: "bogusField",
		},
		{
			name: "unknown field inside an endpoint",
			yaml: "pairs:\n  - id: a\n    slack:\n      channelId: s\n      bogusField: surprise\n" +
				"    teams:\n      teamId: t\n      channelId: c\n",
			want: "bogusField",
		},
		{
			name: "unknown top-level field",
			yaml: "pairs:\n  - id: a\n    slack:\n      channelId: s\n" +
				"    teams:\n      teamId: t\n      channelId: c\nbogusTop: surprise\n",
			want: "bogusTop",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(strings.NewReader(tc.yaml))
			if err == nil {
				t.Fatal("Load() error = nil, want an error for an unrecognised field")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want it to name the unknown field", err)
			}
		})
	}
}

func TestPairByID(t *testing.T) {
	cfg := Config{Pairs: []Pair{
		{ID: "acme-general", Teams: &TeamsEndpoint{TeamID: "t1", ChannelID: "c1"}, Slack: &SlackEndpoint{ChannelID: "s1"}},
		{ID: "acme-support", Teams: &TeamsEndpoint{TeamID: "t2", ChannelID: "c2"}, Slack: &SlackEndpoint{ChannelID: "s2"}},
		{ID: "acme-sales", Teams: &TeamsEndpoint{TeamID: "t3", ChannelID: "c3"}, Slack: &SlackEndpoint{ChannelID: "s3"}},
	}}

	p, ok := cfg.PairByID("acme-support")
	if !ok {
		t.Fatal("PairByID(acme-support) ok = false, want true")
	}
	if p.Slack.ChannelID != "s2" {
		t.Fatalf("Slack.ChannelID = %q, want s2", p.Slack.ChannelID)
	}

	if _, ok := cfg.PairByID("does-not-exist"); ok {
		t.Fatal("PairByID(does-not-exist) ok = true, want false")
	}
}

func TestPairFor(t *testing.T) {
	cfg, err := Load(strings.NewReader(nestedYAML))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	cases := []struct {
		name           string
		platform       model.Platform
		conversationID string
		wantPair       string
		wantFound      bool
	}{
		{
			name:           "teams channel",
			platform:       model.PlatformTeams,
			conversationID: "19:xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx@thread.tacv2",
			wantPair:       "acme-general",
			wantFound:      true,
		},
		{
			name:           "slack channel",
			platform:       model.PlatformSlack,
			conversationID: "C02SIGNAL",
			wantPair:       "acme-signal-group",
			wantFound:      true,
		},
		{
			name:           "signal group",
			platform:       model.PlatformSignal,
			conversationID: "Ci8KIExhbXBz",
			wantPair:       "acme-signal-group",
			wantFound:      true,
		},
		{
			name:           "signal recipient",
			platform:       model.PlatformSignal,
			conversationID: "+491701234567",
			wantPair:       "acme-signal-direct",
			wantFound:      true,
		},
		{
			// The same opaque id on another platform is a different
			// conversation, and answering with this pair would deliver into a
			// channel nobody asked for.
			name:           "right id, wrong platform",
			platform:       model.PlatformSignal,
			conversationID: "C02SIGNAL",
			wantFound:      false,
		},
		{
			name:           "unknown conversation",
			platform:       model.PlatformSlack,
			conversationID: "C99NOPE",
			wantFound:      false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, ok := cfg.PairFor(tc.platform, tc.conversationID)
			if ok != tc.wantFound {
				t.Fatalf("PairFor() ok = %v, want %v", ok, tc.wantFound)
			}
			if ok && p.ID != tc.wantPair {
				t.Fatalf("PairFor() pair = %q, want %q", p.ID, tc.wantPair)
			}
		})
	}
}

func TestPairsFor(t *testing.T) {
	cfg, err := Load(strings.NewReader(nestedYAML))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got := len(cfg.PairsFor(model.PlatformSlack)); got != 3 {
		t.Fatalf("PairsFor(slack) = %d pairs, want 3", got)
	}
	teams := cfg.PairsFor(model.PlatformTeams)
	if len(teams) != 1 || teams[0].ID != "acme-general" {
		t.Fatalf("PairsFor(teams) = %+v, want just acme-general", teams)
	}
	if got := len(cfg.PairsFor(model.PlatformSignal)); got != 2 {
		t.Fatalf("PairsFor(signal) = %d pairs, want 2", got)
	}
}

func TestCounterpart(t *testing.T) {
	cfg, err := Load(strings.NewReader(nestedYAML))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	general, _ := cfg.PairByID("acme-general")
	group, _ := cfg.PairByID("acme-signal-group")

	cases := []struct {
		name           string
		pair           Pair
		source         model.Platform
		wantPlatform   model.Platform
		wantConversion string
		wantFound      bool
	}{
		{
			name:           "teams to slack",
			pair:           general,
			source:         model.PlatformTeams,
			wantPlatform:   model.PlatformSlack,
			wantConversion: "C01ABCDEF",
			wantFound:      true,
		},
		{
			name:           "slack to teams",
			pair:           general,
			source:         model.PlatformSlack,
			wantPlatform:   model.PlatformTeams,
			wantConversion: "19:xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx@thread.tacv2",
			wantFound:      true,
		},
		{
			name:           "signal to slack",
			pair:           group,
			source:         model.PlatformSignal,
			wantPlatform:   model.PlatformSlack,
			wantConversion: "C02SIGNAL",
			wantFound:      true,
		},
		{
			name:           "slack to signal",
			pair:           group,
			source:         model.PlatformSlack,
			wantPlatform:   model.PlatformSignal,
			wantConversion: "Ci8KIExhbXBz",
			wantFound:      true,
		},
		{
			// This is the case Platform.Opposite got wrong: it answered
			// PlatformTeams for anything it did not recognise, so a message
			// from a platform the pair does not carry was delivered into the
			// customer's Teams channel with no error anywhere.
			name:      "platform the pair does not carry",
			pair:      group,
			source:    model.PlatformTeams,
			wantFound: false,
		},
		{
			name:      "unknown platform",
			pair:      general,
			source:    model.Platform("discord"),
			wantFound: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			platform, conversationID, ok := tc.pair.Counterpart(tc.source)
			if ok != tc.wantFound {
				t.Fatalf("Counterpart() ok = %v, want %v", ok, tc.wantFound)
			}
			if !ok {
				return
			}
			if platform != tc.wantPlatform {
				t.Fatalf("Counterpart() platform = %q, want %q", platform, tc.wantPlatform)
			}
			if conversationID != tc.wantConversion {
				t.Fatalf("Counterpart() conversation = %q, want %q", conversationID, tc.wantConversion)
			}
		})
	}
}
