package signaltext

import (
	"testing"
	"unicode/utf16"

	"github.com/aenix-io/slack-connectors/internal/model"
)

// utf16Len is how Signal measures an offset. The tests use it rather than
// literal numbers wherever the count is the point, so a reader can see what
// the offset is counting.
func utf16Len(s string) int { return len(utf16.Encode([]rune(s))) }

func TestToMrkdwnStyles(t *testing.T) {
	cases := []struct {
		name    string
		text    string
		styles  []model.TextStyle
		want    string
		comment string
	}{
		{
			name: "plain text is unchanged",
			text: "hello world",
			want: "hello world",
		},
		{
			name:   "bold span",
			text:   "hello world",
			styles: []model.TextStyle{{Style: model.StyleBold, Start: 6, Length: 5}},
			want:   "hello *world*",
		},
		{
			name:   "italic at the start",
			text:   "hello world",
			styles: []model.TextStyle{{Style: model.StyleItalic, Start: 0, Length: 5}},
			want:   "_hello_ world",
		},
		{
			name:   "strikethrough to the end",
			text:   "hello world",
			styles: []model.TextStyle{{Style: model.StyleStrikethrough, Start: 6, Length: 5}},
			want:   "hello ~world~",
		},
		{
			name:   "monospace",
			text:   "run make test now",
			styles: []model.TextStyle{{Style: model.StyleMonospace, Start: 4, Length: 9}},
			want:   "run `make test` now",
		},
		{
			name:   "whole body",
			text:   "shout",
			styles: []model.TextStyle{{Style: model.StyleBold, Start: 0, Length: 5}},
			want:   "*shout*",
		},
		{
			name: "nested styles close in order",
			text: "hello world",
			styles: []model.TextStyle{
				{Style: model.StyleBold, Start: 0, Length: 11},
				{Style: model.StyleItalic, Start: 6, Length: 5},
			},
			want: "*hello _world_*",
		},
		{
			name: "adjacent spans do not merge",
			text: "abcd",
			styles: []model.TextStyle{
				{Style: model.StyleBold, Start: 0, Length: 2},
				{Style: model.StyleItalic, Start: 2, Length: 2},
			},
			want: "*ab*_cd_",
		},
		{
			name: "overlapping spans",
			text: "abcdef",
			styles: []model.TextStyle{
				{Style: model.StyleBold, Start: 0, Length: 4},
				{Style: model.StyleItalic, Start: 2, Length: 4},
			},
			want: "*ab_cd_*_ef_",
		},
		{
			// Slack has no spoiler. The text is already visible to everyone in
			// the Signal conversation, so it renders plainly rather than
			// printing a delimiter Slack would show as characters.
			name:   "spoiler renders as plain text",
			text:   "the answer is 42",
			styles: []model.TextStyle{{Style: model.StyleSpoiler, Start: 14, Length: 2}},
			want:   "the answer is 42",
		},
		{
			name:   "unknown style is ignored",
			text:   "hello",
			styles: []model.TextStyle{{Style: model.TextStyleKind("RAINBOW"), Start: 0, Length: 5}},
			want:   "hello",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ToMrkdwn(tc.text, tc.styles, nil, nil); got != tc.want {
				t.Fatalf("ToMrkdwn() = %q, want %q", got, tc.want)
			}
		})
	}
}

// Offsets are UTF-16 code units, which is the one thing about this format a
// Go implementation is most likely to get wrong. Each case below has a
// different answer under byte offsets, rune offsets, or both.
func TestToMrkdwnOffsetsAreUTF16CodeUnits(t *testing.T) {
	cases := []struct {
		name   string
		text   string
		styles []model.TextStyle
		want   string
	}{
		{
			// Cyrillic: one UTF-16 unit per character, two bytes per
			// character. Byte offsets would style the wrong half.
			name:   "cyrillic",
			text:   "привет мир",
			styles: []model.TextStyle{{Style: model.StyleBold, Start: 7, Length: 3}},
			want:   "привет *мир*",
		},
		{
			// An astral-plane emoji is one rune and two UTF-16 units. Rune
			// offsets would style one character too early from here on.
			name: "emoji before the styled span",
			text: "🚀 to the moon",
			styles: []model.TextStyle{
				{Style: model.StyleBold, Start: utf16Len("🚀 to the "), Length: utf16Len("moon")},
			},
			want: "🚀 to the *moon*",
		},
		{
			name: "surrogate pair inside the styled span",
			text: "say 🚀 loudly",
			styles: []model.TextStyle{
				{Style: model.StyleBold, Start: utf16Len("say "), Length: utf16Len("🚀 loudly")},
			},
			want: "say *🚀 loudly*",
		},
		{
			name: "combining sequence",
			text: "éclair time",
			styles: []model.TextStyle{
				{Style: model.StyleItalic, Start: 0, Length: utf16Len("éclair")},
			},
			want: "_éclair_ time",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ToMrkdwn(tc.text, tc.styles, nil, nil); got != tc.want {
				t.Fatalf("ToMrkdwn() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestToMrkdwnMentions(t *testing.T) {
	// Signal leaves U+FFFC in the body where a mention belongs and describes
	// it in a range alongside.
	const placeholder = "￼"

	cases := []struct {
		name     string
		text     string
		mentions []model.Mention
		styles   []model.TextStyle
		want     string
	}{
		{
			name:     "mention becomes a name",
			text:     placeholder + " can you look at this",
			mentions: []model.Mention{{DisplayName: "Ada Lovelace", Start: 0, Length: 1}},
			want:     "@Ada Lovelace can you look at this",
		},
		{
			name: "mention in the middle",
			text: "ping " + placeholder + " please",
			mentions: []model.Mention{
				{DisplayName: "Ada", Start: utf16Len("ping "), Length: 1},
			},
			want: "ping @Ada please",
		},
		{
			name: "two mentions",
			text: placeholder + " and " + placeholder,
			mentions: []model.Mention{
				{DisplayName: "Ada", Start: 0, Length: 1},
				{DisplayName: "Grace", Start: utf16Len(placeholder + " and "), Length: 1},
			},
			want: "@Ada and @Grace",
		},
		{
			name:     "unknown mention still reads as a mention",
			text:     placeholder + " hello",
			mentions: []model.Mention{{Start: 0, Length: 1}},
			want:     "@someone hello",
		},
		{
			name: "styles around a mention still apply",
			text: "hi " + placeholder + " now",
			mentions: []model.Mention{
				{DisplayName: "Ada", Start: utf16Len("hi "), Length: 1},
			},
			styles: []model.TextStyle{
				{Style: model.StyleBold, Start: utf16Len("hi ￼ "), Length: utf16Len("now")},
			},
			want: "hi @Ada *now*",
		},
		{
			name: "mention range past the end is dropped",
			text: "short",
			mentions: []model.Mention{
				{DisplayName: "Ada", Start: 99, Length: 1},
			},
			want: "short",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ToMrkdwn(tc.text, tc.styles, tc.mentions, nil); got != tc.want {
				t.Fatalf("ToMrkdwn() = %q, want %q", got, tc.want)
			}
		})
	}
}

// Signal escapes nothing, Slack reads three characters as markup. Without
// escaping here a message containing a bare angle bracket arrives mangled, or
// worse, as a link the sender never wrote.
func TestToMrkdwnEscapesSlackMarkup(t *testing.T) {
	cases := []struct {
		name, text, want string
	}{
		{name: "ampersand", text: "you & me", want: "you &amp; me"},
		{name: "angle brackets", text: "<not a link>", want: "&lt;not a link&gt;"},
		{
			name: "looks like a slack user reference",
			text: "<@U01ABCDEF>",
			want: "&lt;@U01ABCDEF&gt;",
		},
		{
			name: "looks like a broadcast",
			text: "<!channel>",
			want: "&lt;!channel&gt;",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ToMrkdwn(tc.text, nil, nil, nil); got != tc.want {
				t.Fatalf("ToMrkdwn() = %q, want %q", got, tc.want)
			}
		})
	}
}

// Ranges come from other clients and are not guaranteed to fit the body. None
// of these may panic: a slice out of range here happens while delivering
// someone's message.
func TestToMrkdwnSurvivesMalformedRanges(t *testing.T) {
	cases := []struct {
		name     string
		text     string
		styles   []model.TextStyle
		mentions []model.Mention
		want     string
	}{
		{
			name:   "length past the end is clamped",
			text:   "hello",
			styles: []model.TextStyle{{Style: model.StyleBold, Start: 2, Length: 999}},
			want:   "he*llo*",
		},
		{
			name:   "start past the end is dropped",
			text:   "hello",
			styles: []model.TextStyle{{Style: model.StyleBold, Start: 50, Length: 2}},
			want:   "hello",
		},
		{
			name:   "negative start is dropped",
			text:   "hello",
			styles: []model.TextStyle{{Style: model.StyleBold, Start: -1, Length: 3}},
			want:   "hello",
		},
		{
			name:   "zero length is dropped",
			text:   "hello",
			styles: []model.TextStyle{{Style: model.StyleBold, Start: 1, Length: 0}},
			want:   "hello",
		},
		{
			name:     "overlapping mentions keep the first",
			text:     "￼￼",
			mentions: []model.Mention{{DisplayName: "Ada", Start: 0, Length: 2}, {DisplayName: "Grace", Start: 1, Length: 1}},
			want:     "@Ada",
		},
		{
			name: "empty body",
			text: "",
			want: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ToMrkdwn(tc.text, tc.styles, tc.mentions, nil); got != tc.want {
				t.Fatalf("ToMrkdwn() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestFromMrkdwn(t *testing.T) {
	cases := []struct {
		name       string
		in         string
		wantText   string
		wantStyles []model.TextStyle
	}{
		{name: "plain", in: "hello", wantText: "hello"},
		{
			name:       "bold",
			in:         "hello *world*",
			wantText:   "hello world",
			wantStyles: []model.TextStyle{{Style: model.StyleBold, Start: 6, Length: 5}},
		},
		{
			name:       "italic",
			in:         "_quietly_ now",
			wantText:   "quietly now",
			wantStyles: []model.TextStyle{{Style: model.StyleItalic, Start: 0, Length: 7}},
		},
		{
			name:       "strikethrough",
			in:         "~gone~",
			wantText:   "gone",
			wantStyles: []model.TextStyle{{Style: model.StyleStrikethrough, Start: 0, Length: 4}},
		},
		{
			name:       "inline code becomes monospace",
			in:         "run `make test`",
			wantText:   "run make test",
			wantStyles: []model.TextStyle{{Style: model.StyleMonospace, Start: 4, Length: 9}},
		},
		{
			// Signal has no block code, only a monospace range. The newlines
			// survive, which is what a pasted snippet is for.
			name:       "code block keeps its newlines",
			in:         "```one\ntwo```",
			wantText:   "one\ntwo",
			wantStyles: []model.TextStyle{{Style: model.StyleMonospace, Start: 0, Length: 7}},
		},
		{
			name:     "nested emphasis produces both ranges",
			in:       "*bold _and italic_*",
			wantText: "bold and italic",
			wantStyles: []model.TextStyle{
				{Style: model.StyleItalic, Start: 5, Length: 10},
				{Style: model.StyleBold, Start: 0, Length: 15},
			},
		},
		{
			name:     "markers that are not emphasis stay literal",
			in:       "snake_case_name and 5*3=15",
			wantText: "snake_case_name and 5*3=15",
		},
		{
			// Offsets are UTF-16 units: Cyrillic is one unit per character
			// and two bytes, so a byte offset would land mid-character.
			name:       "cyrillic offsets",
			in:         "привет *мир*",
			wantText:   "привет мир",
			wantStyles: []model.TextStyle{{Style: model.StyleBold, Start: 7, Length: 3}},
		},
		{
			// An astral-plane emoji is two units and one rune.
			name:       "emoji before a styled span",
			in:         "🚀 to the *moon*",
			wantText:   "🚀 to the moon",
			wantStyles: []model.TextStyle{{Style: model.StyleBold, Start: 10, Length: 4}},
		},
		{
			name:     "entities are decoded",
			in:       "you &amp; me &lt;here&gt;",
			wantText: "you & me <here>",
		},
		{
			name:     "labelled link keeps both label and target",
			in:       "see <https://example.com|the docs>",
			wantText: "see the docs (https://example.com)",
		},
		{
			name:     "bare link is just the url",
			in:       "see <https://example.com>",
			wantText: "see https://example.com",
		},
		{
			name:     "user reference renders as a name",
			in:       "<@U012ABCDEF|ada> please look",
			wantText: "@ada please look",
		},
		{
			name:     "channel reference renders as a name",
			in:       "see <#C012AB|general>",
			wantText: "see #general",
		},
		{
			// Deliberately plain text: @channel meaning three colleagues must
			// not notify a customer's whole group.
			name:     "broadcast stays plain text",
			in:       "<!channel> heads up",
			wantText: "@channel heads up",
		},
		{
			name:     "emphasis inside code stays literal",
			in:       "`*not bold*`",
			wantText: "*not bold*",
			wantStyles: []model.TextStyle{
				{Style: model.StyleMonospace, Start: 0, Length: 10},
			},
		},
		{name: "empty", in: "", wantText: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			text, styles, _ := FromMrkdwn(tc.in, nil)
			if text != tc.wantText {
				t.Fatalf("text = %q, want %q", text, tc.wantText)
			}
			if len(styles) != len(tc.wantStyles) {
				t.Fatalf("styles = %+v, want %+v", styles, tc.wantStyles)
			}
			for i, want := range tc.wantStyles {
				if styles[i] != want {
					t.Fatalf("styles[%d] = %+v, want %+v", i, styles[i], want)
				}
			}
		})
	}
}

// Whatever FromMrkdwn produces has to be addressable by the offsets it
// reports: a range past the end of the body would be rejected or, worse,
// applied to the wrong characters by a client that clamps differently.
func TestFromMrkdwnOffsetsStayInsideTheBody(t *testing.T) {
	inputs := []string{
		"*bold* _italic_ ~strike~ `code`",
		"🚀 *🚀* 🚀",
		"привет *мир* ```код```",
		"*a _b ~c `d` c~ b_ a*",
	}
	for _, in := range inputs {
		t.Run(in, func(t *testing.T) {
			text, styles, _ := FromMrkdwn(in, nil)
			length := utf16Len(text)
			for _, s := range styles {
				if s.Start < 0 || s.Length <= 0 || s.Start+s.Length > length {
					t.Fatalf("style %+v does not fit a body of %d units (%q)", s, length, text)
				}
			}
		})
	}
}

func TestAuthorLine(t *testing.T) {
	body, styles, _ := FromMrkdwn("see *this*", nil)
	body, styles, _ = AuthorLine("Timur Tukaev", body, styles, nil)

	if body != "Timur Tukaev\nsee this" {
		t.Fatalf("body = %q", body)
	}
	if len(styles) != 2 {
		t.Fatalf("styles = %+v, want the name plus the shifted span", styles)
	}
	if want := (model.TextStyle{Style: model.StyleBold, Start: 0, Length: 12}); styles[0] != want {
		t.Fatalf("styles[0] = %+v, want %+v", styles[0], want)
	}
	// "Timur Tukaev\n" is 13 units, so the body's span moves by 13.
	if want := (model.TextStyle{Style: model.StyleBold, Start: 17, Length: 4}); styles[1] != want {
		t.Fatalf("styles[1] = %+v, want %+v", styles[1], want)
	}
}

// A round trip must not accumulate markup: what Signal sends, Slack renders,
// and what Slack sends, Signal renders — the same words either way.
func TestRoundTripKeepsTheWords(t *testing.T) {
	cases := []string{
		"hello world",
		"see *this* now",
		"_quietly_ и по-русски",
		"🚀 *launch* 🚀",
	}
	for _, in := range cases {
		t.Run(in, func(t *testing.T) {
			text, styles, _ := FromMrkdwn(in, nil)
			back := ToMrkdwn(text, styles, nil, nil)
			if back != in {
				t.Fatalf("round trip = %q, want %q", back, in)
			}
		})
	}
}

// A mention only becomes a real mention for somebody an operator has mapped.
// Everyone else reads as a name — which cannot notify the wrong person, and
// there is no directory that could tell us who they are.
func TestMentionsResolveWhenMapped(t *testing.T) {
	const placeholder = "￼"

	t.Run("signal to slack", func(t *testing.T) {
		resolve := func(serviceID string) (string, bool) {
			if serviceID == "uuid-ada" {
				return "U0ADA", true
			}
			return "", false
		}
		got := ToMrkdwn(placeholder+" and "+placeholder,
			nil,
			[]model.Mention{
				{ExternalID: "uuid-ada", DisplayName: "Ada", Start: 0, Length: 1},
				{ExternalID: "uuid-grace", DisplayName: "Grace", Start: utf16Len(placeholder + " and "), Length: 1},
			},
			resolve)
		if want := "<@U0ADA> and @Grace"; got != want {
			t.Fatalf("ToMrkdwn() = %q, want %q", got, want)
		}
	})

	t.Run("slack to signal", func(t *testing.T) {
		resolve := func(slackID string) (string, bool) {
			if slackID == "U0ADA" {
				return "uuid-ada", true
			}
			return "", false
		}
		text, _, mentions := FromMrkdwn("<@U0ADA|ada> and <@U0BOB|bob> please", resolve)

		// The mapped person becomes a placeholder plus a range; the unmapped
		// one stays readable text.
		if want := placeholder + " and @bob please"; text != want {
			t.Fatalf("text = %q, want %q", text, want)
		}
		if len(mentions) != 1 {
			t.Fatalf("got %d mentions, want 1", len(mentions))
		}
		if mentions[0].ExternalID != "uuid-ada" || mentions[0].Start != 0 || mentions[0].Length != 1 {
			t.Fatalf("mention = %+v", mentions[0])
		}
	})
}

// The author line shifts the body, and a mention is a range over that body.
// Left where it was, it would mark part of the author's own name.
func TestAuthorLineMovesMentions(t *testing.T) {
	resolve := func(string) (string, bool) { return "uuid-ada", true }
	body, styles, mentions := FromMrkdwn("<@U0ADA> take a look", resolve)
	body, styles, mentions = AuthorLine("Timur", body, styles, mentions)

	if body != "Timur\n￼ take a look" {
		t.Fatalf("body = %q", body)
	}
	if len(mentions) != 1 {
		t.Fatalf("got %d mentions, want 1", len(mentions))
	}
	// "Timur\n" is 6 UTF-16 units.
	if mentions[0].Start != 6 {
		t.Fatalf("mention start = %d, want 6", mentions[0].Start)
	}
	if len(styles) == 0 || styles[0].Style != model.StyleBold || styles[0].Length != 5 {
		t.Fatalf("styles = %+v, want the name in bold", styles)
	}
}
