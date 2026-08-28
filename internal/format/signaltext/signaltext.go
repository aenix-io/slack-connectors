// Package signaltext converts between Signal message bodies and Slack
// mrkdwn.
//
// A Signal body is plain text plus ranges: styles and mentions are described
// out of band, by offset and length, rather than by markup inside the text.
// Slack is the opposite — everything is inline. This package is the whole of
// that translation, and it is pure so the cases that matter can be pinned by
// tests rather than discovered in a customer's channel.
//
// Offsets are UTF-16 code units. Signal counts them that way because the
// clients that produced them are written in languages whose strings are
// UTF-16; Go strings are UTF-8 and index by byte. Every offset arriving here
// is therefore translated before it is used, and a range that lands mid-pair
// or past the end is clamped rather than trusted.
package signaltext

import (
	"sort"
	"strings"
	"unicode/utf16"

	"github.com/aenix-io/slack-connectors/internal/format/mrkdwn"
	"github.com/aenix-io/slack-connectors/internal/model"
)

// mentionPlaceholder is the character Signal leaves in the body where a
// mention belongs. The name to show lives in the mention range beside it, so
// rendering means replacing the placeholder rather than inserting into text.
const mentionPlaceholder = '￼'

// marker is the mrkdwn delimiter for a style. Slack has no spoiler, so a
// spoiler's text is rendered plainly: it is already visible to everyone in
// the Signal conversation, and hiding it behind a marker Slack does not
// understand would print the marker itself.
var markers = map[model.TextStyleKind]string{
	model.StyleBold:          "*",
	model.StyleItalic:        "_",
	model.StyleStrikethrough: "~",
	model.StyleMonospace:     "`",
	model.StyleSpoiler:       "",
}

// Resolver maps a person's identity on one platform to their identity on the
// other, reporting false when nobody has mapped them.
//
// There is no directory to discover this from: Signal knows a phone number
// and a service id, a Slack workspace knows neither. An unmapped person is
// therefore normal, and the answer for them is a readable name rather than a
// mention that might notify somebody else.
type Resolver func(externalID string) (string, bool)

// ToMrkdwn renders a Signal body, its style ranges and its mentions as Slack
// mrkdwn.
//
// A mention of somebody resolve knows becomes a real Slack mention, which
// notifies them; everyone else renders as a plain name.
func ToMrkdwn(text string, styles []model.TextStyle, mentions []model.Mention, resolve Resolver) string {
	if text == "" {
		return ""
	}
	units := utf16.Encode([]rune(text))

	// Mentions are substitutions over the body, so they are resolved first
	// and the styles are then applied to what is left. Doing it the other way
	// round would style a placeholder that is no longer there.
	segments := splitByMentions(units, mentions)

	var sb strings.Builder
	for _, seg := range segments {
		if seg.mention != "" {
			// A resolved mention is written after escaping, not before: the
			// angle brackets Slack reads as a reference are exactly what
			// escaping would turn into entities.
			if resolve != nil {
				if slackID, ok := resolve(seg.mentionID); ok {
					sb.WriteString("<@" + slackID + ">")
					continue
				}
			}
			sb.WriteString(escape("@" + seg.mention))
			continue
		}
		sb.WriteString(renderStyles(units, seg.start, seg.end, styles))
	}
	return sb.String()
}

// segment is either a run of body text or a mention standing in for one.
type segment struct {
	start, end int // in UTF-16 units, half-open
	mention    string
	mentionID  string
}

func splitByMentions(units []uint16, mentions []model.Mention) []segment {
	if len(mentions) == 0 {
		return []segment{{start: 0, end: len(units)}}
	}
	ordered := append([]model.Mention(nil), mentions...)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].Start < ordered[j].Start })

	var out []segment
	cursor := 0
	for _, m := range ordered {
		start, end, ok := clamp(m.Start, m.Length, len(units))
		if !ok || start < cursor {
			// A range outside the body, or overlapping one already consumed,
			// is a payload we do not understand. Dropping the mention keeps
			// the message readable; trusting it would slice the text at an
			// arbitrary point.
			continue
		}
		if start > cursor {
			out = append(out, segment{start: cursor, end: start})
		}
		name := m.DisplayName
		if name == "" {
			name = fallbackMentionName(units, start, end)
		}
		out = append(out, segment{start: start, end: end, mention: name, mentionID: m.ExternalID})
		cursor = end
	}
	if cursor < len(units) {
		out = append(out, segment{start: cursor, end: len(units)})
	}
	return out
}

// fallbackMentionName is what a mention renders as when the sender is unknown
// to us. The placeholder itself is invisible in most fonts, so echoing the
// body would silently drop the mention from the sentence.
func fallbackMentionName(units []uint16, start, end int) string {
	body := decode(units[start:end])
	if strings.TrimSpace(strings.Map(dropPlaceholder, body)) == "" {
		return "someone"
	}
	return body
}

func dropPlaceholder(r rune) rune {
	if r == mentionPlaceholder {
		return -1
	}
	return r
}

// renderStyles writes units[start:end] with the style ranges that fall inside
// it, opening and closing markers as the active set changes.
func renderStyles(units []uint16, start, end int, styles []model.TextStyle) string {
	if start >= end {
		return ""
	}
	active := activeStyles(styles, len(units))

	var sb strings.Builder
	var open []model.TextStyleKind
	runStart := start

	flush := func(upto int) {
		if upto > runStart {
			sb.WriteString(escape(decode(units[runStart:upto])))
		}
		runStart = upto
	}

	for i := start; i < end; i++ {
		want := active[i]
		if sameStyles(open, want) {
			continue
		}
		flush(i)
		// Markers nest, so only what differs is reopened: everything after
		// the common prefix closes in reverse, then the rest opens. Closing
		// the whole stack instead would emit "*hello **_world_*" for a bold
		// sentence with an italic word — the delimiters Slack then shows as
		// characters.
		common := commonPrefix(open, want)
		for j := len(open) - 1; j >= common; j-- {
			sb.WriteString(markers[open[j]])
		}
		open = open[:common]
		for _, kind := range want[common:] {
			sb.WriteString(markers[kind])
			open = append(open, kind)
		}
	}
	flush(end)
	for j := len(open) - 1; j >= 0; j-- {
		sb.WriteString(markers[open[j]])
	}
	return sb.String()
}

// activeStyles maps every position in the body to the styles covering it, in
// a stable order so the same input always produces the same markers.
func activeStyles(styles []model.TextStyle, length int) [][]model.TextStyleKind {
	at := make([][]model.TextStyleKind, length+1)
	ordered := append([]model.TextStyle(nil), styles...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].Start != ordered[j].Start {
			return ordered[i].Start < ordered[j].Start
		}
		if ordered[i].Length != ordered[j].Length {
			return ordered[i].Length > ordered[j].Length
		}
		return ordered[i].Style < ordered[j].Style
	})
	for _, s := range ordered {
		if _, ok := markers[s.Style]; !ok {
			continue // a style this Slack rendering has no marker for
		}
		start, end, ok := clamp(s.Start, s.Length, length)
		if !ok {
			continue
		}
		for i := start; i < end; i++ {
			at[i] = append(at[i], s.Style)
		}
	}
	return at
}

// commonPrefix is how many styles the two sets share from the outside in.
// Only styles beyond it have to be closed and reopened.
func commonPrefix(a, b []model.TextStyleKind) int {
	n := 0
	for n < len(a) && n < len(b) && a[n] == b[n] {
		n++
	}
	return n
}

func sameStyles(a, b []model.TextStyleKind) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// clamp turns a Signal range into a half-open interval inside the body,
// reporting false when nothing usable is left.
//
// Ranges arrive from other clients and are not guaranteed to fit: a length
// past the end is what an edited message produces, and slicing on it would
// panic in the middle of delivering someone's message.
func clamp(start, length, size int) (int, int, bool) {
	if length <= 0 || start < 0 || start >= size {
		return 0, 0, false
	}
	end := start + length
	if end > size {
		end = size
	}
	return start, end, true
}

func decode(units []uint16) string {
	return string(utf16.Decode(units))
}

// escape protects the three characters Slack reads as markup. Slack itself
// delivers text with these already escaped, so a body coming from Signal —
// which escapes nothing — has to be escaped here or a message containing
// "<not a link>" arrives mangled.
var escaper = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;")

func escape(s string) string { return escaper.Replace(s) }

// FromMrkdwn converts Slack mrkdwn into a Signal body: plain text, plus the
// style ranges Signal carries beside it.
//
// Slack's entities are decoded, because Signal escapes nothing and a message
// reading "a &amp; b" is what a customer would see otherwise. Offsets in the
// returned ranges are UTF-16 code units, counted over the text as returned.
func FromMrkdwn(in string, resolve Resolver) (string, []model.TextStyle, []model.Mention) {
	b := bodyBuilder{resolve: resolve}
	b.writeNodes(mrkdwn.Parse(in), nil)
	return b.text.String(), b.styles, b.mentions
}

// bodyBuilder accumulates the plain body and the ranges over it. It tracks
// length in UTF-16 units as it goes rather than measuring afterwards, so the
// offsets cannot drift from the text they describe.
type bodyBuilder struct {
	text     strings.Builder
	length   int // in UTF-16 code units
	styles   []model.TextStyle
	mentions []model.Mention
	resolve  Resolver
}

func (b *bodyBuilder) write(s string) {
	if s == "" {
		return
	}
	b.text.WriteString(s)
	b.length += len(utf16.Encode([]rune(s)))
}

// writeNodes renders a run of nodes under the styles currently open.
func (b *bodyBuilder) writeNodes(nodes []mrkdwn.Node, open []model.TextStyleKind) {
	for _, n := range nodes {
		b.writeNode(n, open)
	}
}

func (b *bodyBuilder) writeNode(n mrkdwn.Node, open []model.TextStyleKind) {
	switch n.Kind {
	case mrkdwn.KindEmphasis:
		b.styled(styleOf(n.Style), open, func(inner []model.TextStyleKind) {
			b.writeNodes(n.Children, inner)
		})
	case mrkdwn.KindCodeSpan:
		b.styled(model.StyleMonospace, open, func([]model.TextStyleKind) {
			b.write(unescapeEntities(n.Text))
		})
	case mrkdwn.KindCodeBlock:
		// Signal has no block code, only a monospace range. Keeping the
		// newlines and marking the whole run is as close as the platform
		// gets, and it survives copy-paste, which is what code blocks are
		// usually for.
		b.styled(model.StyleMonospace, open, func([]model.TextStyleKind) {
			b.write(unescapeEntities(n.Text))
		})
	case mrkdwn.KindLink:
		label := unescapeEntities(n.Text)
		url := unescapeEntities(n.URL)
		if label == "" || label == url {
			b.write(url)
			return
		}
		// Signal has no link labels: dropping the URL would hide where a
		// link goes, and dropping the label would lose what it was called.
		b.write(label + " (" + url + ")")
	case mrkdwn.KindUserRef:
		if b.resolve != nil {
			if serviceID, ok := b.resolve(n.ID); ok {
				// Signal carries a mention out of band: the body holds one
				// placeholder character and the range beside it names who is
				// meant.
				b.mentions = append(b.mentions, model.Mention{
					ExternalID: serviceID,
					Start:      b.length,
					Length:     1,
				})
				b.write(string(mentionPlaceholder))
				return
			}
		}
		// Nobody has mapped this person. A readable name is what a reader
		// needs, and it cannot notify the wrong person.
		b.write("@" + unescapeEntities(mrkdwn.RefName(n)))
	case mrkdwn.KindChannelRef:
		b.write("#" + unescapeEntities(mrkdwn.RefName(n)))
	case mrkdwn.KindBroadcast:
		name, addressesEveryone := mrkdwn.BroadcastName(n)
		if addressesEveryone {
			// Deliberately plain text. Someone writes @channel meaning three
			// colleagues, and a translated mention wakes a customer's entire
			// group; one such incident costs more than the convenience.
			b.write("@" + name)
			return
		}
		b.write("@" + unescapeEntities(name))
	default:
		b.write(unescapeEntities(n.Text))
	}
}

// styled records the range a nested render covers, if it covered anything.
func (b *bodyBuilder) styled(kind model.TextStyleKind, open []model.TextStyleKind, render func([]model.TextStyleKind)) {
	start := b.length
	render(append(open, kind))
	if b.length > start {
		b.styles = append(b.styles, model.TextStyle{
			Style:  kind,
			Start:  start,
			Length: b.length - start,
		})
	}
}

func styleOf(s mrkdwn.Style) model.TextStyleKind {
	switch s {
	case mrkdwn.StyleBold:
		return model.StyleBold
	case mrkdwn.StyleStrike:
		return model.StyleStrikethrough
	default:
		return model.StyleItalic
	}
}

// unescaper decodes what Slack escaped. Signal carries plain text, so leaving
// entities in place would show a customer "a &amp; b".
var unescaper = strings.NewReplacer("&amp;", "&", "&lt;", "<", "&gt;", ">", "&quot;", `"`)

func unescapeEntities(s string) string { return unescaper.Replace(s) }

// AuthorLine prefixes a body with the name of the person who wrote it, styled
// bold, and returns the combined body with every range shifted to match.
//
// Signal offers no impersonation: every message the bridge sends is authored
// by the bridge number. Without this line a customer sees a wall of messages
// from one account with no idea who is speaking.
func AuthorLine(
	name, body string, styles []model.TextStyle, mentions []model.Mention,
) (string, []model.TextStyle, []model.Mention) {
	if name == "" {
		return body, styles, mentions
	}
	prefix := name + "\n"
	offset := len(utf16.Encode([]rune(prefix)))

	shifted := make([]model.TextStyle, 0, len(styles)+1)
	shifted = append(shifted, model.TextStyle{
		Style:  model.StyleBold,
		Start:  0,
		Length: len(utf16.Encode([]rune(name))),
	})
	for _, s := range styles {
		s.Start += offset
		shifted = append(shifted, s)
	}
	// Mentions are ranges over the same body, so they move with it. Leaving
	// them where they were would mark the author's own name as a mention of
	// somebody else.
	movedMentions := make([]model.Mention, 0, len(mentions))
	for _, m := range mentions {
		m.Start += offset
		movedMentions = append(movedMentions, m)
	}
	return prefix + body, shifted, movedMentions
}
