// Package slackmrkdwn converts Slack mrkdwn into Microsoft Teams message HTML.
//
// The reading of the input belongs to the mrkdwn package, which the Signal
// converter shares; this package is only the rendering.
package slackmrkdwn

import (
	"strings"

	"github.com/aenix-io/slack-connectors/internal/format/mrkdwn"
)

var emphasisTags = map[mrkdwn.Style][2]string{
	mrkdwn.StyleBold:   {"<strong>", "</strong>"},
	mrkdwn.StyleItalic: {"<em>", "</em>"},
	// <strike>, not <s>: Teams documents only <strike> in its supported set.
	mrkdwn.StyleStrike: {"<strike>", "</strike>"},
}

// ToHTML renders Slack mrkdwn as the HTML Teams accepts in a message body.
//
// Slack delivers text with &, < and > already escaped as entities, so text is
// passed through rather than escaped a second time; only the raw brackets left
// behind by Slack's link syntax are escaped here.
func ToHTML(in string) string {
	var sb strings.Builder
	renderNodes(&sb, mrkdwn.Parse(in))
	return sb.String()
}

func renderNodes(sb *strings.Builder, nodes []mrkdwn.Node) {
	for _, n := range nodes {
		renderNode(sb, n)
	}
}

func renderNode(sb *strings.Builder, n mrkdwn.Node) {
	switch n.Kind {
	case mrkdwn.KindCodeBlock:
		sb.WriteString("<pre><code>")
		sb.WriteString(escapeText(n.Text))
		sb.WriteString("</code></pre>")
	case mrkdwn.KindCodeSpan:
		sb.WriteString("<code>")
		sb.WriteString(escapeText(n.Text))
		sb.WriteString("</code>")
	case mrkdwn.KindEmphasis:
		tag := emphasisTags[n.Style]
		sb.WriteString(tag[0])
		renderNodes(sb, n.Children)
		sb.WriteString(tag[1])
	case mrkdwn.KindLink:
		sb.WriteString(`<a href="`)
		sb.WriteString(escapeAttr(unescapeEntities(n.URL)))
		sb.WriteString(`">`)
		sb.WriteString(escapeText(labelOr(n.Text, n.URL)))
		sb.WriteString("</a>")
	case mrkdwn.KindUserRef:
		// Teams has no way to resolve a Slack user id, so the readable name
		// is the best available rendering. Identity linking lands later.
		sb.WriteString("@")
		sb.WriteString(escapeText(mrkdwn.RefName(n)))
	case mrkdwn.KindChannelRef:
		sb.WriteString("#")
		sb.WriteString(escapeText(mrkdwn.RefName(n)))
	case mrkdwn.KindBroadcast:
		name, everyone := mrkdwn.BroadcastName(n)
		if everyone {
			// Deliberately plain text: someone writing @channel to three
			// colleagues must not notify a customer's whole team.
			sb.WriteString("@")
			sb.WriteString(name)
			return
		}
		if n.Text != "" {
			sb.WriteString(escapeText(name))
			return
		}
		sb.WriteString("@")
		sb.WriteString(escapeText(name))
	default:
		writeText(sb, n.Text)
	}
}

func labelOr(label, fallback string) string {
	if label != "" {
		return label
	}
	return fallback
}

// writeText escapes the brackets Slack leaves raw and turns newlines into the
// break Teams renders.
func writeText(sb *strings.Builder, s string) {
	for _, r := range s {
		switch r {
		case '<':
			sb.WriteString("&lt;")
		case '>':
			sb.WriteString("&gt;")
		case '\n':
			sb.WriteString("<br>")
		default:
			sb.WriteRune(r)
		}
	}
}

// textEscaper escapes the brackets Slack leaves raw. Ampersands are left alone
// because Slack has already escaped them and escaping twice is the failure
// mode this converter exists to avoid.
var textEscaper = strings.NewReplacer("<", "&lt;", ">", "&gt;")

func escapeText(s string) string { return textEscaper.Replace(s) }

var attrEscaper = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;")

// escapeAttr escapes a URL for an href. The URL is unescaped first so that a
// link Slack delivered with entities is not escaped a second time.
func escapeAttr(s string) string { return attrEscaper.Replace(s) }

var entityUnescaper = strings.NewReplacer("&amp;", "&", "&lt;", "<", "&gt;", ">", "&quot;", `"`)

func unescapeEntities(s string) string { return entityUnescaper.Replace(s) }
