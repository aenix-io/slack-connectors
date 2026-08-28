// Package mrkdwn parses Slack's message markup into a tree.
//
// It exists because two converters need the same reading of the same text:
// one renders it as Teams HTML, the other as a Signal body with formatting
// carried in ranges beside it. Slack's rules for when an asterisk is emphasis
// and when it is arithmetic are subtle enough that having two implementations
// of them means having two different ones within a year.
//
// The parser does not unescape. Slack delivers text with &, < and > already
// escaped, and whether they should stay that way depends on the target: Slack
// and Teams both want entities, Signal wants the characters. Each renderer
// decides.
package mrkdwn

import (
	"regexp"
	"strings"
	"unicode"
)

// Kind distinguishes the nodes a parsed message is made of.
type Kind int

const (
	// KindText is literal text, still carrying Slack's entity escaping.
	KindText Kind = iota
	// KindEmphasis is a styled span; its Style says which, and its Children
	// are what it contains.
	KindEmphasis
	// KindCodeSpan and KindCodeBlock carry their content verbatim: markup
	// inside code is not markup.
	KindCodeSpan
	KindCodeBlock
	// KindLink is <url> or <url|label>.
	KindLink
	// KindUserRef is <@U…> or <@U…|label>, KindChannelRef is <#C…|name>.
	KindUserRef
	KindChannelRef
	// KindBroadcast is <!channel>, <!here>, <!everyone> and the labelled
	// forms such as <!subteam^S012|@devs>.
	KindBroadcast
)

// Style is which emphasis a span carries.
type Style int

const (
	StyleBold Style = iota
	StyleItalic
	StyleStrike
)

// Node is one element of a parsed message.
type Node struct {
	Kind Kind
	// Text is literal text, code content, a link label, or a reference's
	// display text. Empty for a node whose content is in Children.
	Text string
	// ID is the identifier behind a reference: a user id, a channel id, or a
	// broadcast name such as "channel".
	ID string
	// URL is a link's target.
	URL string
	// Style applies to KindEmphasis.
	Style Style
	// Children are the contents of an emphasis span.
	Children []Node
}

// bracketPattern matches Slack's angle-bracket forms — links, user mentions,
// channel references and broadcasts — the one place where angle brackets
// arrive raw rather than as entities. A match that turns out not to be one of
// those forms is text.
var bracketPattern = regexp.MustCompile(`<([@#!]?)([^<>|]*)(?:\|([^<>]*))?>`)

var emphasisStyles = map[rune]Style{
	'*': StyleBold,
	'_': StyleItalic,
	'~': StyleStrike,
}

// Parse reads Slack mrkdwn into a tree.
func Parse(in string) []Node {
	if in == "" {
		return nil
	}
	var out []Node
	for _, seg := range splitCode(in) {
		switch seg.kind {
		case KindCodeBlock:
			out = append(out, Node{Kind: KindCodeBlock, Text: strings.Trim(seg.text, "\n")})
		case KindCodeSpan:
			out = append(out, Node{Kind: KindCodeSpan, Text: seg.text})
		default:
			out = append(out, parseBrackets(seg.text)...)
		}
	}
	return out
}

type codeSegment struct {
	kind Kind
	text string
}

// splitCode carves out code first so that emphasis and links inside it stay
// literal.
func splitCode(in string) []codeSegment {
	var out []codeSegment
	var text strings.Builder
	flush := func() {
		if text.Len() > 0 {
			out = append(out, codeSegment{kind: KindText, text: text.String()})
			text.Reset()
		}
	}
	for i := 0; i < len(in); {
		if strings.HasPrefix(in[i:], "```") {
			if end := strings.Index(in[i+3:], "```"); end >= 0 {
				flush()
				out = append(out, codeSegment{kind: KindCodeBlock, text: in[i+3 : i+3+end]})
				i += 3 + end + 3
				continue
			}
		}
		if in[i] == '`' {
			if end := strings.IndexByte(in[i+1:], '`'); end >= 0 {
				flush()
				out = append(out, codeSegment{kind: KindCodeSpan, text: in[i+1 : i+1+end]})
				i += 1 + end + 1
				continue
			}
		}
		text.WriteByte(in[i])
		i++
	}
	flush()
	return out
}

// parseBrackets splits a run of text on Slack's bracket forms, parsing
// emphasis in what lies between them. Emphasis is not carried across a
// bracket boundary.
func parseBrackets(s string) []Node {
	var out []Node
	last := 0
	for _, m := range bracketPattern.FindAllStringSubmatchIndex(s, -1) {
		out = append(out, parseEmphasis(s[last:m[0]])...)
		label := ""
		if m[6] >= 0 {
			label = s[m[6]:m[7]]
		}
		if node, ok := bracketNode(s[m[2]:m[3]], s[m[4]:m[5]], label); ok {
			out = append(out, node)
		} else {
			// Not one of Slack's forms: ordinary text that happens to sit
			// between angle brackets.
			out = append(out, parseEmphasis(s[m[0]:m[1]])...)
		}
		last = m[1]
	}
	out = append(out, parseEmphasis(s[last:])...)
	return out
}

func bracketNode(sigil, body, label string) (Node, bool) {
	switch sigil {
	case "@":
		return Node{Kind: KindUserRef, ID: body, Text: label}, true
	case "#":
		return Node{Kind: KindChannelRef, ID: body, Text: label}, true
	case "!":
		return Node{Kind: KindBroadcast, ID: body, Text: label}, true
	default:
		if !isLinkTarget(body) {
			return Node{}, false
		}
		return Node{Kind: KindLink, URL: body, Text: label}, true
	}
}

// isLinkTarget reports whether an unsigilled bracket body is a URL. Whitespace
// disqualifies it: Slack never sends a link containing any, and the text is
// far more likely to be prose that happens to sit between angle brackets.
func isLinkTarget(body string) bool {
	if strings.ContainsAny(body, " \t\n\r") {
		return false
	}
	return strings.HasPrefix(body, "http://") ||
		strings.HasPrefix(body, "https://") ||
		strings.HasPrefix(body, "mailto:")
}

func parseEmphasis(s string) []Node {
	if s == "" {
		return nil
	}
	runes := []rune(s)
	var out []Node
	var literal strings.Builder
	flush := func() {
		if literal.Len() > 0 {
			out = append(out, Node{Kind: KindText, Text: literal.String()})
			literal.Reset()
		}
	}
	for i := 0; i < len(runes); i++ {
		if style, ok := emphasisStyles[runes[i]]; ok && opensEmphasis(runes, i) {
			if j := closerIndex(runes, i); j > 0 {
				flush()
				out = append(out, Node{
					Kind:     KindEmphasis,
					Style:    style,
					Children: parseEmphasis(string(runes[i+1 : j])),
				})
				i = j
				continue
			}
		}
		literal.WriteRune(runes[i])
	}
	flush()
	return out
}

// opensEmphasis reports whether the marker at i can open a span. Slack only
// treats a marker as formatting when it follows a boundary and is followed by
// content, which is what keeps snake_case_name and 5*3 literal.
func opensEmphasis(runes []rune, i int) bool {
	if i > 0 && isWord(runes[i-1]) {
		return false
	}
	return i+1 < len(runes) && !unicode.IsSpace(runes[i+1])
}

// closerIndex finds the matching marker, or -1. A closer must follow content
// and must not be glued to a following word.
func closerIndex(runes []rune, open int) int {
	marker := runes[open]
	for j := open + 2; j < len(runes); j++ {
		if runes[j] != marker || unicode.IsSpace(runes[j-1]) {
			continue
		}
		if j+1 == len(runes) || !isWord(runes[j+1]) {
			return j
		}
	}
	return -1
}

func isWord(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r)
}

// BroadcastName reports the display text of a broadcast node and whether it is
// one of the three that address everyone.
//
// Both renderers need this distinction: @channel and @here are deliberately
// not translated into a real mention on the far platform, because someone
// writing @channel to three colleagues should not notify a customer's entire
// group.
func BroadcastName(n Node) (name string, addressesEveryone bool) {
	switch n.ID {
	case "here", "channel", "everyone":
		return n.ID, true
	}
	if n.Text != "" {
		return n.Text, false
	}
	return n.ID, false
}

// RefName is the display text of a user or channel reference: the label Slack
// supplied, else the bare id.
func RefName(n Node) string {
	if n.Text != "" {
		return n.Text
	}
	return n.ID
}
