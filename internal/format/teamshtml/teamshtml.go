// Package teamshtml converts Microsoft Teams message HTML into Slack mrkdwn.
package teamshtml

import (
	"fmt"
	"regexp"
	"strings"

	"golang.org/x/net/html"
)

// ToMrkdwn renders Teams message HTML as Slack mrkdwn. Unknown elements are
// unwrapped rather than dropped, so text is never silently lost.
func ToMrkdwn(in string) string {
	if strings.TrimSpace(in) == "" {
		return ""
	}
	doc, err := html.Parse(strings.NewReader(in))
	if err != nil {
		// html.Parse does fail on input, not only on the reader: it refuses a
		// document with more than 512 open elements, which is about 3 KiB
		// against a 4 MiB notification limit.
		//
		// The fallback must still escape. Returning the input verbatim
		// bypassed every guarantee this package exists to provide at once —
		// no conversion and no escaping — which handed the message's author
		// byte-level control of the Slack message text, including Slack's own
		// live control sequences. Text is kept, its power is not.
		return strings.TrimSpace(escape(stripTags(in)))
	}
	var sb strings.Builder
	walk(&sb, doc, &listState{})
	return strings.TrimSpace(collapseBlankLines(sb.String()))
}

// listState carries ordered-list numbering down the tree.
type listState struct {
	ordered bool
	index   int
}

func walk(sb *strings.Builder, n *html.Node, list *listState) {
	if n.Type == html.TextNode {
		sb.WriteString(escape(n.Data))
		return
	}
	if n.Type != html.ElementNode {
		walkChildren(sb, n, list)
		return
	}

	switch n.Data {
	case "strong", "b":
		wrap(sb, n, "*", list)
	case "em", "i":
		wrap(sb, n, "_", list)
	case "s", "strike", "del":
		wrap(sb, n, "~", list)
	case "code":
		wrap(sb, n, "`", list)
	case "pre":
		// Escaped like every other text node: Slack needs &, < and > to
		// arrive escaped inside code fences too, and code is exactly where
		// those characters turn up.
		sb.WriteString("\n```\n")
		sb.WriteString(escape(strings.Trim(textContent(n), "\n")))
		sb.WriteString("\n```\n")
	case "a":
		writeLink(sb, n, list)
	case "br":
		sb.WriteString("\n")
	case "p", "div":
		walkChildren(sb, n, list)
		sb.WriteString("\n")
	case "ul":
		sb.WriteString("\n")
		walkChildren(sb, n, &listState{ordered: false})
	case "ol":
		sb.WriteString("\n")
		walkChildren(sb, n, &listState{ordered: true})
	case "li":
		writeListItem(sb, n, list)
	case "img":
		// A message whose only content is an image used to convert to the
		// empty string, which Slack refuses to post — so the message failed
		// every attempt and was lost. Real image transfer is a later phase;
		// the placeholder keeps the fact that something was posted.
		writePlaceholder(sb, "image", attr(n, "alt"))
	case "emoji":
		if alt := attr(n, "alt"); alt != "" {
			sb.WriteString(":" + escape(alt) + ":")
			return
		}
		sb.WriteString("[emoji]")
	case "attachment":
		writePlaceholder(sb, "attachment", attr(n, "name"))
	case "script", "style", "template", "head", "noscript":
		// Markup machinery, not conversation. The parser hands their contents
		// over as text nodes, and unwrapping them printed stylesheets and
		// script bodies into the customer's channel.
		return
	default:
		walkChildren(sb, n, list)
	}
}

// writePlaceholder marks content that cannot cross yet, naming it when Teams
// gave it a name.
func writePlaceholder(sb *strings.Builder, kind, label string) {
	if label == "" {
		fmt.Fprintf(sb, "[%s]", kind)
		return
	}
	fmt.Fprintf(sb, "[%s: %s]", kind, escape(label))
}

func writeListItem(sb *strings.Builder, n *html.Node, list *listState) {
	list.index++
	if list.ordered {
		fmt.Fprintf(sb, "%d. ", list.index)
	} else {
		sb.WriteString("• ")
	}
	walkChildren(sb, n, list)
	sb.WriteString("\n")
}

func writeLink(sb *strings.Builder, n *html.Node, list *listState) {
	raw := attr(n, "href")
	text := strings.TrimSpace(textContent(n))

	safe, ok := sanitizeHref(raw)
	if !ok {
		// Not a target this converter will emit. The words are kept, the link
		// is not: an href it cannot vouch for must never reach Slack's own
		// bracket syntax.
		walkChildren(sb, n, list)
		return
	}
	if text == "" || text == raw {
		fmt.Fprintf(sb, "<%s>", safe)
		return
	}
	fmt.Fprintf(sb, "<%s|%s>", safe, escape(text))
}

// hrefEscaper prepares a URL for Slack's <target|label> syntax. `<` and `>`
// would close the link early, `|` would introduce a second target, and `&`
// has to arrive escaped like any other text.
var hrefEscaper = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", "|", "%7C")

// sanitizeHref reports whether an href may be emitted as a Slack link, and
// returns it in the form to emit.
//
// Only web and mail schemes are allowed. An unschemed href was previously
// interpolated verbatim, which let `!channel` or `@U012ABCDEF` become a real
// broadcast or mention — a Teams-side author pinging the whole Slack channel.
func sanitizeHref(href string) (string, bool) {
	lower := strings.ToLower(href)
	switch {
	case strings.HasPrefix(lower, "http://"),
		strings.HasPrefix(lower, "https://"),
		strings.HasPrefix(lower, "mailto:"):
		return hrefEscaper.Replace(href), true
	default:
		return "", false
	}
}

func wrap(sb *strings.Builder, n *html.Node, marker string, list *listState) {
	var inner strings.Builder
	walkChildren(&inner, n, list)
	body := inner.String()
	if strings.TrimSpace(body) == "" {
		return
	}
	sb.WriteString(marker)
	sb.WriteString(body)
	sb.WriteString(marker)
}

func walkChildren(sb *strings.Builder, n *html.Node, list *listState) {
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		walk(sb, c, list)
	}
}

func textContent(n *html.Node) string {
	var sb strings.Builder
	var rec func(*html.Node)
	rec = func(node *html.Node) {
		if node.Type == html.TextNode {
			sb.WriteString(node.Data)
		}
		for c := node.FirstChild; c != nil; c = c.NextSibling {
			rec(c)
		}
	}
	rec(n)
	return sb.String()
}

func attr(n *html.Node, name string) string {
	for _, a := range n.Attr {
		if a.Key == name {
			return a.Val
		}
	}
	return ""
}

// escape re-escapes the three characters Slack requires to arrive escaped.
func escape(s string) string {
	r := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;")
	return r.Replace(s)
}

// collapseBlankLines squeezes runs of blank lines produced by block elements.
func collapseBlankLines(s string) string {
	lines := strings.Split(s, "\n")
	out := make([]string, 0, len(lines))
	prevBlank := false
	for _, line := range lines {
		trimmed := strings.TrimRight(line, " \t")
		blank := trimmed == ""
		if blank && prevBlank {
			continue
		}
		out = append(out, trimmed)
		prevBlank = blank
	}
	return strings.Join(out, "\n")
}

// htmlTagPattern matches an HTML element tag — a name, then attributes or an
// immediate close. Used only on the unparseable fallback path, to drop markup
// structure while keeping prose.
//
// It deliberately does not match Slack's own bracket forms (`<!channel>`,
// `<@U012>`, `<https://x|label>`), whose first character or punctuation rules
// them out. Those survive stripping and are then escaped, so they arrive as
// the text they were rather than as live syntax.
var htmlTagPattern = regexp.MustCompile(`(?s)</?[a-zA-Z][a-zA-Z0-9-]*(\s[^>]*)?>`)

func stripTags(s string) string {
	return htmlTagPattern.ReplaceAllString(s, "")
}
