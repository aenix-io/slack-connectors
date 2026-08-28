package teamshtml

import (
	"strings"
	"testing"
)

func TestToMrkdwn(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{name: "plain text", in: "<p>hello</p>", want: "hello"},
		{name: "bold strong", in: "<p><strong>bold</strong></p>", want: "*bold*"},
		{name: "bold b", in: "<p><b>bold</b></p>", want: "*bold*"},
		{name: "italic em", in: "<p><em>it</em></p>", want: "_it_"},
		{name: "italic i", in: "<p><i>it</i></p>", want: "_it_"},
		{name: "strike s", in: "<p><s>gone</s></p>", want: "~gone~"},
		{name: "strike strike", in: "<p><strike>gone</strike></p>", want: "~gone~"},
		{name: "inline code", in: "<p><code>x := 1</code></p>", want: "`x := 1`"},
		{name: "code block", in: "<pre><code>line1\nline2</code></pre>", want: "```\nline1\nline2\n```"},
		{name: "link", in: `<p><a href="https://example.com">docs</a></p>`, want: "<https://example.com|docs>"},
		{name: "link without text", in: `<p><a href="https://example.com"></a></p>`, want: "<https://example.com>"},
		{name: "line break", in: "<p>one<br>two</p>", want: "one\ntwo"},
		{name: "two paragraphs", in: "<p>one</p><p>two</p>", want: "one\ntwo"},
		{name: "unordered list", in: "<ul><li>a</li><li>b</li></ul>", want: "• a\n• b"},
		{name: "ordered list", in: "<ol><li>a</li><li>b</li></ol>", want: "1. a\n2. b"},
		{name: "nested formatting", in: "<p><strong>very <em>bold</em></strong></p>", want: "*very _bold_*"},
		{name: "escapes ampersand", in: "<p>a &amp; b</p>", want: "a &amp; b"},
		{name: "escapes angle brackets", in: "<p>a &lt; b</p>", want: "a &lt; b"},
		{name: "strips unknown tags", in: "<p><span>text</span></p>", want: "text"},
		{name: "empty input", in: "", want: ""},
		{name: "collapses trailing whitespace", in: "<p>text   </p>", want: "text"},
		{name: "code block escapes markup", in: "<pre><code>if a &lt; b &amp;&amp; c</code></pre>", want: "```\nif a &lt; b &amp;&amp; c\n```"},
		{name: "link text is flattened", in: `<p><a href="https://example.com"><strong>docs</strong></a></p>`, want: "<https://example.com|docs>"},
		{name: "ordered list numbers siblings", in: "<ol><li>a</li><li>b</li><li>c</li></ol>", want: "1. a\n2. b\n3. c"},
		{name: "second ordered list restarts", in: "<ol><li>a</li><li>b</li></ol><ol><li>c</li><li>d</li></ol>", want: "1. a\n2. b\n\n1. c\n2. d"},
		{name: "nested ordered list numbers independently", in: "<ol><li>one<ol><li>a</li><li>b</li></ol></li><li>two</li></ol>", want: "1. one\n1. a\n2. b\n\n2. two"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ToMrkdwn(tc.in); got != tc.want {
				t.Fatalf("ToMrkdwn(%q)\n got: %q\nwant: %q", tc.in, got, tc.want)
			}
		})
	}
}

// Teams messages that carry no prose — a pasted screenshot, a lone emoji, a
// file — converted to the empty string, which chat.postMessage rejects. The
// message then failed every attempt forever and was eventually lost. Real
// transfer of images and files is a later phase; until then a placeholder
// keeps the fact that something was posted, which is the part that must not
// vanish.
func TestToMrkdwnNeverLosesNonTextContent(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "image alone becomes a placeholder",
			in:   `<p><img src="https://x/i.png" itemid="1"></p>`,
			want: "[image]",
		},
		{
			name: "image uses its alt text when Teams supplies one",
			in:   `<p><img src="https://x/i.png" alt="Q3 revenue chart"></p>`,
			want: "[image: Q3 revenue chart]",
		},
		{
			name: "two images each get a placeholder",
			in:   `<div><img src="a"></div><div><img src="b"></div>`,
			want: "[image]\n[image]",
		},
		{
			name: "emoji alone becomes its alt text",
			in:   `<p><emoji alt="smile"></emoji></p>`,
			want: ":smile:",
		},
		{
			name: "emoji without alt still leaves a mark",
			in:   `<p><emoji></emoji></p>`,
			want: "[emoji]",
		},
		{
			name: "attachment becomes a placeholder",
			in:   `<attachment id="1"></attachment>`,
			want: "[attachment]",
		},
		{
			name: "an image among words keeps both",
			in:   `<p>see <img src="a" alt="this"> here</p>`,
			want: "see [image: this] here",
		},
		{
			name: "alt text is escaped like any other text",
			in:   `<p><img src="a" alt="a & b <c>"></p>`,
			want: "[image: a &amp; b &lt;c&gt;]",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ToMrkdwn(tc.in); got != tc.want {
				t.Fatalf("ToMrkdwn(%q)\n got: %q\nwant: %q", tc.in, got, tc.want)
			}
		})
	}
}

// Script and style content is markup machinery, not conversation. The parser
// puts it in text nodes, and unwrapping unknown elements printed it into the
// customer's Slack channel.
func TestToMrkdwnDropsNonProseElements(t *testing.T) {
	cases := []struct{ name, in, want string }{
		{name: "script", in: `<p>before</p><script>var s = "leaked"</script><p>after</p>`, want: "before\nafter"},
		{name: "style", in: `<style>.a{color:red}</style><p>body</p>`, want: "body"},
		{name: "title", in: `<title>title text</title><p>body</p>`, want: "body"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ToMrkdwn(tc.in); got != tc.want {
				t.Fatalf("ToMrkdwn(%q)\n got: %q\nwant: %q", tc.in, got, tc.want)
			}
		})
	}
}

// x/net/html refuses a document nesting more than 512 open elements — about
// 3 KiB, well inside the 4 MiB notification limit. The fallback returned the
// input verbatim, so on that path no tag was converted and nothing was
// escaped: a Teams-side author controlled the Slack message text byte for
// byte, including Slack's own live control sequences.
func TestToMrkdwnParseFailureIsSanitised(t *testing.T) {
	const depth = 600
	var sb strings.Builder
	for i := 0; i < depth; i++ {
		sb.WriteString("<div>")
	}
	sb.WriteString(`<!channel> and <@U012ABCDEF> and <https://evil|click>`)
	for i := 0; i < depth; i++ {
		sb.WriteString("</div>")
	}
	in := sb.String()

	got := ToMrkdwn(in)

	// Whatever this path produces, it must not hand Slack live syntax.
	for _, forbidden := range []string{"<!channel>", "<@U012ABCDEF>", "<https://evil|click>"} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("output carries live Slack syntax %q:\n%s", forbidden, got)
		}
	}
	if strings.Contains(got, "<div>") {
		t.Fatalf("output carries raw unconverted HTML:\n%s", got)
	}
	// The words themselves should survive; only their power should not.
	if !strings.Contains(got, "channel") {
		t.Fatalf("output lost the message text entirely:\n%s", got)
	}
}

// An href was interpolated into Slack's <...> link syntax unescaped, while the
// label was escaped. Slack reads that syntax, so an href could become a real
// @-mention of the whole channel or of a chosen person, or close the link early
// and append arbitrary mrkdwn.
func TestToMrkdwnLinkHrefCannotInjectSlackSyntax(t *testing.T) {
	cases := []struct{ name, in string }{
		{name: "channel broadcast", in: `<a href="!channel"></a>`},
		{name: "here broadcast", in: `<a href="!here">!here</a>`},
		{name: "user mention", in: `<a href="@U012ABCDEF"></a>`},
		{name: "user mention with label", in: `<a href="@U012ABCDEF">hi</a>`},
		{name: "pipe smuggles a second target", in: `<a href="https://good|https://evil">click</a>`},
		{name: "bracket closes the link early", in: `<a href="https://x&gt; and now &lt;!channel">t</a>`},
		{name: "javascript scheme", in: `<a href="javascript:alert(1)">x</a>`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ToMrkdwn(tc.in)
			for _, forbidden := range []string{"<!channel>", "<!here>", "<@U012ABCDEF>", "<@U012ABCDEF|hi>"} {
				if strings.Contains(got, forbidden) {
					t.Fatalf("ToMrkdwn(%q) = %q, which contains live Slack syntax %q",
						tc.in, got, forbidden)
				}
			}
			// A link this converter emits must carry exactly one target.
			if strings.Count(got, "|") > 1 {
				t.Fatalf("ToMrkdwn(%q) = %q, which smuggles more than one link target", tc.in, got)
			}
		})
	}
}

// The ordinary case must keep working: a real URL still becomes a real link.
func TestToMrkdwnKeepsValidLinks(t *testing.T) {
	cases := []struct{ name, in, want string }{
		{name: "labelled", in: `<a href="https://example.com">docs</a>`, want: "<https://example.com|docs>"},
		{name: "bare", in: `<a href="https://example.com">https://example.com</a>`, want: "<https://example.com>"},
		{name: "mailto", in: `<a href="mailto:a@b.c">mail</a>`, want: "<mailto:a@b.c|mail>"},
		{name: "query string with an ampersand", in: `<a href="https://x/?a=1&amp;b=2">q</a>`, want: "<https://x/?a=1&amp;b=2|q>"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ToMrkdwn(tc.in); got != tc.want {
				t.Fatalf("ToMrkdwn(%q)\n got: %q\nwant: %q", tc.in, got, tc.want)
			}
		})
	}
}
