package slackmrkdwn

import "testing"

func TestToHTML(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{name: "plain text", in: "hello", want: "hello"},
		{name: "bold", in: "*bold*", want: "<strong>bold</strong>"},
		{name: "italic", in: "_italic_", want: "<em>italic</em>"},
		{name: "strikethrough", in: "~strike~", want: "<strike>strike</strike>"},
		{name: "inline code", in: "`x := 1`", want: "<code>x := 1</code>"},
		{name: "code block spanning lines", in: "```line1\nline2```", want: "<pre><code>line1\nline2</code></pre>"},
		{name: "labelled link", in: "<https://example.com|docs>", want: `<a href="https://example.com">docs</a>`},
		{name: "bare link", in: "<https://example.com>", want: `<a href="https://example.com">https://example.com</a>`},
		{name: "newline becomes br", in: "one\ntwo", want: "one<br>two"},
		{name: "two emphases in one line", in: "*bold* and _italic_", want: "<strong>bold</strong> and <em>italic</em>"},
		{name: "emphasis inside code span stays literal", in: "`*not bold*`", want: "<code>*not bold*</code>"},
		{name: "link inside code span stays literal", in: "`<https://example.com>`", want: "<code>&lt;https://example.com&gt;</code>"},
		{name: "escaped ampersand passes through unchanged", in: "a &amp; b", want: "a &amp; b"},
		{name: "escaped angle bracket passes through unchanged", in: "a &lt; b", want: "a &lt; b"},
		{name: "snake_case_name stays literal", in: "snake_case_name", want: "snake_case_name"},
		{name: "unmatched asterisk stays literal", in: "a * b * c", want: "a * b * c"},
		{name: "empty input", in: "", want: ""},

		// Additional cases discovered while implementing.
		{name: "code block trims single surrounding newlines", in: "```\nline1\nline2\n```", want: "<pre><code>line1\nline2</code></pre>"},
		{name: "link with ampersand in url is escaped in href", in: "<https://example.com?a=1&b=2|docs>", want: `<a href="https://example.com?a=1&amp;b=2">docs</a>`},
		{name: "asterisk adjacent to word char does not open emphasis", in: "5*3=15", want: "5*3=15"},
		{name: "stray unmatched angle bracket is escaped not left raw", in: "a < b", want: "a &lt; b"},

		// Slack's other angle-bracket forms. Left as raw text they read as
		// markup noise in the customer's Teams channel.
		{name: "user mention without a label falls back to the id", in: "<@U012ABCDEF> hi", want: "@U012ABCDEF hi"},
		{name: "user mention uses its label when Slack supplies one", in: "<@U012ABCDEF|ada> hi", want: "@ada hi"},
		{name: "channel reference uses its name", in: "see <#C012AB|general>", want: "see #general"},
		{name: "channel reference without a label falls back to the id", in: "see <#C012AB>", want: "see #C012AB"},
		{name: "here broadcast", in: "<!here> ping", want: "@here ping"},
		{name: "channel broadcast", in: "<!channel> ping", want: "@channel ping"},
		{name: "everyone broadcast", in: "<!everyone> ping", want: "@everyone ping"},
		{name: "user group uses its label verbatim", in: "<!subteam^S012|@devs> ping", want: "@devs ping"},
		{name: "date renders its fallback text verbatim", in: "<!date^1700000000^{date_short}|Nov 14, 2023>", want: "Nov 14, 2023"},
		{name: "mention inside a code span stays literal", in: "`<@U012ABCDEF>`", want: "<code>&lt;@U012ABCDEF&gt;</code>"},
		{name: "mention label passes ampersands through like any other text", in: "<@U1|Ada & Co>", want: "@Ada & Co"},
		// A label carrying angle brackets cannot parse as a mention at all,
		// so every bracket ends up escaped. What matters is that no raw
		// markup reaches the Teams body.
		{name: "bracket in a mention label yields no raw markup", in: "<@U1|<img src=x>>", want: "&lt;@U1|&lt;img src=x&gt;&gt;"},
		{name: "emphasis still applies around a mention", in: "*hi* <@U1>", want: "<strong>hi</strong> @U1"},
		{name: "unknown bracket form stays literal", in: "<not a link>", want: "&lt;not a link&gt;"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ToHTML(tc.in); got != tc.want {
				t.Fatalf("ToHTML(%q)\n got: %q\nwant: %q", tc.in, got, tc.want)
			}
		})
	}
}
