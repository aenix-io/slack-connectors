package emojicode

import "testing"

func TestToSlackName(t *testing.T) {
	cases := []struct {
		name string
		char string
		want string
		ok   bool
	}{
		{name: "thumbs up", char: "👍", want: "+1", ok: true},
		{name: "party", char: "🎉", want: "tada", ok: true},
		{name: "rocket", char: "🚀", want: "rocket", ok: true},
		// Signal sends the variation selector; shortcode tables usually do not
		// carry it, and the difference is invisible to a human.
		{name: "with variation selector", char: "❤️", want: "heart", ok: true},
		{name: "not an emoji", char: "x", ok: false},
		{name: "empty", char: "", ok: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ToSlackName(tc.char)
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v (got %q)", ok, tc.ok, got)
			}
			if ok && got != tc.want {
				t.Fatalf("ToSlackName(%q) = %q, want %q", tc.char, got, tc.want)
			}
		})
	}
}

func TestToUnicode(t *testing.T) {
	cases := []struct {
		name string
		code string
		want string
		ok   bool
	}{
		{name: "thumbsup", code: "thumbsup", want: "👍", ok: true},
		{name: "with colons", code: ":tada:", want: "🎉", ok: true},
		{name: "skin tone falls back to the base", code: "wave::skin-tone-3", want: "👋", ok: true},
		// A custom emoji is uploaded to one workspace and has no unicode at
		// all, so there is nothing to send to Signal.
		{name: "custom emoji", code: "aenix-logo", ok: false},
		{name: "empty", code: "", ok: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ToUnicode(tc.code)
			if ok != tc.ok {
				t.Fatalf("ok = %v, want %v (got %q)", ok, tc.ok, got)
			}
			if ok && got != tc.want {
				t.Fatalf("ToUnicode(%q) = %q, want %q", tc.code, got, tc.want)
			}
		})
	}
}

// A reaction that crosses and comes back must still be the same reaction:
// otherwise the loop-prevention record, which keys on the emoji, would not
// match and the bridge would mirror its own reaction back.
func TestRoundTrip(t *testing.T) {
	for _, char := range []string{"👍", "🎉", "🚀", "👀", "✅"} {
		t.Run(char, func(t *testing.T) {
			name, ok := ToSlackName(char)
			if !ok {
				t.Fatalf("ToSlackName(%q) found no shortcode", char)
			}
			back, ok := ToUnicode(name)
			if !ok {
				t.Fatalf("ToUnicode(%q) found no character", name)
			}
			if back != char {
				t.Fatalf("round trip = %q, want %q", back, char)
			}
		})
	}
}
