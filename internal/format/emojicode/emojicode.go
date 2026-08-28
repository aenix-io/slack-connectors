// Package emojicode translates between Slack's emoji shortcodes and the
// unicode characters Signal carries.
//
// Slack names a reaction ("thumbsup"); Signal sends the character ("👍").
// Neither will accept the other's form, so a reaction cannot cross without
// this. The table is the GitHub/Slack shortcode set, which is what Slack's
// own picker is built on.
package emojicode

import (
	"strings"

	"github.com/kyokomi/emoji/v2"
)

// variationSelector is the invisible codepoint clients append to make an
// emoji render in colour. It is part of what Signal sends and absent from
// most shortcode tables, so lookups try both forms rather than failing on a
// character nobody can see.
const variationSelector = "️"

// byUnicode maps a character to the shortest shortcode naming it. Shortest
// because the table holds aliases — "+1" and "thumbsup" name the same
// character — and the shorter one is the more common spelling.
var byUnicode = buildReverse()

func buildReverse() map[string]string {
	out := make(map[string]string)
	for code, char := range emoji.CodeMap() {
		name := strings.Trim(code, ":")
		char = strings.TrimSuffix(char, " ")
		existing, seen := out[char]
		if !seen || len(name) < len(existing) {
			out[char] = name
		}
	}
	return out
}

// ToSlackName returns the Slack reaction name for a unicode emoji, reporting
// false when the character has no shortcode we know.
func ToSlackName(char string) (string, bool) {
	if char == "" {
		return "", false
	}
	for _, candidate := range variants(char) {
		if name, ok := byUnicode[candidate]; ok {
			return name, true
		}
	}
	return "", false
}

// ToUnicode returns the character a Slack reaction name stands for, reporting
// false when it is a custom emoji — one uploaded to the workspace, which has
// no unicode form and therefore nothing to send.
func ToUnicode(name string) (string, bool) {
	if name == "" {
		return "", false
	}
	// A skin-tone variant arrives as "wave::skin-tone-3"; the base emoji is
	// closer to what the sender meant than nothing at all.
	if base, _, found := strings.Cut(name, "::"); found {
		name = base
	}
	char := emoji.CodeMap()[":"+strings.Trim(name, ":")+":"]
	if char == "" {
		return "", false
	}
	return strings.TrimSuffix(char, " "), true
}

// variants is the character as sent, and the same character with the
// variation selector added or removed.
func variants(char string) []string {
	if strings.HasSuffix(char, variationSelector) {
		return []string{char, strings.TrimSuffix(char, variationSelector)}
	}
	return []string{char, char + variationSelector}
}
