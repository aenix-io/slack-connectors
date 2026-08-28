package version

import "testing"

func TestStringIncludesVersion(t *testing.T) {
	original := Version
	t.Cleanup(func() { Version = original })

	Version = "1.2.3"
	got := String()
	if got != "teams-slack-bridge 1.2.3" {
		t.Fatalf("got %q, want %q", got, "teams-slack-bridge 1.2.3")
	}
}
