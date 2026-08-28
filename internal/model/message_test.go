package model

import "testing"

func TestIsReply(t *testing.T) {
	cases := []struct {
		name     string
		parentID string
		want     bool
	}{
		{name: "thread root", parentID: "", want: false},
		{name: "reply", parentID: "1565045424600", want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := Message{ParentID: tc.parentID}
			if got := m.IsReply(); got != tc.want {
				t.Fatalf("IsReply() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPlatformValid(t *testing.T) {
	cases := []struct {
		name string
		p    Platform
		want bool
	}{
		{name: "teams", p: PlatformTeams, want: true},
		{name: "slack", p: PlatformSlack, want: true},
		{name: "signal", p: PlatformSignal, want: true},
		{name: "zero value", p: Platform(""), want: false},
		{name: "unknown platform", p: Platform("discord"), want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.p.Valid(); got != tc.want {
				t.Fatalf("Valid() = %v, want %v", got, tc.want)
			}
		})
	}
}
