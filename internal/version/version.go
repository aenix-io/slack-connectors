// Package version carries build identity for logs and metrics.
package version

// Version is overridden at build time with -ldflags.
var Version = "dev"

// String renders the service identity.
func String() string {
	return "teams-slack-bridge " + Version
}
