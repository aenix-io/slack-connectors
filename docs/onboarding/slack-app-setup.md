# Slack — app setup for the Teams ↔ Slack bridge

This is everything a Slack workspace administrator needs in order to let our
service mirror messages between a Slack channel and a Microsoft Teams channel.
It is deliberately narrow: the app sees only the channels you invite it to.

Please send back the three values listed under **What we need back**.

This is the Slack counterpart to [entra-id-request.md](entra-id-request.md),
which covers the Microsoft side.

---

## 1. Create the app

At https://api.slack.com/apps → **Create New App** → **From scratch**:

- **App Name:** `Teams–Slack Bridge` (any name is fine; it is what Slack shows
  as the poster of mirrored messages)
- **Workspace:** the workspace containing the channels to mirror

## 2. Add bot token scopes

**OAuth & Permissions** → **Scopes** → **Bot Token Scopes**. Add these four:

| Scope | Why it is needed |
|---|---|
| `chat:write` | Post the mirrored Teams messages into the Slack channel |
| `chat:write.customize` | Post under the author's own name and avatar, so it reads as a person speaking, not a bot announcement |
| `channels:history` | Receive new messages from the public channels in scope |
| `users:read` | Resolve display names, so a Slack message reaches Teams attributed to its author rather than to "Slack" |

Add `groups:history` as well if any channel in scope is **private**.

> `files:read` and `files:write` are needed only once attachment transfer ships
> in a later stage. If your policy prefers least privilege now and a second
> install later, the scopes above are sufficient to start.

## 3. Install the app and record the token

**OAuth & Permissions** → **Install to Workspace** → review and allow.

Record the **Bot User OAuth Token**. It starts with `xoxb-`.

## 4. Record the signing secret

**Basic Information** → **App Credentials** → **Signing Secret** → **Show**.

Record it. This is *not* the bot token and not the client secret. The service
uses it to verify that every incoming request genuinely came from Slack; with
the wrong value, every event is rejected as a forgery.

## 5. Enable event subscriptions

Tell us before doing this step: the request URL has to be answering already,
because Slack sends a verification challenge the moment you enter it and
refuses to save the URL until it gets the right answer back.

**Event Subscriptions** → toggle **Enable Events** on.

- **Request URL:** `https://<host>/webhook/slack` — we will give you the host

Then, under **Subscribe to bot events**, add:

| Event | Why it is needed |
|---|---|
| `message.channels` | New messages in the public channels in scope |
| `message.groups` | Same, for private channels — only if any channel in scope is private |

Save. Slack will prompt you to reinstall the app; do so.

## 6. Invite the bot to each channel

In every Slack channel that should be mirrored:

```
/invite @Teams–Slack Bridge
```

This step is easy to miss and fails **silently**: without it Slack delivers no
events for the channel and the service cannot post into it, with no error
shown anywhere on your side.

## 7. Name the channels in scope

For each Slack channel to mirror, we need its **channel ID** — not just its
name. Open the channel → click its name → the ID is at the bottom of the
dialog, and it starts with `C`.

Please also confirm who on your side should be told if the bridge stops
delivering.

---

## What we need back

1. Bot User OAuth Token (`xoxb-…`)
2. Signing Secret
3. Channel IDs in scope, and which Teams channel each should pair with

Send the token and the secret through whatever channel your security policy
prescribes for credentials — not by email, and not in a Slack message.

---

## What the service does and does not do

**Does:** receives new messages from the channels you invited it to; posts them
into the paired Teams channel, attributed to their author and threaded; posts
Teams replies back into the Slack channel.

**Does not:** read any other channel, any direct message, or any history from
before it was invited; store message text — the database holds only the
message identifiers needed to match the two sides; write message content to
logs.

Removing the app from the workspace revokes all access immediately.

---

## Where the values end up

For the operator deploying the service, the two credentials above are read
from the environment and belong in the Kubernetes Secret named by
`existingSecret` in `deploy/helm/values.yaml`:

| Value from this document | Environment variable |
|---|---|
| Bot User OAuth Token | `SLACK_BOT_TOKEN` |
| Signing Secret | `SLACK_SIGNING_SECRET` |

The channel IDs go into the `pairs` list in the same values file, as
`slackChannelId`.
