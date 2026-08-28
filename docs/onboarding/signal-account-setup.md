# Signal bridge — setup and operations

Written for a person, not for a script. Everything here is done once, by hand,
and the parts that are done by hand are done by hand for a reason: Signal's
terms forbid creating accounts through automated means, and the account this
bridge runs as is a real Signal account with a real phone number.

## What the client has to be told first

Before a conversation is bridged, the client is told this in writing, and the
date they acknowledged it is recorded here:

- the bridge number is operated by us and is a member of the conversation;
- message content is decrypted in our infrastructure and copied into Slack;
- the Signal conversation is therefore protected end-to-end **up to** the
  bridge, not end-to-end between the client and the people reading it in Slack;
- disappearing messages set on the Signal side do not propagate to the Slack
  copy.

Approval is per client, not per connector. The first client approved this on
2026-08-06.

| | |
|---|---|
| Bridge number | _fill in_ |
| SIM held by | _fill in_ |
| Registration Lock PIN | 1Password, company vault |
| `age` backup key | 1Password, same vault |
| Second holder of both | _fill in_ |
| sidecar bump PR owner | Timur Tukaev |

## 1. Register the number

A real SIM or eSIM. Signal blocks many VoIP ranges, and a number that stops
working takes the conversation with it.

```bash
signal-cli -a "$NUMBER" register
# Signal usually asks for a CAPTCHA. Solve it at
# https://signalcaptchas.org/registration/generate.html, right-click the
# "Open Signal" link, copy it, and pass the token:
signal-cli -a "$NUMBER" register --captcha "$TOKEN"

signal-cli -a "$NUMBER" verify "$CODE"        # add --pin if a lock PIN is set
```

Then give the account a face, so participants see who they are talking to
rather than a bare number:

```bash
signal-cli -a "$NUMBER" updateProfile \
  --given-name "AENIX Bridge" \
  --about "Mirrors this conversation into a Slack channel" \
  --avatar bridge-avatar.png
```

## 2. Join the conversation

The client adds the bridge number to their Signal group, or starts a 1:1 with
it. Then read the group id back:

```bash
signal-cli -a "$NUMBER" --output=json listGroups | jq '.[] | {id, name}'
```

For a 1:1 pair, the recipient is the client's number or service id.
`listContacts --detailed` prints both.

## 3. Slack app

Private channel, then an app with:

- **Scopes:** `channels:history`, `groups:history`, `channels:read`,
  `groups:read`, `chat:write`, `chat:write.customize`, `files:read`,
  `files:write`, `users:read`, `reactions:read`, `reactions:write`
- **Events:** `message.channels`, `message.groups`, `reaction_added`,
  `reaction_removed`
- **Request URL:** `https://<host>/webhook/slack`

## 4. Configuration

```yaml
pairs:
  - id: acme-signal
    slack:
      channelId: C0BND5E608J
    signal:
      groupId: "Ci8KIExhbXBz"      # or recipient: "+491701234567"

users:
  - pairId: acme-signal
    slackUserId: U01ABCDEF
    signalUuid: 8f5ac0de-0000-4000-8000-000000000001
    displayName: Timur Tukaev
```

Mentions translate only for people listed under `users`. There is no directory
to discover this from — Signal knows a phone number and a service id, a Slack
workspace knows neither — and guessing would notify the wrong person at a
customer. Everyone unlisted still appears by name.

## Playbooks

### Signal is demanding a CAPTCHA

`SignalBridgeProofRequired` fired, or a delivery failed with a challenge token
in the log. Signal's anti-abuse system has decided this account looks like a
spammer. Nothing more gets through until a human clears it:

1. Take the challenge token from the log.
2. Solve the CAPTCHA at https://signalcaptchas.org/challenge/generate.html and
   copy the "Open Signal" link.
3. `signal-cli -a "$NUMBER" submitRateLimitChallenge --challenge "$TOKEN" --captcha "$CAPTCHA"`

Messages that failed during the block are not retried automatically. Check the
channel against the conversation before telling anyone it is caught up.

### The client is up to date but the bridge is silent

Check `bridge_signal_connected` and `bridge_signal_subscription_active`. A pod
that is Ready has both. If the socket is up and the subscription is not, the
bridge is connected to a daemon that is not delivering: restart the pod, which
re-subscribes.

Messages sent while it was down are **not** lost. The daemon runs with
`--receive-mode=manual`, so nothing is drained from the Signal server until the
bridge subscribes.

### The signal-cli client expired

`SignalCliNearlyExpired` was ignored for a fortnight. The Signal server now
refuses the client and the bridge is deaf. Merge the open bump PR — the weekly
workflow keeps one on hand — or bump `signalCli.tag` and `signalCli.releasedAt`
by hand and roll.

### Restoring the account volume

Losing the volume means re-registering, which shows every participant a
changed safety number — the warning that reads as a compromise to exactly the
kind of client who chose Signal. Restore rather than re-register.

From a snapshot: clone the `VolumeSnapshot` into a new PVC and point the
release at it.

From the weekly dump:

```bash
aws s3 cp "$DESTINATION/signal-slack-bridge/$STAMP.tar.gz.age" - \
  | age -d -i age-key.txt \
  | tar -C /var/lib -xz
```

The `age` key is in 1Password. Without it the archive is unreadable, which is
the point — and which is why the key has a named second holder.

### The quarterly restore drill

Once per quarter, restore the newest dump into a scratch namespace, start
signal-cli against it, and confirm `listGroups` returns the pilot group. The
drill has to include the decryption: a drill that skips it tests the wrong
half of the procedure.

## What this bridge does not carry

Stories, polls, payments, calls, stickers, view-once messages and contact
cards are dropped. Typing indicators and read receipts are not mirrored.
Reactions cross, but every mirrored reaction is placed by one identity, so
three people reacting the same way appear as one — and on the Signal side,
which holds one reaction per sender, the most recent one wins.

Disappearing-message timers apply to the Signal copy only. If a conversation
has one, `bridge_expiring_messages_observed_total` counts it and the log warns
— the Slack copy stays.
