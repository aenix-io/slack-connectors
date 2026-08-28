# Installing the Teams–Slack bridge (resource-specific consent)

The bridge reads messages through a Teams app installed into your team. The
installation itself grants the access — scoped to that one team. Nothing is
registered in your Entra ID, no tenant-wide permissions are requested, no
secrets are created on your side, and no Microsoft approval process is
involved.

## What your Teams administrator does (once)

1. **Upload our app package** to the org catalog: Teams admin center →
   **Teams apps → Manage apps → Upload new app**, using the zip we provide.
   (Alternatively, allow custom app upload for the pilot team and the team
   owner sideloads it.)
2. **Check that resource-specific consent is not blocked**: Teams admin
   center → org-wide app settings. RSC for team-scoped apps is on by default;
   if your tenant restricts it, the install in step 3 will say so explicitly.

## What the team owner does (per team)

3. In the team: **⋯ → Manage team → Apps → Add the "Teams–Slack Bridge"**.
   The consent screen lists the app's bot and one permission: *read this
   team's channel messages*. Accepting it is the grant; the bot is what posts
   Slack replies into the channel.

## What we need back

1. Your **Directory (tenant) ID** (Entra admin center → Overview — it is not
   a secret, it only tells our service which tenant to authenticate against).
2. **Team and channel names** to mirror, and who owns that team.
3. Who on your side should be told if the bridge stops delivering.

No client secrets: the service authenticates as our application; your side
holds no credentials for it.

## Scope and limitations

- Access is per team: channels of teams without the app are invisible to the
  service, and removing the app from the team revokes access immediately.
- **Standard channels only** for the pilot: Microsoft does not deliver
  change notifications for private and shared channels under resource-specific
  consent.
- The service does not store message text — the database holds only the
  identifiers needed to match the two sides — and does not write message
  content to logs.

## Replies from Slack into Teams

Slack replies are posted into the channel by the bot that ships inside this
app package, threaded under the message they answer. They appear under the
bot's name with the Slack author's name as the first line — Microsoft offers
no impersonation for applications.
