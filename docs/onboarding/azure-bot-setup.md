# Bot registration — one-time setup on OUR side

Posting Slack replies into Teams goes through the Bot Framework connector,
and the connector only talks to registered bots. This is our infrastructure,
done once; customers never see it and need no Azure anywhere.

## Primary path: Teams Developer Portal (no Azure, no card)

1. Sign in to https://dev.teams.microsoft.com with our Microsoft work
   account → **Tools → Bot management → + New Bot**.
2. Record the **Bot ID** the portal mints. It is a separate identity from
   the Graph application: the service therefore runs with two credential
   pairs — `AZURE_CLIENT_ID`/`AZURE_CLIENT_SECRET` for reading through
   Graph, `BOT_APP_ID`/`BOT_APP_SECRET` for posting through the connector.
3. Inside the bot → **Client secrets → Add a secret**; it is shown once.
4. The endpoint address can stay empty (or any HTTPS URL): the manifest
   marks the bot `isNotificationOnly`, users cannot message it, and the
   bridge receives nothing over Bot Framework. A "Failed to load bot event
   subscription" toast in the portal is cosmetic — that section simply has
   nothing to load for a fresh bot.

`BOT_APP_ID` goes into the app package (`bots[].botId`, see
`deploy/teams-app/README.md`) and, with the secret, into the deployment.

## Alternative: Azure Bot resource

An Azure subscription (free, card for identity check) allows registering
the bot on the existing Graph application id instead — one identity for
everything. Azure portal → **Create → Azure Bot**: pricing tier F0,
*Multi Tenant*, *Use existing app registration*. Channel messages to Teams
are free on either tier; S1 exists for premium channels the bridge does not
use. Then set `BOT_APP_ID`/`BOT_APP_SECRET` to the same values as the Graph
pair.

## Enable the Teams channel

Either way, the registration must have the **Microsoft Teams channel**
enabled (Developer Portal does this by default for bots it creates; in
Azure it is the Channels blade). Without it the connector answers 403 for
every Teams conversation.

## How posting works after this

The Teams app package carries the bot id in its `bots` section. When a team
owner installs the app, the bot becomes a member of the team's standard
channels — that membership is what lets it post. `BotNotInConversationRoster`
in the logs therefore means exactly one thing: the app is not installed in
that team. Loop prevention does not depend on the bot's identity: the bridge
recognises its own posts by the recorded delivery result.
