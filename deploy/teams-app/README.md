# Teams app package (RSC)

The bridge reads a team's channels through resource-specific consent: the
customer installs this Teams app into the team, and that installation IS the
grant. No Entra app registration on the customer side, no tenant-wide admin
consent, no Microsoft protected-API approval.

## Building the package

1. Substitute our multi-tenant Entra application id:

   ```sh
   AZURE_CLIENT_ID=<graph app id> BOT_APP_ID=<bot id> \
     envsubst < manifest.json > build/manifest.json
   ```

2. `color.png` (192×192) and `outline.png` (32×32) ship in this directory —
   the Æ ligature on the manifest's accent color. Replace with final design
   assets whenever they arrive; an app update, not a re-install.

3. Zip the three files flat (no directory inside):

   ```sh
   cd build && zip ../teams-slack-bridge.zip manifest.json color.png outline.png
   ```

## Notes

- `webApplicationInfo.id` ties the installation to our Entra app: installing
  the package creates the service principal in the customer tenant and grants
  `ChannelMessage.Read.Group` for that team only.
- The `bots` entry is how Slack replies reach Teams: app-only Graph cannot
  post channel messages, so sending goes through the bot. `isNotificationOnly`
  keeps users from messaging it. The bot must also exist as an Azure Bot
  resource bound to the same application id — see
  `docs/onboarding/azure-bot-setup.md`.
- Private and shared channels do not deliver change notifications under RSC
  (tenant-wide protected permission required); the pilot scopes to standard
  channels.
