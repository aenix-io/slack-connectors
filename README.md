# Slack connectors

Bidirectional bridges between a Slack channel and a conversation on a platform
a counterparty will not leave. Two exist: Microsoft Teams and Signal. They
share a core, a store and the Slack adapter, and differ only in what the far
side can be made to do.

- **Teams ↔ Slack** — `cmd/bridge`, chart [`deploy/helm`](deploy/helm). The rest
  of this document.
- **Signal ↔ Slack** — `cmd/signal-bridge`, chart
  [`deploy/helm-signal`](deploy/helm-signal), setup and operations in
  [`docs/onboarding/signal-account-setup.md`](docs/onboarding/signal-account-setup.md).
  A dedicated Signal account, driven by signal-cli running as a sidecar, is a
  member of the customer's conversation. **A bridge terminates Signal's
  end-to-end guarantee** — plaintext exists in our cluster and is copied into
  Slack — which is a thing to say to a customer in writing before their
  conversation is bridged, and is written out in the design and the runbook.

## Teams–Slack Bridge

A bidirectional bridge between a Microsoft Teams channel and a Slack channel.

- **Teams → Slack**: channel messages are read through Microsoft Graph change
  notifications, using resource-specific consent (RSC) scoped to a single team.
- **Slack → Teams**: replies are posted back through the Bot Framework
  connector.

The service is hosted by us. To onboard a customer, the customer only grants
access in their own Microsoft tenant and sends us a few non-secret identifiers;
we then set those values in our deployment. No client secrets ever leave the
customer tenant.

- Deployment chart: [`deploy/helm`](deploy/helm)
- Teams app package: [`deploy/teams-app`](deploy/teams-app)
- Portal (click-through) onboarding: [`docs/onboarding`](docs/onboarding)

## Customer onboarding via CLI

Everything the customer must do can be driven from the Azure CLI. The commands
below are run **by an administrator of the customer tenant**, signed in to that
tenant. They install our Teams app into one team (the install itself is the RSC
grant — nothing is registered in the customer's Entra ID) and collect the three
identifiers we need back.

### Prerequisites

```bash
# Azure CLI, signed in to the CUSTOMER tenant as a Teams administrator.
az login --tenant <CUSTOMER_TENANT_DOMAIN_OR_ID>
```

We provide the Teams app package as `teams-slack-bridge.zip` (built from
`deploy/teams-app`, with our bot and Graph application IDs baked in).

### 1. Tenant ID (send this to us)

```bash
az account show --query tenantId --output tsv
```

Not a secret — it only tells our service which tenant to authenticate against.

### 2. Find the team and the standard channel to mirror (send both IDs to us)

```bash
# Teams the signed-in admin belongs to (add the target team's owner if needed):
az rest --method GET \
  --url "https://graph.microsoft.com/v1.0/me/joinedTeams?\$select=id,displayName"

# Channels of the chosen team — pick a STANDARD channel
# (private/shared channels are not supported by change notifications under RSC):
az rest --method GET \
  --url "https://graph.microsoft.com/v1.0/teams/<TEAM_ID>/channels?\$select=id,displayName,membershipType"
```

### 3. Upload our app package to the org catalog (admin, once)

```bash
TOKEN=$(az account get-access-token --resource https://graph.microsoft.com \
          --query accessToken --output tsv)

curl --request POST \
  "https://graph.microsoft.com/v1.0/appCatalogs/teamsApps" \
  --header "Authorization: Bearer $TOKEN" \
  --header "Content-Type: application/zip" \
  --data-binary @teams-slack-bridge.zip
```

### 4. Install the app into the target team (this is the access grant)

```bash
# Resolve the catalog id of our app by its manifest id:
az rest --method GET \
  --url "https://graph.microsoft.com/v1.0/appCatalogs/teamsApps?\$filter=externalId eq 'bb722207-7b0d-4247-bb47-c90e866b75b0'&\$select=id,displayName"

# Install it into the team — accepting the RSC permission
# (read this team's channel messages):
az rest --method POST \
  --url "https://graph.microsoft.com/v1.0/teams/<TEAM_ID>/installedApps" \
  --headers "Content-Type=application/json" \
  --body '{"teamsApp@odata.bind":"https://graph.microsoft.com/v1.0/appCatalogs/teamsApps/<CATALOG_APP_ID>"}'
```

Removing the app from the team (`DELETE .../installedApps/<id>`) revokes access
immediately.

### 5. Send us

1. **Tenant ID** (step 1).
2. **Team ID** and **channel ID** of the standard channel to mirror (step 2).

That is all — the service authenticates as our own application; the customer
holds no credentials for it.

### Troubleshooting: Teams messages never arrive

In rare tenants, Graph delivers change notifications with a `null` validation
token and the service drops them silently. Fix on the customer side:

```bash
# Ensure the "Microsoft Graph Change Tracking" service principal does not
# require assignment (appId 0bf30f3b-4a52-48df-9a82-234910c4a086):
az ad sp update --id 0bf30f3b-4a52-48df-9a82-234910c4a086 \
  --set appRoleAssignmentRequired=false
```

## Scope and limitations

- Access is per team: channels of teams without the app installed are invisible
  to the service; removing the app revokes access at once.
- **Standard channels only** for the pilot — Microsoft does not deliver change
  notifications for private and shared channels under RSC.
- The service does not store message text (only the identifiers needed to match
  the two sides) and does not write message content to logs. The Signal
  connector keeps that property with one documented exception: signal-cli
  writes received files to its own volume, and the bridge deletes each one
  after transferring it.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
