# Personal notifications and persistent gateway login

Open **Notifications** from the navigation. Each signed-in user can add any
number of browser, Discord, Slack, or generic HTTPS webhook channels. Channels
can target every project (including future projects) or selected project IDs,
and success, failure, or cancellation outcomes. A channel can be paused, edited,
tested, or deleted. Changing its type requires adding a new channel.

Browser channels belong to the browser where they were created. Grant permission
when saving an enabled browser channel. Deployer checks for alerts every 15
seconds while a page is open. Background push while Deployer is closed is not
implemented. Multiple tabs coordinate polling where the browser supports Web
Locks. Click an alert to open its build. Delivery history records acknowledgement
by the browser, which does not guarantee the operating system displayed a banner.

For Discord and Slack, paste an existing incoming webhook URL. URLs are encrypted
at rest and never returned by list or history endpoints. On edit, leaving the URL
blank preserves it. Discord uses status-colored embeds with project, build,
revision, duration, trigger, completion time, and a clickable title. Success is
green, failure red, cancellation amber, and connection tests blue. Slack uses a
heading, compact detail fields, and a build link. Discord mentions are disabled;
Slack project values are plain text so they cannot trigger mentions or formatting.
See the official [Discord webhook documentation](https://docs.discord.com/developers/resources/webhook)
and [Slack webhook setup](https://docs.slack.dev/messaging/sending-messages-using-incoming-webhooks/).
Generic webhooks receive JSON with project/build IDs, name, revision, outcome,
trigger, duration, completion time, and a build URL. Logs, build arguments, and hook contents are
excluded. Internal/private destinations and redirects are rejected.

The PostgreSQL queue survives server restarts. Terminal build routing is marked
only after its deliveries are committed, and each channel/build pair is unique.
Existing imported history is marked processed so migration does not send old
alerts. Routing uses settings in effect when the worker processes the final
build, normally within five seconds. Network errors, HTTP 408/429, and server
errors are retried with backoff, up to five attempts; other client errors are
terminal. Delivery status and safe errors are visible in the last 50 entries per
channel. History is retained for 30 days. A delivery error never changes a build
outcome. Delivery is at least once: a crash after sending but before recording
the result can cause a duplicate. Generic receivers can deduplicate using the
stable `Idempotency-Key` request header.

## Server configuration

Set `DEPLOYER_NOTIFICATION_KEY` to the base64 encoding of 32 random bytes. Keep
the key across restarts and include it in protected configuration backups;
restoring the database without the key makes encrypted endpoints unusable.
Set `DEPLOYER_PUBLIC_URL` so messages include useful external build links.

The authorization gateway must overwrite `X-Deployer-User` with the stable OIDC
subject after successful authentication. Only requests from a loopback proxy
may use this assertion. Do not expose the upstream application to untrusted
networks. Machine bearer credentials cannot manage personal channels, inboxes,
or acknowledgements. Existing deployment authorization remains with the gateway;
this feature does not introduce project access restrictions or new admin roles.

For Apache mod_auth_openidc, configure the Deployer virtual host with:

```apache
OIDCSessionType server-cache:persistent
OIDCSessionInactivityTimeout 2592000
OIDCSessionMaxDuration 2592000
OIDCCacheType file
OIDCCacheDir /var/cache/apache2/deployer-oidc
OIDCCacheEncrypt On
OIDCRemoteUserClaim sub
RequestHeader unset X-Deployer-User early
<Location />
    AuthType openid-connect
    Require valid-user
    RequestHeader set X-Deployer-User expr=%{REMOTE_USER}
</Location>
```

Create the cache directory with mode 0700 owned by Apache's worker user. This
keeps sessions in an encrypted, persistent server-side cache and sets a 30-day
absolute duration and inactivity window. Keep the OIDC crypto passphrase stable.
An existing session may need one new login after changing cache storage.
See the upstream [session and cache configuration reference](https://github.com/OpenIDC/mod_auth_openidc/blob/master/auth_openidc.conf).

Set `DEPLOYER_LOGOUT_URL` to the gateway callback's `logout` URL, redirecting to
`/signed-out`. The gateway must allow `/signed-out` without authentication. Use
the gateway's normal logout so the cached device session is invalidated. Cache
administration can revoke Deployer sessions without deleting application data.
Provider account/SSO policy can still require earlier reauthentication.

Machine API traffic needs a separate gateway path that skips interactive OIDC
only when a syntactically valid bearer header is present. Deployer then verifies
the credential and scope and rejects invalid bearers. Do not bypass OIDC for
ordinary UI/API browser traffic. Runner paths continue using their agent tokens.

## API

- `GET/POST /api/notifications/channels`: list or create the current user's channels.
- `PUT/DELETE /api/notifications/channels/:id`: edit or delete an owned channel.
- `POST /api/notifications/channels/:id/test`: queue a test for an enabled channel.
- `GET /api/notifications/channels/:id/deliveries`: bounded delivery history.
- `GET /api/notifications/inbox?browser_id=...`: pending alerts for this device.
- `POST /api/notifications/ack`: acknowledge an owned browser alert (`id`, `browser_id`).

Browser mutations require `X-Deployer-CSRF: 1`. Webhook URL fields are write-only.
