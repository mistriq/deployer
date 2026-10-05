# App identity and PWA support

The header uses an SVG version of the approved D/arrow logo. Editable SVG
sources, browser icons, Apple touch icons, install icons, and a 1200 × 630
social card live in `internal/app/web/static/brand/`.

The shared page head includes favicon, Open Graph/Twitter image metadata,
theme colors, and `/manifest.webmanifest`. Set `DEPLOYER_PUBLIC_URL` to the
external HTTPS origin so social image URLs are absolute. Link-preview crawlers
cannot see metadata behind the authorization gateway without access; these tags
do not change gateway access rules.

The manifest provides 192/512 px app icons, a separate maskable icon, root scope,
and standalone display. On HTTPS (or localhost), `/static/pwa.js` registers the
root `/service-worker.js`. Installation uses the browser's native install menu;
on iOS, use Share → Add to Home Screen. The gateway must preserve the same-origin
manifest, worker, and icon routes for authenticated users. Manifest requests use
credentials. Verify installation through the real gateway before release.

For the Apache OIDC gateway, allow these public, non-personal assets inside the
Deployer virtual host. This keeps login redirects out of install resources and
allows the public signed-out page to load its logo metadata. Keep application
pages and APIs under their existing authorization rules:

```apache
<LocationMatch "^/(manifest\\.webmanifest|service-worker\\.js|favicon\\.ico|static/offline\\.html|static/brand/[a-z0-9-]+\\.(svg|png|ico))$">
    AuthType None
    Require all granted
</LocationMatch>
```

The worker caches only the generic, self-contained offline page. Failed page
navigations show a reconnect message; project pages, credentials, API responses,
logs, SSE streams, and write requests are never stored by the worker. There is
no offline deploy queue or background notification permission request. Worker
updates use the browser's normal lifecycle. Increment its cache version when
changing the offline page.

See [MDN's installability requirements](https://developer.mozilla.org/en-US/docs/Web/Progressive_web_apps/Guides/Making_PWAs_installable)
for browser and secure-origin requirements. Run `node scripts/test-pwa-worker.mjs`
to check offline fallback, gateway redirects, and API/mutation bypass behavior.

Static assets use content ETags and private revalidation: a changed binary
supplies new files, while repeat requests receive an empty 304 response. Live
data requests keep their normal network behavior.

The project dashboard uses three database queries: projects, runners, and
bounded build metadata. The project API uses two. Build logs are loaded only by
build detail requests. Migration `007_project_build_history.sql` adds the
`builds(project_id, id DESC)` index used by PostgreSQL's limited per-project
lookups; the SQLite test schema has the same index.
