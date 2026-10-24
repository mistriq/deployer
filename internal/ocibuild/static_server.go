package ocibuild

import "encoding/base64"

// How a generated static image serves its files. nginx's stock configuration answers 404 for any path that
// is not a file, which breaks single-page apps (React Router, Vue Router…, so every app made with Lovable, Bolt
// or v0): /dashboard works when clicked to, but not when reloaded or opened from a link. The configuration is
// written in the build stage from the built files:
//   - a site with its own 404.html at the top (Astro, Hugo, Jekyll, Next export…) keeps real 404s and shows
//     that page for them;
//   - otherwise every path that is not a file gets index.html, as single-page app hosts do.
//
// Paths ending in a file extension (/missing.js, /logo.png) are always a plain 404, so a missing asset is never
// answered with HTML. Redirects stay relative, so they don't leak the container's port 8080 behind the proxy.
const staticServerScript = `#!/bin/sh
set -e
dir="$1"
if [ -f "$dir/404.html" ]; then
  fallback="=404"
  errors="error_page 404 /404.html;"
elif [ -f "$dir/index.html" ]; then
  fallback="/index.html"
  errors=""
else
  fallback="=404"
  errors=""
fi
cat <<CONF
server {
    listen 8080;
    server_name _;
    root /usr/share/nginx/html;
    index index.html index.htm;
    absolute_redirect off;
    $errors
    location / {
        try_files \$uri \$uri/ \$uri.html $fallback;
    }
    location ~ \.[A-Za-z0-9]+\$ {
        try_files \$uri =404;
    }
}
CONF
`

// staticServerStep writes /tmp/molo-nginx.conf in the build stage; the nginx stage copies it into place.
func staticServerStep(dir string) string {
	script := base64.StdEncoding.EncodeToString([]byte(staticServerScript))
	return "RUN echo " + script + " | base64 -d > /tmp/molo-nginx.sh && sh /tmp/molo-nginx.sh /app/" + dir + " > /tmp/molo-nginx.conf && rm -f /tmp/molo-nginx.sh\n"
}

const staticServerCopy = "COPY --from=build /tmp/molo-nginx.conf /etc/nginx/conf.d/default.conf\n"
