const OFFLINE_URL = '/static/offline.html';
const CACHE_NAME = 'deployer-offline-v1';

self.addEventListener('install', event => {
    event.waitUntil((async () => {
        const response = await fetch(OFFLINE_URL, {cache: 'reload', credentials: 'same-origin'});
        // An expired gateway session can redirect to a sign-in page.
        if (!response.ok || response.redirected || !response.headers.get('Content-Type')?.includes('text/html')) {
            throw new Error('Offline page unavailable');
        }
        const cache = await caches.open(CACHE_NAME);
        await cache.put(OFFLINE_URL, response);
    })());
});

self.addEventListener('activate', event => {
    event.waitUntil((async () => {
        for (const name of await caches.keys()) {
            if (name.startsWith('deployer-offline-') && name !== CACHE_NAME) await caches.delete(name);
        }
        await self.clients.claim();
    })());
});

self.addEventListener('fetch', event => {
    const request = event.request;
    const url = new URL(request.url);
    // API requests, SSE streams, downloads, and mutations always use the network.
    // Only failed HTML navigations get a generic offline screen.
    if (request.method !== 'GET' || request.mode !== 'navigate' || url.origin !== self.location.origin ||
        !(url.pathname === '/' || url.pathname === '/runners' || url.pathname === '/notifications' ||
          /^\/(projects|builds|runners)\//.test(url.pathname))) return;
    event.respondWith(fetch(request, {cache: 'no-store'}).catch(async () => {
        const cache = await caches.open(CACHE_NAME);
        return (await cache.match(OFFLINE_URL)) || Response.error();
    }));
});
