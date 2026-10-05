import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

const source = readFileSync(new URL('../internal/app/web/static/service-worker.js', import.meta.url), 'utf8');
function worker(fetch) {
    const handlers = new Map();
    const stored = new Map();
    const deleted = [];
    const cache = {put: async (url, response) => stored.set(url, response), match: async url => stored.get(url)};
    const self = {location: {origin: 'https://deployer.test'}, clients: {claim: async () => {}}, addEventListener: (name, handler) => handlers.set(name, handler)};
    vm.runInNewContext(source, {self, URL, Response, fetch, caches: {open: async () => cache, keys: async () => ['unrelated-app', 'deployer-offline-old'], delete: async name => deleted.push(name)}});
    return {handlers, stored, deleted};
}
async function lifecycle(w, name) {
    let pending;
    w.handlers.get(name)({waitUntil: promise => {pending = promise;}});
    await pending;
}
function navigation(w, path, mode = 'navigate', method = 'GET') {
    let response;
    w.handlers.get('fetch')({request: {url: 'https://deployer.test' + path, mode, method}, respondWith: promise => {response = promise;}});
    return response;
}
const offline = new Response('<h1>You’re offline</h1>', {headers: {'Content-Type': 'text/html'}});
const w = worker(async request => {
    if (request === '/static/offline.html') return offline.clone();
    throw new TypeError('Network unavailable');
});
await lifecycle(w, 'install');
assert.deepEqual([...w.stored.keys()], ['/static/offline.html']);
await lifecycle(w, 'activate');
assert.deepEqual(w.deleted, ['deployer-offline-old']);
assert.match(await (await navigation(w, '/projects/123')).text(), /offline/);
for (const path of ['/api/projects', '/api/builds/1/stream', '/download/deployer', '/signed-out', '/oidc/callback']) {
    assert.equal(navigation(w, path), undefined, path + ' must bypass the worker');
}
assert.equal(navigation(w, '/', 'navigate', 'POST'), undefined);
assert.equal(navigation(w, '/projects/123', 'cors'), undefined);
const serverError = worker(async () => new Response('Server error', {status: 500}));
assert.equal((await navigation(serverError, '/')).status, 500);
const redirected = worker(async () => ({ok: true, redirected: true}));
await assert.rejects(lifecycle(redirected, 'install'), /Offline page unavailable/);
assert.equal(redirected.stored.size, 0, 'Gateway sign-in pages must not be cached');
console.log('PWA worker boundaries passed');
