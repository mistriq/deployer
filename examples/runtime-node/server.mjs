import http from 'node:http';
const port = Number(process.env.PORT || 3000);
const server = http.createServer((request, response) => {
  const path = new URL(request.url, 'http://localhost').pathname;
  response.setHeader('Content-Type', 'application/json');
  if (path === '/healthz') {
    response.end(JSON.stringify({ status: 'ok' }));
    return;
  }
  if (path !== '/') {
    response.writeHead(404);
    response.end(JSON.stringify({ error: 'not found' }));
    return;
  }
  response.end(JSON.stringify({ message: 'Node deployment is live.', runtime: process.version }));
});
server.listen(port, '0.0.0.0', () => console.log(`Listening on ${port}`));
for (const signal of ['SIGTERM', 'SIGINT']) {
  process.on(signal, () => server.close(() => process.exit(0)));
}
