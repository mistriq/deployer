#!/usr/bin/env node
// Recovery of the one pre-admission operation captured during the coordinated
// sandbox restart. Never resets Runtime, submits a deployment, or starts workers.
import {readFile, stat, writeFile} from 'node:fs/promises';
import {createDecipheriv, createHash} from 'node:crypto';
import {resolve, dirname} from 'node:path';
import {fileURLToPath} from 'node:url';
import {execFileSync} from 'node:child_process';
import {isDeepStrictEqual} from 'node:util';
import net from 'node:net';

const jobID = 'dep_ecf8156baba32e8bdb8c89cb2b0afbd6';
const projectID = 'prj_ec24c947789e0d0f3e341f6ddbce2443';
const expectedDigest = 'sha256:a8d1f9bb91443da3b3986c7af2c491d3bbc1cae7f9317f6748e6a24d006194da';
const root = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const [mode, backupArg] = process.argv.slice(2);
function requireThat(ok, message) { if (!ok) throw Error(message); }
async function privateRead(path) {
  const s = await stat(path);
  requireThat(s.isFile() && !(s.mode & 0o077), 'Recovery input must be a private regular file');
  return readFile(path);
}
async function stateAt(dir) {
  const b = await privateRead(resolve(dir, 'state.enc'));
  const key = Buffer.from((await privateRead(resolve(dir, 'state-key'))).toString().trim(), 'hex');
  const d = createDecipheriv('aes-256-gcm', key, b.subarray(0, 12));
  d.setAAD(Buffer.from('runtime-deployer-v1'));
  d.setAuthTag(b.subarray(-16));
  return JSON.parse(Buffer.concat([d.update(b.subarray(12, -16)), d.final()]));
}
async function listening(port) {
  return new Promise(resolveResult => {
    const socket = net.connect({host: '127.0.0.1', port});
    const done = result => { socket.destroy(); resolveResult(result); };
    socket.once('connect', () => done(true));
    socket.once('error', () => done(false));
    socket.setTimeout(1000, () => done(false));
  });
}
async function main() {
  requireThat(['check', 'restore'].includes(mode) && backupArg, 'Usage: node scripts/sandbox-recover.mjs check|restore PRIVATE_BACKUP_DIRECTORY');
  const backup = resolve(backupArg);
  requireThat(!((await stat(backup)).mode & 0o077), 'Backup directory must be private');
  const inventory = JSON.parse(await privateRead(resolve(backup, 'runtime-inventory.json')));
  const mapping = JSON.parse(await privateRead(resolve(backup, 'local-project-map.json'))).find(p => p.runtime_project_id === projectID);
  requireThat(mapping && /^[a-f0-9]{64}$/.test(mapping.organization_directory), 'Target organization not captured');
  const originalDir = resolve(root, 'portal/data/runtime/sandbox/organizations', mapping.organization_directory);
  const saved = await stateAt(resolve(backup, mapping.organization_directory));
  const current = await stateAt(originalDir);
  const job = current.jobs[jobID];
  requireThat(job && isDeepStrictEqual(job, saved.jobs[jobID]), 'Worker job changed since backup; re-inspect before recovery');
  requireThat(job.state === 'deploying' && job.stage === 'submitting' && !job.runtime_deployment_id && job.artifact?.Digest === expectedDigest, 'Unexpected job state or artifact');
  requireThat('prj_' + createHash('sha256').update(job.tenant_id + '\0' + job.project_id).digest('hex').slice(0, 32) === projectID, 'Project ownership mismatch');
  const project = inventory.projects.find(p => p.id === projectID);
  requireThat(project, 'Missing saved Runtime project');
  const normalize = m => ({...m, runtime: {...m.runtime, env_names: m.runtime.env_names || []}});
  requireThat(isDeepStrictEqual(normalize(project.manifest), normalize(job.snapshot.spec.manifest)), 'Saved manifests differ');
  const conn = JSON.parse(await privateRead(resolve(originalDir, 'connection.json')));
  requireThat(Number.isInteger(conn.port) && conn.port > 1023 && conn.port < 65536, 'Invalid saved worker port');
  requireThat(!execFileSync('ps', ['-axo', 'comm'], {encoding: 'utf8'}).split('\n').some(p => /(?:^|\/)runtime-deployer$/.test(p.trim())), 'A Deployer worker is running; stop it before recovery');
  requireThat(!await listening(conn.port) && !await listening(4173), 'Worker or portal listener is running; stop it before recovery');
  const token = (await privateRead(resolve(root, 'portal/data/runtime/runtime-token'))).toString().trim();
  async function call(method, path, body) {
    requireThat(path.startsWith('/api/internal/v1/') && !path.includes('..'), 'Invalid Runtime path');
    if (method !== 'GET') requireThat(mode === 'restore' && ((method === 'POST' && path === '/api/internal/v1/projects' && body.project_id === projectID) || (method === 'PUT' && path === `/api/internal/v1/projects/${projectID}/env`)), 'Mutation outside recovery allowlist');
    const response = await fetch(new URL(path, 'https://scr.socen.eu'), {
      method, redirect: 'error', signal: AbortSignal.timeout(15000),
      headers: {Authorization: 'Bearer ' + token, 'X-Socen-Sandbox': 'true', 'Content-Type': 'application/json'},
      body: body ? JSON.stringify(body) : undefined,
    });
    requireThat(response.ok, `Runtime ${method} failed with HTTP ${response.status}`);
    return response.json();
  }
  const node = await call('GET', '/api/internal/v1/node');
  const sandbox = await call('GET', '/api/internal/v1/sandbox');
  requireThat(node.node_id === 'nod_sandbox' && node.driver === 'mock' && node.proxy === 'mock' && sandbox.enabled === true, 'Expected mock sandbox');
  const resetObserved = sandbox.seeded_at !== inventory.sandbox.seeded_at;
  const report = {mode, deployment_id: jobID, project_id: projectID, worker_stopped: true, artifact_unchanged: true, environment_variables: Object.keys(job.snapshot.env || {}).length, sandbox_restart_observed: resetObserved, cpu_total: node.total.cpu_millis, cpu_available: node.total.cpu_millis - node.allocated.cpu_millis - node.reserve.cpu_millis};
  if (mode === 'check') { console.log(JSON.stringify(report, null, 2)); return; }
  requireThat(resetObserved && node.total.cpu_millis >= 16000 && node.healthy && !node.draining && !node.global_stop && report.cpu_available >= project.manifest.resources.cpu_millis, 'Updated sandbox has not restarted or lacks capacity; no writes performed');
  const projects = await call('GET', '/api/internal/v1/projects');
  const existing = projects.items.find(p => p.id === projectID);
  if (existing) {
    requireThat(existing.slug === project.slug && existing.hostname === project.hostname && isDeepStrictEqual(normalize(existing.manifest), normalize(project.manifest)), 'Existing project conflicts with backup');
    const history = await call('GET', `/api/internal/v1/projects/${projectID}/deployments`);
    requireThat(history.count === 0 && history.items.length === 0, 'Deployment already exists; inspect before changing environment');
  }
  await call('POST', '/api/internal/v1/projects', {project_id: projectID, slug: project.slug, hostname: project.hostname, manifest: project.manifest});
  const restored = await call('GET', `/api/internal/v1/projects/${projectID}`);
  requireThat(restored.id === projectID && restored.hostname === project.hostname && isDeepStrictEqual(normalize(restored.manifest), normalize(project.manifest)), 'Restored project mismatch; worker remains stopped');
  await call('PUT', `/api/internal/v1/projects/${projectID}/env`, {env: job.snapshot.env || {}});
  const env = await call('GET', `/api/internal/v1/projects/${projectID}/env`);
  requireThat(isDeepStrictEqual(env.variables.map(v => v.name).sort(), Object.keys(job.snapshot.env || {}).sort()), 'Environment name mismatch; worker remains stopped');
  const receipt = {...report, restored_at: new Date().toISOString(), seeded_at: sandbox.seeded_at, project_restored: true, environment_restored: true, worker_started: false};
  await writeFile(resolve(backup, 'restore-receipt.json'), JSON.stringify(receipt, null, 2), {mode: 0o600});
  console.log(JSON.stringify(receipt, null, 2));
}
main().catch(() => { console.error('Recovery stopped. No worker was started. Check private inputs, stopped processes and sandbox readiness; do not retry by creating a new deployment.'); process.exitCode = 1; });
