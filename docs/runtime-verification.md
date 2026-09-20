# Předání a protokol ověření — 20. září 2026

## Výsledek

Implementace je v izolovaném worktree `/Users/jakubillich/Documents/Codex/2026-09-14/ca/work/deployer-runtime`, lokální větev `codex/runtime-engine`, založená na `f97dc3627984e5b5c49741d740612fe85fdd5dac`. Původní checkout `deployer-public` zůstal na `main` čistý. Portálové soubory nebyly upravené. Nic nebylo pushnuto a žádné skutečné nasazení ani jiná mutace na vzdáleném VPS neproběhla.

Spustitelná interní služba obsahuje šest dohodnutých endpointů, autentizaci organizace pomocí servisního tokenu, šifrovaný durable store, FIFO operace v projektu s férovým střídáním projektů, obnovu rozpracovaných operací, OCI build/push adaptér, Runtime capability/manifest validaci, health + route ověření, idempotentní obnovení historického digestu a oba zdroje redigovaných logů.

Postup nastavení a úplný kontrakt: [runtime-deployer.md](runtime-deployer.md). Upstream response předpoklady: [runtime-engine-adapter.md](runtime-engine-adapter.md).

## Skutečně provedené lokální ověření

| Kontrola | Výsledek |
| --- | --- |
| `go test ./...` | PASS včetně původního Deployeru |
| `go test -race ./...` | PASS, všechny balíčky |
| `go vet ./...` | PASS |
| `go build ./...` | PASS |
| `git diff --check` | PASS |
| Build `bin/runtime-deployer` a původního `bin/deployer` | PASS, lokální macOS/arm64 |
| `GOOS=linux GOARCH=amd64 go build -o bin/runtime-deployer-linux-amd64 ./cmd/runtime-deployer` | PASS, pouze cross-compilation |
| `python3 scripts/runtime-smoke.py` | PASS: reálná binárka, kontrola konfigurace, loopback HTTP, 401 bez tokenu, autorizovaný 404, SIGTERM shutdown; žádná úloha nebyla odeslaná |
| Skutečný Git v testech | PASS: pin SHA, committed obsah, vynechání dirty/untracked obsahu a `.git` |
| Node příklad | PASS: syntaxe a lokální HTTP `/` + `/healthz` |

Testované scénáře: tenant/project boundary; idempotence a změna body pod stejným klíčem; restart store i worker fáze; nejistá odpověď Runtime a retry se stejným ID; selhání build/publish; nedostupnost Runtime a draining; withholding online do ověření konkrétního release/routy; selhání health; rollback historického digestu i env; env NUL/invalid names; redakce současných i historických secretů; cursor replay/retence; nejistý fsync po rename; lock druhého procesu; timeout aktivace s pokračováním reconciliation; férovost mezi projekty.

## Co bylo simulované

- Registry publikace/digest: fake Buildx executor vytváří metadata; žádný reálný registry push nebyl ověřen.
- Propojené statické/Node integrační scénáře: reálné HTTP portálové API a reálný Runtime HTTP adaptér vůči lokálnímu `httptest` Runtime fixture, injektovaný build artifact. Každá mutace fixture ověřuje `X-Socen-Sandbox: true`; úspěch, idempotence, logy, neúspěšný kandidát se zachováním starého release a rollback jsou automatizované.
- VPS health, route revision a runtime logy: odpovědi fixture, nikoli skutečný běžící kontejner.

Lokální Docker klient je dostupný, ale Docker daemon nebyl dostupný. Proto nebyl možný ani lokální skutečný image build. Veřejný Runtime prototyp vyžaduje autentizaci a nebyl poskytnut scoped token; živý autorizovaný sandbox happy/failure path se neprovedl. Nejsou doložené entity response schemas mimo snapshot; adaptér při neznámém výsledku nesmí hlásit online.

## Zbývá před skutečným provozem

1. Zpřístupnit izolovaný Docker/BuildKit build host a zvolit privátní registr s oddělenými push/pull účty.
2. Dodat secret soubory lokálně mimo Git, potvrdit endpoint a autorizovaně ověřit skutečný Runtime sandbox včetně response schémat.
3. Nastavit interní TLS/mTLS přístup portálu, per-organization service tokens, zálohování store a klíče. Klientské OIDC/RBAC patří portálové službě.
4. Teprve na samostatný pokyn provést skutečný VPS deploy. Pro sandbox a produkci použít oddělený store.

Omezení této verze: jediný proces/worker; public HTTPS Git bez GitHub App/SSH/submodules/LFS; mutable base-image tags (výsledný release je přesto pinned digest); omezená tail retence upstream logů; historický výsledek online není nepřetržitý uptime monitoring; historie/idempotency receipts se automaticky nemažou. Tato omezení jsou popsaná i v provozním návodu, nejedná se o ověřený produkční rollout.

## Změněné soubory

- `.gitignore`
- `README.md`
- `cmd/runtime-deployer/main.go`
- `cmd/runtime-deployer/main_test.go`
- `deploy/runtime/runtime-deployer.service`
- `deploy/runtime/runtime.env.example`
- `docs/runtime-deployer.md`
- `docs/runtime-engine-adapter.md`
- `docs/runtime-verification.md`
- `examples/README.md`
- `examples/runtime-node/build-spec.json`
- `examples/runtime-node/server.mjs`
- `examples/runtime-static/build-spec.json`
- `examples/runtime-static/public/healthz`
- `examples/runtime-static/public/index.html`
- `internal/controlplane/api.go`
- `internal/controlplane/api_test.go`
- `internal/controlplane/integration_test.go`
- `internal/controlplane/logs.go`
- `internal/controlplane/logs_test.go`
- `internal/controlplane/regression_test.go`
- `internal/controlplane/store.go`
- `internal/controlplane/types.go`
- `internal/controlplane/worker.go`
- `internal/controlplane/worker_test.go`
- `internal/ocibuild/builder.go`
- `internal/ocibuild/builder_test.go`
- `internal/runtimeengine/client.go`
- `internal/runtimeengine/client_test.go`
- `internal/runtimeengine/types.go`
- `internal/runtimeengine/validate.go`
- `scripts/runtime-smoke.py`
