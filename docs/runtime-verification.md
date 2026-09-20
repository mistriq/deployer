# Předání a protokol ověření — 20. září 2026

## Obnova po restartu — PASS, 10:49–10:51 UTC

Blokace popsaná níže je vyřešená. Sandbox po restartu měl 16000 CPU, 2000 alokovaných a 500 rezervovaných (13500 dostupných). Guardovaný recovery skript obnovil původní projekt a prázdné env, potom byl spuštěn existující portál a jeho sandboxový worker bez úpravy zdrojových souborů portálu.

Původní `dep_ecf8156baba32e8bdb8c89cb2b0afbd6` přešel do `online/active` v 10:49:56 UTC. Runtime historie obsahuje právě jednu položku `dep_06gbwvypqbpnq7qq4ww8q4fd4g`, external ID i idempotency key jsou původní Deployer ID, digest zůstává `sha256:a8d1f9bb91443da3b3986c7af2c491d3bbc1cae7f9317f6748e6a24d006194da`. Build ID a commit zůstaly stejné; nový build neproběhl. Release `rel_06gbwvypqh635249h8swswe2x8` odpovídá aktivnímu projektu i routě, desired/applied/release revision jsou 2. Timeline obsahuje health_check → activating → active.

Oba Deployer zdroje logů prošly: build 69 řádků, runtime 1 řádek, opakované čtení od koncového kurzoru vrací nula dalších řádků. [Sdílitelný výpis důkazů bez credentials](runtime-resume-evidence.json) obsahuje zúženou odpověď historie a routy.

Čerstvý `TestLiveSandbox` prošel za 10,19 s pro static i Node HTTP: upsert/env, idempotence, health/routing, deployment/release logy, odmítnutí neplatného artifactu, vyvolaná health chyba při zachování předchozího release a idempotentní obnovení historického artifactu. Vyhrazené projekty jsou `prj_codex_static_89dbca1bb2baa5e3` a `prj_codex_node_http_89dbca1bb2baa5e3`; úspěšné deploymenty `dep_06gbww48xpqmwstt743qxhatwr` a `dep_06gbww4tf7zta9gza9syfq1z3r`, failed candidates `dep_06gbww4f4tqcqg4n0sp8hkhtbw` a `dep_06gbww50kxdrbq8cwm4e61p5sg`, restoration `dep_06gbww4mawx52wm80wkqseryer` a `dep_06gbww561q055gapmcpjjrt21w`. Po QA sandbox alokuje 4500 z 16000 CPU; žádné sdílené projekty nebyly měněny.

Rozsah: původní portálová operace byla obnovena přes skutečný Deployer worker. Nové static/Node scénáře testují Runtime adaptér přímo, nikoli nové kompletní browser/build průchody portálem. Sandbox má mock driver/proxy, takže toto ověřuje lifecycle, nikoli vzdálený pull z lokálního registru nebo veřejný hosting. Předchozí skutečné lokální Docker testy jsou doložené odděleně níže. Historické Runtime ID před restartem nejsou v novém sandboxu platné; jejich inventář zůstává v privátní záloze.

## Aktuální blokace portálového workflow

Navazující ověření v 02:04–02:06 UTC zjistilo vyčerpanou CPU kapacitu vzdáleného sandboxu. Portálová operace `dep_ecf8156baba32e8bdb8c89cb2b0afbd6` stále čeká v `deploying/submitting`; její image, commit i build ID zůstaly stejné a Runtime zatím nepřijal žádný deployment tohoto projektu. Níže uvedené dřívější PASS výsledky neznamenají, že tato operace už pokračovala.

[Přesná diagnóza a zpráva pro správce Runtime](runtime-capacity-handoff.md) obsahuje rozpis historických releasů, konkrétní vlastní kandidáty pro případný úklid a akceptační postup. Tentokrát proběhly pouze sandboxové GET požadavky; žádný úklid, reset ani změna portálu. Správce Runtime musí obnovit kapacitní rezervu před ověřením pokračování původní operace a nového kompletního workflow.

Deployer má doplněný regresní test opakovaného `NODE_CAPACITY_EXHAUSTED` přes restart: stejný artifact a ID, žádný rebuild, přesně jedna přijatá simulovaná operace a následná aktivace. Vzdálený test nyní před první mutací ověřuje kapacitu pro všech šest kandidátů; snapshot není rezervace a souběh dalších agentů může stále změnit admission výsledek. `go test -race ./internal/controlplane ./internal/runtimeengine` prošel. Čerstvé read-only kontroly dřívějších static/Node testů potvrdily aktivní release, lifecycle health, shodné revize rout a oba Runtime log endpointy. Nejde o nové nasazení ani důkaz skutečného vzdáleného hostingu.

## Výsledek

Implementace je v izolovaném worktree `/Users/jakubillich/Documents/ChatGPT/Deployer & customer center/deployer`, lokální větev `codex/runtime-engine`, založená na `f97dc3627984e5b5c49741d740612fe85fdd5dac`. Původní checkout `deployer-public` zůstal na `main` čistý. Portálové soubory nebyly upravené. Git změny nebyly pushnuté. Produkční node/VPS nebyl změněn; vzdálené mutace proběhly výhradně v odděleném sandboxu.

Spustitelná interní služba obsahuje šest dohodnutých endpointů, autentizaci organizace pomocí servisního tokenu, šifrovaný durable store, FIFO operace v projektu s férovým střídáním projektů, obnovu rozpracovaných operací, OCI build/push adaptér, Runtime capability/manifest validaci, health + route ověření, idempotentní obnovení historického digestu a oba zdroje redigovaných logů.

Postup nastavení a úplný kontrakt: [runtime-deployer.md](runtime-deployer.md). Ověřený upstream response kontrakt: [runtime-engine-adapter.md](runtime-engine-adapter.md).

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

- Výchozí unit testy registry publikace/digestu používají fake Buildx executor. V prvním průchodu nebyl reálný push ověřen; následný skutečný Docker test je doložený níže.
- Propojené statické/Node integrační scénáře: reálné HTTP portálové API a reálný Runtime HTTP adaptér vůči lokálnímu `httptest` Runtime fixture, injektovaný build artifact. Každá mutace fixture ověřuje `X-Socen-Sandbox: true`; úspěch, idempotence, logy, neúspěšný kandidát se zachováním starého release a rollback jsou automatizované.
- VPS health, route revision a runtime logy: odpovědi fixture, nikoli skutečný běžící kontejner.

Při prvním průchodu Docker daemon nebyl dostupný. Po navazujícím požadavku byl Docker Desktop spuštěn pomocí pluginu Computer; doplňující skutečné Docker ověření popisuje následující sekce. Původně chybějící autentizaci doplnil uživatel v navazujícím kroku; poté proběhlo read-only ověření skutečných schémat a úspěšný živý sandbox happy/failure/restore test popsaný níže.

## Zbývá před skutečným provozem

1. Zvolit produkční izolovaný build host a privátní registr s oddělenými push/pull oprávněními; lokální Docker/registry tok už je ověřený.
2. Nastavit produkční secret soubory lokálně mimo Git a potvrdit definitivní endpoint. Prototypový sandbox včetně skutečných response schémat již ověřený je.
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

## Doplňující skutečný Docker test

Na navazující požadavek uživatele byl přes plugin Computer otevřen Docker Desktop. UI potvrdilo Engine running, CLI ověřilo Docker Engine 28.3.3. Test odhalil a opravil nalezení Buildx pluginu při izolovaném `HOME`/`DOCKER_CONFIG` na macOS. Testovací Docker příkazy také používají izolovanou konfiguraci, aby nevolaly uživatelův keychain helper.

`OCI_DOCKER_INTEGRATION=1 go test -v ./internal/ocibuild -run TestDockerRegistryIntegration -count=1 -timeout=10m` **PASS**; navíc `OCI_DOCKER_INTEGRATION=1 go test -race ./internal/ocibuild -count=1 -timeout=10m` **PASS**. Test ověřil skutečný build, autentizovaný push/digest, pull přes druhou identitu, kontejnery obou aplikací a health. Registry běží pouze na loopbacku Docker hostitele (VM na macOS). Docker inspect kontroluje UID 10001, read-only filesystem, tmpfs, zahození capabilities, no-new-privileges, 0,5 CPU, 128 MiB RAM a 64 PID. Obě aplikace vrací `/healthz = 200`, neexistující cesta vrací 404. Neautorizovaný registry request vrací 401. Standardní registry Basic Auth nerozlišuje read/write ACL; pull-only oprávnění produkčního účtu nebylo ověřeno.

Dočasné kontejnery, registry volume, přihlašovací soubory a odkazy na testovací image se uklízejí. Sdílené base images a build cache zůstávají; původní kontejnery ani nastavení daemonu se nemění. Docker Desktop zůstává spuštěný.

Vzdálený veřejný `/healthz` na `scr.socen.eu` vrací 200. V této fázi ještě chyběl token. Následný autorizovaný běh je doložený v následující sekci; používá nezávislý guard přesného HTTPS hostu, sandbox hlavičky a vlastních testovacích projektů.

Nové/dodatečně změněné soubory: `internal/ocibuild/docker_integration_test.go`, `internal/ocibuild/builder.go`, `internal/ocibuild/builder_test.go`, `internal/runtimeengine/live_integration_test.go`, `examples/README.md`, `README.md` a oba hlavní provozní/ověřovací dokumenty.

Ověřené digesty jednoho úspěšného reálného běhu:

- Static: `sha256:535ac97b7dda38e8927b07e1916f91c42f82101faa4394c6fbf8b7b49b162fe3`
- Node: `sha256:3fccf9c75fb83b4734ffbeef8ee31418c530625a00971ff91efa73d3812d4138`

Kontrola po testu našla nula kontejnerů s labelem `deployer.integration` a nula testovacích image referencí `127.0.0.1:*/runtime-*`.

## Autorizovaný vzdálený sandbox — PASS

Po dodání tokenu uživatelem byl nejprve read-only ověřen skutečný kontrakt. Sandbox odpověděl jako `nod_sandbox`, `driver=mock`, `proxy=mock`, `store=memory`. Odhalené rozdíly byly opraveny v adaptéru: deployment `phase`, release lifecycle metadata bez `healthy`, route `route_id` a strukturované logy `{items:[{at,stream,text}]}`. Portálové logy nyní zachovávají zdrojové timestampy/streamy a kurzor rozlišuje opakovaný text podle identity záznamu.

Výsledek: `RUN_RUNTIME_SANDBOX_TESTS=1 go test -v ./internal/runtimeengine -run '^TestLiveSandbox$' -count=1 -timeout=6m` **PASS**, 10,29 s testu. Token se předal přes dočasný privátní soubor, nikoli argumentem; image/digest byly převzaté z read-only sandboxového záznamu Acme. Každá mutace byla kontrolována nezávislým guardem cíle, sandbox hlavičky i projektového ID.

| Varianta | Testovací projekt | Úspěšný deployment | Kandidát s chybným health checkem | Obnovení historické verze |
| --- | --- | --- | --- | --- |
| Static | `prj_codex_static_1b7861d48b85c9e4` | `dep_06gbrj4h98a6a9ks1a7k977r60` | `dep_06gbrj4qf4wv5c009t7w6r082w` | `dep_06gbrj4wn6kdg7pe1mrgma36k4` |
| Node HTTP | `prj_codex_node_http_1b7861d48b85c9e4` | `dep_06gbrj56q5sch7jzzs0sph99kw` | `dep_06gbrj5d2pavpmr7w6y81fw5mw` | `dep_06gbrj5jr567zjddh2ndexmpv8` |

V obou variantách prošlo: opakovaný project upsert; env 2 → 1 → 0 proměnných s kontrolou write-only metadata; opakované deployment ID; lifecycle health + matching active release + aplikovaná route revision; deployment/release logy; HTTP 422 `ARTIFACT_DIGEST_MISMATCH`; aktivně vyvolaná health chyba očekáváním HTTP 201 místo odpovědi 200; zachování starého aktivního release; idempotentní obnovení známého artifactu a původního manifestu do nového release.

V sandboxu zůstávají vyhrazené testovací projekty a pomocný `prj_codex_probe_38ed10711f47` pro kontrolu výsledků. Sdílený sandbox nebyl resetován. Native `/rollback` nebyl volán; ověřena byla bezpečná implementace portálového rollbacku přes nový idempotentní deployment historického artifactu. Živý sandbox používá svůj známý mock artifact, nikoli pull z lokálního testovacího registru; kompletní produkční VPS cesta tím není deklarovaná za ověřenou.

Dodatečně změněné soubory: `internal/runtimeengine/client.go`, `types.go`, `client_test.go`, `live_integration_test.go`, `internal/controlplane/logs.go`, nový `structured_logs_test.go`, `README.md` a runtime dokumentace. Regresní testy pokrývají i explicitně nezdravý release, chybějící lifecycle důkaz, timestamp/stream, restart kurzoru a opakované shodné texty s různými timestampy.

Po opravách znovu prošly `go test -race ./...`, `go vet ./...`, build služby, CLI smoke test a `git diff --check`. Kontrola projektových souborů nenašla token. Dočasná privátní kopie tokenu byla po ověření odstraněna.
