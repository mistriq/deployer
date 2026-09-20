# Runtime Engine Deployer — interní v1 API a provoz

Nový samostatný příkaz `cmd/runtime-deployer` obsluhuje portál → Git → OCI registry → Runtime Engine. Původní `cmd/deployer`, jeho web, runner a Compose tok zůstávají beze změny. Nový příkaz je nenačítá. Ze stávajícího řešení přebírá vzor injektovatelného command executoru, průběžných build událostí a redakce; původní globální databázi a transport archivů nepřebírá, protože jsou navázané na starý runner.

## Stav ověření

Výchozí automatické testy používají skutečný Git a šifrovaný diskový store, fake command executor pro registry push a lokální HTTP Runtime fixture. Doplňující skutečný Docker test ověřil build obou ukázkových aplikací, autentizovaný push do dočasného lokálního registru, pull podle digestu a spuštění s omezeními Runtime. Následně prošel i autorizovaný vzdálený sandbox na `scr.socen.eu`: obě varianty, idempotence, env, health/routing/logy, odmítnutí digestu, selhání nové verze a obnovení historického artifactu. Skutečné formáty odpovědí jsou v [runtime-engine-adapter.md](runtime-engine-adapter.md).

Sandbox používá mock driver a známý sandbox image digest. Lokální Docker test a vzdálený sandbox jsou dvě samostatně ověřené části; **produkční registr, skutečný image pull na VPS a definitivní produkční URL ještě ověřené nejsou**. Samotný HTTP 202 nikdy neznamená aktivní release.

## Spuštění

Potřeba: Go toolchain podle `go.mod`, Git, Docker s Buildx a vyhrazený build host. Repo musí být přístupné přes HTTPS bez interaktivního přihlášení; privátní Git autentizace v této verzi není implementovaná. OCI registr může být privátní. Build kontejnerům se nepředávají Runtime env secrety ani prostředí hostitele. Kód repozitáře se vykonává při buildu: build host musí být izolovaný od produkce a interních sítí; Docker socket představuje oprávnění nad build hostem.

```sh
go build -o bin/runtime-deployer ./cmd/runtime-deployer
go test ./...
go vet ./...
python3 scripts/runtime-smoke.py
go test -race ./internal/controlplane ./internal/ocibuild ./internal/runtimeengine
```

Konfigurace je v [runtime.env.example](../deploy/runtime/runtime.env.example), volitelná jednotka v [runtime-deployer.service](../deploy/runtime/runtime-deployer.service). Nastavte proměnné prostředí podle příkladu. Secret soubory musí být privátní regular files (`0600` nebo `0400`), přístupné pouze účtu služby:

- `DEPLOYER_STATE_KEY_FILE`: 32 náhodných bajtů zapsaných jako 64 hex znaků. Vygenerujte přímo do souboru, například `umask 077; openssl rand -hex 32 > state-key`. Klíč neztratit; bez něj nelze obnovit uložené env ani úlohy.
- `PORTAL_TOKENS_FILE`: JSON objekt organizace → unikátní náhodný servisní token (alespoň 32 znaků). Například tvar `{"org_a":"<service token>","org_b":"<different service token>"}`. Nejde o uživatelské OIDC tokeny. Portál smí token příslušné organizace použít až po vlastní kontrole uživatelského oprávnění k projektu.
- `RUNTIME_TOKEN_FILE`: samostatný scoped Runtime token; potřebné scopes uvádí dokumentace adaptéru. Nikdy do browseru.
- `REGISTRY_PUSH_PASSWORD_FILE` a `REGISTRY_PUSH_USERNAME`: účet pro push. Pull-only účet nastavuje provozovatel přímo v Runtime Enginu; Deployer ho nezná. Bez username/password se použije anonymní přístup, nikoli ambientní Docker credentials.

Před spuštěním lze zkontrolovat lokální konfiguraci pomocí `bin/runtime-deployer --check-config`; síťové mutace se neprovádějí. Příkaz ověří také otevření a dešifrování store. Poté spusťte `bin/runtime-deployer`.

Listener povoluje pouze loopback IP (default `127.0.0.1:8091`). Pro vzdálený portál použijte privátní TLS/mTLS reverse proxy nebo zabezpečený tunel; nevystavujte veřejný ingress. Všechny endpointy vyžadují `Authorization: Bearer <portal service token>`. Server ignoruje jakékoli klientem podstrčené tenant hlavičky; organizaci určuje token. Projektové ID je v této verzi globálně unikátní, podruhé je jiná organizace nepřevezme. Runtime ID je odvozené hashováním organizace a projektu. Request body limit je 256 KiB.

## Kontrakt

Každá mutace obsahuje `request_id` (1–128 znaků, písmena/číslice/`_`/`-`, začíná písmenem nebo číslicí). V rozsahu organizace + metody + cesty se stejné ID a stejný JSON body vrací k původní odpovědi; změněný body vrací `409 REQUEST_CONFLICT`. Porovnání body je bajtové: při retry znovu odešlete stejný serializovaný body. `request_id` zachovejte i při timeoutu/reconnectu. Projektová ID mají stejný formát. Nové úmyslné nasazení musí mít nové ID.

### PUT /internal/v1/projects/{project_id}

Upsert lokální požadované konfigurace. `revision` roste při nové změně. Lokální manifest se validuje při přijetí, capability limity skutečného Runtime se ověřují před jeho mutací. Synchronizace Runtime probíhá v deployment workeru, nikoli v tomto PUT.

```json
{
  "request_id": "project-config-1",
  "repository": "https://github.com/your-org/your-app.git",
  "ref": "refs/heads/main",
  "build": {
    "kind": "node-http",
    "context_dir": ".",
    "install_command": "npm ci",
    "build_command": "npm run build",
    "start_command": "npm start",
    "port": 3000,
    "platform": "linux/amd64"
  },
  "manifest": {
    "version": 1,
    "kind": "node-http",
    "policy_version": "policy-2026-07",
    "runtime": {
      "port": 3000,
      "user": "10001:10001",
      "read_only_root": true,
      "tmpfs": [{"path": "/tmp", "size_mb": 64}],
      "env_names": ["DATABASE_URL"]
    },
    "health": {
      "path": "/healthz",
      "expect_status_min": 200,
      "expect_status_max": 299,
      "timeout_ms": 2000,
      "interval_ms": 500,
      "retries": 20,
      "grace_period_ms": 1000
    },
    "resources": {"cpu_millis": 500, "memory_mb": 512, "pids_limit": 128, "disk_mb": 1024},
    "network": {"websocket": true, "egress_allowed": true, "max_body_bytes": 10485760, "rate_limit_rps": 50}
  }
}
```

Odpověď `200 {"project_id":"…","revision":1,"request_id":"…"}`. `policy_version` musí odpovídat nasazené Runtime politice. Pro statický projekt nastavte oba `kind` na `static`, port na `8080`, health path na existující asset a `build.output_dir` na `dist`. Pokud nepotřebujete npm, použijte explicitní install/build command z ukázkových projektů. Volitelné `build.dockerfile` volí vlastní Dockerfile relativně ke context dir. Bez něj se generuje recipe pro statický nebo Node HTTP server. `platform` nastavte podle VPS; výchozí hodnota je `linux/amd64`, lze zvolit `linux/arm64`. `build_args` nejsou secret mechanismus; build argumenty se mohou dostat do image/history. Runtime env se do buildu nepředává.

### PUT /internal/v1/projects/{project_id}/env

```json
{"request_id":"env-1","env":{"DATABASE_URL":"<secret value>"}}
```

Úplná náhrada; `{}` vymaže všechny proměnné. Odpověď vrací jen `project_id`, `revision`, `names`, `request_id`. Při vytvoření deploymentu musí názvy přesně odpovídat `manifest.runtime.env_names`; chybějící ani přebytečné proměnné se tiše nepřijmou. Hodnoty jsou na disku v AES-GCM šifrovaném store. Snapshot každého deploymentu chrání běžící úlohu před souběžnou změnou konfigurace/env. Nové env se synchronizuje na Runtime až před dalším deploymentem. Hodnoty nikdy nejsou v read API.

### POST /internal/v1/projects/{project_id}/deployments

```json
{"request_id":"deploy-1"}
```

Odpověď `202 {"deployment_id":"dep_…","request_id":"deploy-1"}`. Worker nejprve přeloží ref na commit a uloží SHA; další pokusy buildu už používají jen tento SHA. Pokud chcete přesný commit již při přijetí požadavku, nastavte `ref` na plné SHA. Výstup buildu je immutable digest; tag není identita release. Po úspěšné publikaci se artifact uloží dříve, než proběhne Runtime mutace. Runtime deployment dostane stabilní lokální ID jako `external_deployment_id` i `Idempotency-Key`.

### GET /internal/v1/deployments/{deployment_id}

Vrací `deployment_id`, `project_id`, `request_id`, `state`, `stage`, `error_code`, bezpečné `message`, `created_at`, `updated_at`, `commit`, `build_id`, `runtime_deployment_id`, `runtime_release_id`, `site_url`. Volitelné údaje jsou přítomné až po jejich zjištění. Secret snapshot ani upstream response body se nevrací.

| state | Portál | Význam |
| --- | --- | --- |
| queued | Sestavuje se | Čeká na worker |
| building | Sestavuje se | Resolve/build |
| publishing | Zveřejňuje se | Buildx build + push ještě nedodal digest |
| deploying | Zveřejňuje se | Sync/submission/health/route nebo bezpečné retry |
| online | Online | Konkrétní release byl ověřen jako zdravý a jeho route jako aplikovaná |
| build_failed | Chyba | Zdroj, build nebo publikace selhaly |
| deployment_failed | Chyba | Runtime kandidát nebo validace selhaly |
| cancelled | Chyba / Zrušeno | Runtime zrušil kandidáta |

Buildx kombinuje sestavení a push do jednoho příkazu: pokud nelze spolehlivě odlišit jejich selhání, použije se `BUILD_PUBLISH_FAILED`. Přechod do `publishing` vychází z export/push progress událostí, nikdy neznamená úspěšný release.

`stage` je technický detail, např. `resolving`, `building`, `publishing`, `runtime_sync`, `submitting`, `verifying`, `active`, `failed`. Runtime `online` není monitoring SLA: představuje výsledek konkrétní operace v čase `updated_at`. Pozdější externí vypnutí VPS nebo routy nemění historický výsledek. Nový neúspěšný deployment nikdy nepřebírá URL/úspěch ze starého release. Při retry zůstává operace neterminální a `error_code` může popisovat dočasný problém.

Lokální stabilní kódy: `SOURCE_FAILED`, `BUILD_FAILED`, `BUILD_PUBLISH_FAILED`, `REGISTRY_PUBLISH_FAILED`, `ARTIFACT_INVALID`, `MANIFEST_INVALID`, `RUNTIME_UNAVAILABLE`, `SUBMISSION_UNCERTAIN`, `ACTIVATION_UNCONFIRMED`, `STORE_UNAVAILABLE`, `UNAUTHORIZED`, `NOT_FOUND`, `INVALID_REQUEST`, `REQUEST_CONFLICT`, `INVALID_SOURCE`, `INVALID_CURSOR`, `RUNTIME_LOGS_UNAVAILABLE`. Adaptér propouští jen známé Runtime kódy (například `NODE_DRAINING`, `HEALTH_CHECK_FAILED`, `ROUTE_PROBE_FAILED`), nikoli jejich neověřené texty. Terminální Runtime failure se automaticky nespouští znovu. Nedostupnost/přetížení před přijetím požadavku používá retry; úloha zachovává identitu.

### GET /internal/v1/deployments/{deployment_id}/logs?source=build|runtime&cursor=…

Odpověď:

```json
{"items":[{"cursor":1,"time":"2026-09-20T00:00:00Z","source":"build","stream":"stdout","text":"building"}],"next_cursor":"1","truncated":false,"source":"build"}
```

Pollujte například každé 2 sekundy zvlášť pro každý zdroj, po reconnectu obnovte poslední cursor. Prázdná stránka není konec streamu. `source` je povinný, bez cursoru se čte od začátku dostupného bufferu. Max. 200 řádků na stránku, posledních 2000 řádků každého zdroje se uchovává napříč restartem. Starší kurzor vrátí `truncated:true`. Runtime řádky zachovávají skutečný upstream `at` jako `time` a původní `stream`. Build/legacy textové logy bez metadat používají čas zachycení a `stdout`.

Build výstup se průběžně čte z procesu a rediguje před zápisem. Runtime logy se načítají přes deployment endpoint, po aktivaci přes release endpoint; čtou se při ověřování i při portálovém pollingu. U upstream tail-only API se ztrátu mezi pollingy nelze pokusit skrýt: při chybějícím překryvu se vloží systémový řádek o nedostupném okně. Překryv strukturovaných logů se určuje podle ID nebo timestampu + streamu + redigovaného textu; stejné texty s odlišnými timestampy se neztrácejí. Záznamy shodné ve všech těchto údajích bez upstream ID nelze rozlišit dokonale. Portálový cursor zajišťuje replay zachycených řádků, nikoli neomezený archiv VPS. Runtime logy jsou redigovány znovu v Deployeru, včetně historických env hodnot, servisních tokenů a běžných token patternů. Libovolná úmyslná transformace secretu v aplikaci není obecně detekovatelná; aplikace nemají secrety logovat.

### POST /internal/v1/projects/{project_id}/rollback

```json
{"request_id":"rollback-1","release_id":"rel_previous"}
```

Odpověď je stejný sledovatelný `deployment_id` jako při deploy. `release_id` je povinný a musí patřit dříve ověřené úspěšné operaci téhož projektu/organizace. Rollback obnoví její digest, manifest a env snapshot bez nového buildu; výsledkem je **nový Runtime release**. Toto je záměrné upřesnění společného kontraktu: katalog nepublikuje idempotenci nativního `/rollback`, proto portálový rollback používá idempotentní `/deployments`. Přímá metoda adaptéru `Rollback` existuje, ale worker ji nepoužívá. Ztracená odpověď tak nevytvoří druhý rollback. Aktuální požadovaná konfigurace projektu zůstane zachována pro příští běžný deploy.

## Restart, zálohy a omezení

Jeden proces má exkluzivní lock nad store. Jediný worker vykonává jednu fázi najednou, zachovává FIFO v rámci projektu a střídá projekty podle posledního postupu; při restartu navazuje z uložené fáze, případně bezpečně opakuje sync nebo odeslání se stejným klíčem. Přerušený build se může zopakovat pro stejné SHA, ale nezaloží nový Runtime deployment. Nedostupná nebo nejistá operace blokuje další operace stejného projektu, jiné projekty mohou postupovat. Po 15 minutách nepotvrzené aktivace se zobrazí `ACTIVATION_UNCONFIRMED`; ověřování původního ID pokračuje. Nejistý výsledek se nepovažuje za dokončené selhání, které by dovolilo konfliktní nové nasazení. Více workerů/HA není podporováno.

Store se zapisuje přes dočasný soubor, fsync a atomický rename; obsah včetně všech logů je šifrovaný. Zálohujte `state.enc` a samostatně šifrovací klíč; nepřenášejte je společně nechráněným kanálem. Službu před obnovou zastavte. Request receipts a historie se automaticky nemažou (zachování idempotence/rollbacků); sledujte místo na disku. Pro velké objemy je potřeba databázová/archivní varianta. Nedostatek místa zastaví worker, místo aby pokračoval s neuloženým stavem. Selhání synchronizace adresáře po rename zablokuje další zápisy až do restartu, aby se nerozešel stav v paměti a na disku.

## Bezpečné vzdálené ověření před produkcí

1. Zprovozněte soukromý registry push účet a Runtime pull-only účet. Ověřte dostupnost architektury obrazu pro VPS.
2. Získejte scoped token a potvrďte URL sandboxu. Použijte `RUNTIME_SANDBOX=true`; adaptér přidá `X-Socen-Sandbox: true` ke **každému** requestu včetně mutací. Použijte vyhrazený state directory; nemíchejte sandbox a produkční záznamy.
3. Proveďte obě ukázkové aplikace přes šest portálových endpointů, ověřte odpovědi Runtime podle dokumentace adaptéru a kontrolu healthy release + route revision. Zopakujte stejný request, zastavte/restartujte worker při čekání a ověřte stejné ID.
4. Ověřte kandidáta s chybným health endpointem a rollback. Starší release má dál obsluhovat route; nový nesmí být označen online.
5. Teprve po samostatné autorizaci skutečného nasazení nastavte definitivní produkční endpoint, nový store a `RUNTIME_SANDBOX=false`. Tato implementace ani její testy tento krok neprovádějí.

## Opakovatelné skutečné integrační testy

Lokální Docker test spustí jen vlastní dočasný registry na loopbacku, vygeneruje oddělené testovací identity pro push/pull, sestaví oba ukázkové projekty, publikuje a načte je podle digestu a ověří HTTP health v kontejnerech s omezenými právy. Existující kontejnery ani konfiguraci daemonu neupravuje. Vanilla registry používá Basic Auth bez oddělených read/write ACL; produkční pull-only oprávnění tím nejsou otestovaná.

```sh
OCI_DOCKER_INTEGRATION=1 go test -v ./internal/ocibuild -run TestDockerRegistryIntegration -count=1
```

Pro vzdálený sandbox lze explicitně zapnout následující test (jinak je přeskočený). Token se čte ze souboru, jeho hodnota není argument příkazu ani výstup testu. Před prvním mutujícím testem proveďte autorizovanou read-only kontrolu schémat a vyberte známý sandbox artifact; jeho image/digest nastavte v `RUNTIME_SANDBOX_ARTIFACT_IMAGE` a `RUNTIME_SANDBOX_ARTIFACT_DIGEST`. Test nepoužívá vymyšlený digest. Nezávislá transportní pojistka před každým requestem kontroluje přesný HTTPS host `scr.socen.eu`, API cestu a sandboxovou hlavičku; mutace omezí na přesná ID vlastních testovacích projektů v upsert/env/deployments. Test neresetuje sdílený sandbox ani nemanipuluje s produkčním node. Zanechá vlastní projekty s prefixem `prj_codex_` pro kontrolu výsledků. Sandbox používá simulované artifacty; tento test neslibuje skutečný pull z lokálního registru na VPS.

```sh
RUNTIME_TOKEN_FILE=/private/path/runtime-token RUN_RUNTIME_SANDBOX_TESTS=1 \
  go test -v ./internal/runtimeengine -run '^TestLiveSandbox$' -count=1
```

Test ověřuje upsert, úplnou náhradu a vymazání env bez vracení hodnot, idempotenci deploymentu, aktivní release/routu, oba Runtime log endpointy, serverové odmítnutí neplatného digestu, aktivně vyvolané selhání health checku se zachováním starého release a obnovení historického artifactu. Živý běh prošel 20. září 2026; výsledky a ID jsou v protokolu ověření.
