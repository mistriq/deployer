package ocibuild

import (
	"encoding/base64"
	"strings"
)

// The leak guard runs in the build stage of every generated recipe, after the build and before anything is
// copied into the image that visitors reach:
//   - "public" mode (static sites) deletes private files from the published folder: dotfiles such as .env or
//     .git (except .well-known), private keys, database dumps and backups. Each one is named in the build log.
//   - Both modes then read the files a browser can download and stop the build when one holds a secret key
//     (a live Stripe key, a Supabase service_role or secret key, an OpenAI, Anthropic, GitHub or SendGrid key,
//     a private key). The log names the kind of key, the file and the line, never the key itself.
//
// A stopped build never replaces the live version. Public keys (Stripe publishable, Supabase anon or
// publishable) are meant for browsers and pass.
const guardScript = `const fs=require("fs"),path=require("path");
const [given,...roots]=process.argv.slice(2),mode=given.replace(/\+allow$/,""),allow=given.endsWith("+allow");
const privateFile=/^(\.(?!well-known$).+|id_(rsa|dsa|ecdsa|ed25519)|.+\.(pem|key|p12|pfx|sql|sqlite|sqlite3|db|bak|old|orig|swp|log)|.+~)$/i;
const jwtRole=token=>{try{return JSON.parse(Buffer.from(token.split(".")[1],"base64url").toString("utf8")).role;}catch{return null;}};
const rules=[
["a Stripe secret key",/\b[rs]k_live_[0-9A-Za-z]{20,255}/g],
["a Supabase service_role key",/\beyJ[0-9A-Za-z_-]{10,1000}\.eyJ[0-9A-Za-z_-]{20,4000}\.[0-9A-Za-z_-]{20,1000}/g,token=>jwtRole(token)==="service_role"],
["a Supabase secret key",/\bsb_secret_[0-9A-Za-z_-]{20,200}/g],
["an OpenAI API key",/\bsk-[0-9A-Za-z_-]{16,300}T3BlbkFJ[0-9A-Za-z_-]{16,300}/g],
["an Anthropic API key",/\bsk-ant-(?:api|admin)\d\d-[0-9A-Za-z_-]{80,300}/g],
["a GitHub token",/\b(?:gh[opsu]_[0-9A-Za-z]{36}|github_pat_[0-9A-Za-z_]{60,255})/g],
["a SendGrid API key",/\bSG\.[0-9A-Za-z_-]{22}\.[0-9A-Za-z_-]{43}/g],
["a private key",/-----BEGIN (?:RSA |EC |DSA |OPENSSH |ENCRYPTED )?PRIVATE KEY-----/g]];
const shown=p=>p.replace(/[^\w./@+-]/g,"?").slice(0,160);
const found=[];let files=0;
function walk(root,dir){let entries;try{entries=fs.readdirSync(dir,{withFileTypes:true});}catch{return;}
for(const entry of entries){const file=path.join(dir,entry.name),rel=path.relative(root,file);
if(mode==="public"&&privateFile.test(entry.name)){fs.rmSync(file,{recursive:true,force:true});console.log("molo-guard: kept private (not published): "+shown(rel));continue;}
if(entry.isSymbolicLink()||entry.name==="node_modules")continue;
if(entry.isDirectory()){walk(root,file);continue;}
if(!entry.isFile()||++files>50000)continue;
let data;try{if(fs.statSync(file).size>8388608)continue;data=fs.readFileSync(file);}catch{continue;}
if(data.subarray(0,8000).includes(0))continue;
const text=data.toString("utf8");
for(const [label,pattern,accept] of rules){pattern.lastIndex=0;let match;
// A file the scanner cannot read through is reported and skipped; it never fails the build.
try{while((match=pattern.exec(text))){if(accept&&!accept(match[0]))continue;
found.push(label+" in "+shown(rel)+" (line "+(text.slice(0,match.index).split("\n").length)+")");break;}}
catch{console.log("molo-guard: could not check "+shown(rel)+" for "+label);}}}}
for(const root of roots)if(fs.existsSync(root))walk(root,root);
if(found.length&&allow){for(const item of found.slice(0,20))console.log("molo-guard: published anyway: "+item);
console.log("molo-guard: the owner chose to deploy anyway. Anyone can copy these keys: treat them as leaked.");}
else if(found.length){for(const item of found.slice(0,20))console.log("molo-guard: secret found: "+item);
console.log("molo-guard: stopped before going live. Visitors' browsers can read these files, so anyone could copy the key. Remove it from the code, create a new key with its provider, and keep it on a server (Environment variables). Your live version did not change.");
process.exit(1);}
`

// guardMode adds "+allow" when the owner chose "Deploy anyway" (BuildSpec.AllowSecrets).
func guardMode(mode string, allow bool) string {
	if allow {
		return mode + "+allow"
	}
	return mode
}

// guardStep is the RUN line for the build stage (node is available there). mode is "public" for a published
// folder, "app" for the browser files of a server app; paths are absolute folders, missing ones are skipped.
func guardStep(mode string, paths ...string) string {
	script := base64.StdEncoding.EncodeToString([]byte(guardScript))
	return "RUN echo " + script + " | base64 -d > /tmp/molo-guard.js && node /tmp/molo-guard.js " + mode + " " + strings.Join(paths, " ") + " && rm -f /tmp/molo-guard.js\n"
}

// Folders that server frameworks send to browsers as they are: Next.js, Nuxt and Nitro (TanStack Start),
// Vite SSR, React Router, SvelteKit, and the public/ folder most of them serve.
var appBrowserDirs = []string{"/app/.next/static", "/app/.output/public", "/app/dist/client", "/app/build/client", "/app/.svelte-kit/output/client", "/app/public"}
