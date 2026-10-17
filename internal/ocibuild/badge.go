package ocibuild

import "encoding/base64"

// The "Running on molo" badge for sites on Molo's free plan. It is plain HTML and CSS (no script, no
// tracking, no external request) so it works under any site's JavaScript and cannot break it; visitors
// can hide it with the ✕ (a checkbox, no JavaScript). The portal asks for it with BuildSpec.Badge.
const badgeHTML = `<div data-molo-badge style="all:initial"><input type="checkbox" id="molo-badge-hide" style="display:none" aria-hidden="true"><style>` +
	`#molo-badge-hide:checked~.molo-badge{display:none}` +
	`.molo-badge{all:initial;position:fixed;right:16px;bottom:16px;z-index:2147483647;display:flex;align-items:center;gap:4px;padding:5px 5px 5px 12px;border-radius:999px;background:#0a0a0a;box-shadow:0 8px 28px rgba(0,0,0,.22),inset 0 0 0 1px rgba(255,255,255,.09);font:600 13px/1 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;color:#fff;animation:molo-badge-in .6s cubic-bezier(.2,.8,.2,1) .4s both}` +
	`.molo-badge a{all:unset;display:flex;align-items:center;gap:7px;cursor:pointer;color:#fff;padding:3px 0}` +
	`.molo-badge svg{display:block;flex:none}` +
	`.molo-badge-label{all:unset;font:500 12px/1 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;color:rgba(255,255,255,.62)}` +
	`.molo-badge-word{all:unset;font:700 14px/1 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;letter-spacing:-.04em;color:#fff}` +
	`.molo-badge-dot{all:unset;display:inline-block;width:5px;height:5px;margin-left:1px;border-radius:50%;background:#f24602;transform:translateY(3px)}` +
	`.molo-badge a:hover .molo-badge-word{text-decoration:underline;text-decoration-color:#f24602;text-underline-offset:3px}` +
	`.molo-badge-x{all:unset;display:flex;align-items:center;justify-content:center;width:24px;height:24px;margin-left:4px;border-radius:50%;cursor:pointer;color:rgba(255,255,255,.5);font:400 16px/1 sans-serif}` +
	`.molo-badge-x:hover{background:rgba(255,255,255,.12);color:#fff}` +
	`@keyframes molo-badge-in{from{opacity:0;transform:translateY(10px)}}` +
	`@media (prefers-reduced-motion:reduce){.molo-badge{animation:none}}` +
	`@media print{.molo-badge{display:none}}` +
	`</style><div class="molo-badge" role="complementary" aria-label="Molo">` +
	`<a href="https://gomolo.eu/?ref=badge" target="_blank" rel="noopener">` +
	`<svg width="16" height="14" viewBox="0 0 16 14" aria-hidden="true"><g fill="#fff"><path d="M4.2 0h2.4L3.6 14H1.2z"/><path d="M8.4 3h2.4L8.4 14H6z"/><path d="M12.6 6H15l-1.7 8h-2.4z"/></g></svg>` +
	`<span class="molo-badge-label">{{label}}</span><span class="molo-badge-word">molo</span><span class="molo-badge-dot"></span></a>` +
	`<label class="molo-badge-x" for="molo-badge-hide" title="{{hide}}" aria-label="{{hide}}">×</label></div></div>`

// Runs in the build stage (node:22-alpine): inserts the badge before </body> of every HTML file of the
// output directory, once. Pages with lang="cs" get the Czech label.
const badgeScript = `const fs=require("fs"),path=require("path");
const root=process.argv[2],badge=Buffer.from(process.argv[3],"base64").toString("utf8");
const label=html=>/<html[^>]*\blang=["']?cs/i.test(html)?{label:"Běží na",hide:"Skrýt"}:{label:"Running on",hide:"Hide"};
function walk(dir){for(const entry of fs.readdirSync(dir,{withFileTypes:true})){const file=path.join(dir,entry.name);
if(entry.isSymbolicLink())continue;if(entry.isDirectory()){walk(file);continue;}if(!/\.html?$/i.test(entry.name))continue;
let html=fs.readFileSync(file,"utf8");if(html.includes("data-molo-badge"))continue;const words=label(html);
const piece=badge.replace(/\{\{label\}\}/g,words.label).replace(/\{\{hide\}\}/g,words.hide);
const at=html.toLowerCase().lastIndexOf("</body>");html=at<0?html+piece:html.slice(0,at)+piece+html.slice(at);fs.writeFileSync(file,html);}}
if(fs.existsSync(root))walk(root);
`

// badgeStep is the RUN line that badges every HTML page under /app/<dir> in the build stage.
func badgeStep(dir string) string {
	script := base64.StdEncoding.EncodeToString([]byte(badgeScript))
	html := base64.StdEncoding.EncodeToString([]byte(badgeHTML))
	return "RUN echo " + script + " | base64 -d > /tmp/molo-badge.js && node /tmp/molo-badge.js /app/" + dir + " " + html + " && rm -f /tmp/molo-badge.js\n"
}
