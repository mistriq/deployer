package ocibuild

import (
	"encoding/base64"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestGuardRunsInEveryGeneratedRecipe(t *testing.T) {
	static, _ := Dockerfile(BuildSpec{Kind: "static", OutputDir: "dist", Badge: true})
	guard := strings.Index(static, "node /tmp/molo-guard.js public /app/dist")
	if guard < 0 || guard > strings.Index(static, "molo-badge.js") || guard > strings.Index(static, "FROM nginxinc") {
		t.Fatalf("static sites are guarded in the build stage, before the badge:\n%s", static)
	}
	app, _ := Dockerfile(BuildSpec{Kind: "node-http"})
	guard = strings.Index(app, "node /tmp/molo-guard.js app /app/.next/static")
	if guard < 0 || guard < strings.Index(app, "RUN npm run build") || guard > strings.LastIndex(app, "FROM node") {
		t.Fatalf("apps are guarded after the build, in the build stage:\n%s", app)
	}
}

// Keys are assembled at run time so the repository never contains anything that looks like a real key.
func fakeJWT(role string) string {
	part := func(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }
	return part(`{"alg":"HS256","typ":"JWT"}`) + "." + part(`{"iss":"supabase","ref":"abcdefghijklmnop","role":"`+role+`","iat":1700000000}`) + "." + strings.Repeat("s", 43)
}

func runGuard(t *testing.T, args ...string) (string, error) {
	t.Helper()
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node not installed")
	}
	script := filepath.Join(t.TempDir(), "guard.js")
	os.WriteFile(script, []byte(guardScript), 0o644)
	out, err := exec.Command("node", append([]string{script}, args...)...).CombinedOutput()
	return string(out), err
}

func TestGuardKeepsPrivateFilesOffTheSite(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{".env", ".git/config", "backup.sql", "keys/server.pem", "id_rsa", "index.php.bak", ".well-known/security.txt", "index.html", "assets/app.js"} {
		os.MkdirAll(filepath.Dir(filepath.Join(dir, name)), 0o755)
		os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644)
	}
	// A live key in .env is fine once .env is no longer published.
	os.WriteFile(filepath.Join(dir, ".env"), []byte("STRIPE_SECRET_KEY=sk_"+"live_"+strings.Repeat("a1", 12)), 0o644)
	out, err := runGuard(t, "public", dir, filepath.Join(dir, "missing"))
	if err != nil {
		t.Fatalf("nothing secret is left, the build passes: %v\n%s", err, out)
	}
	for _, gone := range []string{".env", ".git", "backup.sql", "keys/server.pem", "id_rsa", "index.php.bak"} {
		if _, e := os.Stat(filepath.Join(dir, gone)); e == nil {
			t.Fatalf("%s is still published", gone)
		}
		if !strings.Contains(out, "kept private (not published): "+gone) {
			t.Fatalf("%s is not named in the log:\n%s", gone, out)
		}
	}
	for _, kept := range []string{".well-known/security.txt", "index.html", "assets/app.js", "keys"} {
		if _, e := os.Stat(filepath.Join(dir, kept)); e != nil {
			t.Fatalf("%s was removed", kept)
		}
	}
}

func TestGuardStopsSecretKeysInBrowserFiles(t *testing.T) {
	stripe := "sk_" + "live_" + strings.Repeat("Z9", 14)
	cases := map[string]string{
		"a Stripe secret key":         `const stripe = Stripe("` + stripe + `")`,
		"a Supabase service_role key": "createClient(url, '" + fakeJWT("service_role") + "')",
		"a private key":               "-----BEGIN " + "PRIVATE KEY-----\nMIIE",
	}
	for label, content := range cases {
		dir := t.TempDir()
		os.MkdirAll(filepath.Join(dir, "assets"), 0o755)
		os.WriteFile(filepath.Join(dir, "assets", "index-4f2a.js"), []byte("// build\n\n"+content), 0o644)
		out, err := runGuard(t, "app", filepath.Join(dir, "missing"), dir)
		if err == nil {
			t.Fatalf("%s must stop the build:\n%s", label, out)
		}
		if !strings.Contains(out, "molo-guard: secret found: "+label+" in assets/index-4f2a.js (line 3)") || !strings.Contains(out, "Your live version did not change.") {
			t.Fatalf("the log must name %s, the file and the line:\n%s", label, out)
		}
		if strings.Contains(out, stripe) || strings.Contains(out, "c2VydmljZV9yb2xl") {
			t.Fatal("the log must never repeat the key")
		}
	}
}

func TestGuardLetsPublicKeysThrough(t *testing.T) {
	dir := t.TempDir()
	public := `createClient("https://x.supabase.co", "` + fakeJWT("anon") + `"); Stripe("pk_` + `live_` + strings.Repeat("a1", 12) + `"); const k = "sb_publishable_abcdefghijklmnopqrstuv"; const test = "sk_` + `test_` + strings.Repeat("a1", 12) + `";`
	os.WriteFile(filepath.Join(dir, "app.js"), []byte(public), 0o644)
	os.WriteFile(filepath.Join(dir, "logo.png"), append([]byte{0x89, 'P', 'N', 'G', 0}, []byte("sk_"+"live_"+strings.Repeat("a1", 12))...), 0o644)
	if out, err := runGuard(t, "public", dir); err != nil {
		t.Fatalf("publishable, anon, test keys and binary files pass:\n%s", out)
	}
}
