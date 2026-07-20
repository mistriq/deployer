package app

import "regexp"

type secretRedactionRule struct {
	pattern     *regexp.Regexp
	replacement string
}

var secretRedactors = []secretRedactionRule{
	{regexp.MustCompile(`(?i)(Authorization:\s*(?:Bearer|Basic)\s+)[^\s,;]+`), `${1}[REDACTED]`},
	{regexp.MustCompile(`(?i)(--(?:token|password|secret|api-key)(?:=|\s+))[^\s]+`), `${1}[REDACTED]`},
	{regexp.MustCompile(`(?i)([?&](?:access_token|api_key|apikey|password|secret|token)=)[^&#\s]+`), `${1}[REDACTED]`},
	{regexp.MustCompile(`(?i)(\b[A-Z0-9_]*(?:TOKEN|SECRET|PASSWORD|PASSWD|API_KEY|PRIVATE_KEY|DATABASE_URL|DSN|CREDENTIALS?)[A-Z0-9_]*\s*=\s*)(?:"[^"]*"|'[^']*'|[^\s]+)`), `${1}[REDACTED]`},
	{regexp.MustCompile(`(?i)("(?:access_token|api_key|apikey|authorization|credential|database_url|password|private_key|secret|token)"\s*:\s*)"[^"]*"`), `${1}"[REDACTED]"`},
	{regexp.MustCompile(`(?i)(\b(?:https?|postgres(?:ql)?|mysql|redis)://)[^/@\s:]+:[^/@\s]+@`), `${1}[REDACTED]@`},
	{regexp.MustCompile(`-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----`), `[REDACTED PRIVATE KEY]`},
	{regexp.MustCompile(`\b(?:dpl_|ghp_|gho_|ghu_|ghs_|github_pat_|sk_live_|xox[baprs]-)[A-Za-z0-9_-]{8,}\b`), `[REDACTED]`},
	{regexp.MustCompile(`\bAKIA[A-Z0-9]{16}\b`), `[REDACTED]`},
}

func redactSecrets(text string) string {
	for _, redactor := range secretRedactors {
		text = redactor.pattern.ReplaceAllString(text, redactor.replacement)
	}
	return text
}
