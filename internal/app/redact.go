package app

import "regexp"

var secretRedactors = []*regexp.Regexp{
	regexp.MustCompile(`(?i)((?:https?|postgres(?:ql)?|redis|amqp)://)[^/\s:@]+:[^/\s@]+@`),
	regexp.MustCompile(`(?i)(https://(?:hooks\.slack\.com/services|discord\.com/api/webhooks)/)[^\s"']+`),
	regexp.MustCompile(`(?i)(Authorization:\s*Bearer\s+)[^\s]+`),
	regexp.MustCompile(`(?i)(DEPLOYER_TOKEN=)[^\s]+`),
	regexp.MustCompile(`(?i)(token=)[A-Za-z0-9._~+/=-]+`),
	regexp.MustCompile(`(?i)(--token\s+)[^\s]+`),
	regexp.MustCompile(`(?i)((?:password|passwd|api[_-]?key|client[_-]?secret|[a-z0-9_]*secret|[a-z0-9_]*token)\s*[=:]\s*)(?:"[^"]*"|'[^']*'|[^\s,;]+)`),
}

func redactSecrets(text string) string {
	for _, redactor := range secretRedactors {
		text = redactor.ReplaceAllString(text, `${1}[REDACTED]`)
	}
	return text
}
