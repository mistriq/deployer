package main

import "regexp"

var secretRedactors = []*regexp.Regexp{
	regexp.MustCompile(`(?i)(Authorization:\s*Bearer\s+)[^\s]+`),
	regexp.MustCompile(`(?i)(DEPLOYER_TOKEN=)[^\s]+`),
	regexp.MustCompile(`(?i)(token=)[A-Za-z0-9._~+/=-]+`),
	regexp.MustCompile(`(?i)(--token\s+)[^\s]+`),
}

func redactSecrets(text string) string {
	for _, redactor := range secretRedactors {
		text = redactor.ReplaceAllString(text, `${1}[REDACTED]`)
	}
	return text
}
