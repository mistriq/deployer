# Contributing

Thanks for helping improve Deployer.

## Development

```bash
go test ./...
go vet ./...
go test -race ./...
go run golang.org/x/vuln/cmd/govulncheck@v1.3.0 ./...
```

Use a temporary database while developing:

```bash
DEPLOYER_DB_PATH=/tmp/deployer-dev.db go run .
```

## Pull Requests

- Keep changes focused.
- Include tests for storage, authentication, runner protocol, packaging, and
  deployment behavior when touching those areas.
- Do not commit databases, binaries, logs, screenshots with real data, secrets,
  or machine-specific config.
- Run the verification commands before submitting.

## Security-Sensitive Changes

Changes to authentication, runner tokens, artifact transfer, archive extraction,
shell execution, auto-update, and deployment commands need extra review and
tests.
