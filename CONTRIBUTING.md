# Contributing

Thanks for helping improve Deployer.

## Development

```bash
go test ./...
go vet ./...
go test -race ./...
go run golang.org/x/vuln/cmd/govulncheck@v1.3.0 ./...
```

Use a disposable PostgreSQL database while developing:

```bash
DEPLOYER_DATABASE_URL='postgres://deployer:password@127.0.0.1:5432/deployer_dev?sslmode=disable' go run ./cmd/deployer
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
