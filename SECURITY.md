# Security Policy

Deployer is a deployment tool. Treat access to the server UI/API and runner
tokens as production-level privileges.

## Supported Versions

Security fixes are provided for the latest released version only until the
project has a formal release policy.

## Reporting Vulnerabilities

Please report security issues privately by opening a GitHub Security Advisory
for this repository, or by contacting the maintainer through the repository's
published contact channel.

Do not open public issues for vulnerabilities that expose secrets, enable
unauthorized deploys, compromise runners, or allow arbitrary file writes or
command execution.

## Operational Guidance

- Run the server behind HTTPS.
- Set `DEPLOYER_ADMIN_PASSWORD` when exposing the UI/API beyond localhost.
- Keep runner tokens private and rotate them if they appear in logs, URLs, shell
  history, screenshots, or support bundles.
- Do not publish `deployer.db`, build logs, screenshots, or local systemd/env
  files.
