#!/usr/bin/env bash
set -euo pipefail
umask 077

deployer_backup_dir="${DEPLOYER_BACKUP_DIR:-$HOME/.local/share/deployer/postgresql-backups}"
deployer_db_container="${DEPLOYER_POSTGRES_CONTAINER:-deployer-production-postgres-1}"
mkdir -p "$deployer_backup_dir"
chmod 700 "$deployer_backup_dir"
deployer_backup_stamp="$(date -u +%Y%m%dT%H%M%SZ)"
deployer_backup_target="$deployer_backup_dir/deployer-$deployer_backup_stamp.dump"
deployer_backup_temp="$(mktemp "$deployer_backup_dir/.backup-XXXXXX")"
trap 'rm -f "$deployer_backup_temp"' EXIT
docker exec "$deployer_db_container" pg_dump -U deployer -d deployer --format=custom > "$deployer_backup_temp"
docker exec -i "$deployer_db_container" pg_restore --list < "$deployer_backup_temp" >/dev/null
mv "$deployer_backup_temp" "$deployer_backup_target"
if [[ -f "$HOME/.config/deployer/deployer.env" ]]; then
    install -m 600 "$HOME/.config/deployer/deployer.env" "$deployer_backup_dir/deployer-$deployer_backup_stamp.env"
fi
printf 'Verified PostgreSQL backup: %s\n' "$deployer_backup_target"
