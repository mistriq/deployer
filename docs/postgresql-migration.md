# PostgreSQL migration

PostgreSQL is the supported production database. SQLite remains available only
for automated tests and as the read-only source of the migration command. New
production features do not need to maintain a second SQLite implementation.

## Rehearse first

1. Stop Deployer so the SQLite file has no writers. Confirm the service is
   stopped before taking the backup.
2. Copy the database file and its `-wal` and `-shm` companions, when present, to
   protected storage. Keep the original files in place and record checksums.
3. Create an empty rehearsal PostgreSQL database and set
   `DEPLOYER_DATABASE_URL` to its connection URL.
4. Run `deployer migrate-sqlite --source /path/to/deployer.db`. The importer
   opens SQLite read-only, refuses unknown tables or a non-empty PostgreSQL
   target, preserves explicit IDs and token hashes, checks row counts, canonical
   row-content digests, and foreign keys in one transaction, and advances
   PostgreSQL identity sequences. Timestamp digest comparison normalizes values
   to PostgreSQL's microsecond precision while retaining the same instant.
5. Start a rehearsal Deployer instance against PostgreSQL. Authenticate an
   existing runner, inspect representative projects and historical builds, then
   run a deployment against a disposable repository, runner, and target.
6. Stop the rehearsal instance and start it again. Verify build history and
   runner authentication once more.

Do not use a live deploy directory for the rehearsal.

## Cut over

Repeat the stop, backup, checksum, and import steps using a fresh production
PostgreSQL database. Keep all writes paused until the verification report is
complete. Configure the service with `DEPLOYER_DATABASE_URL`, start it, and
verify project/runner counts, recent builds, annotations, a runner heartbeat,
and one controlled deployment.

The import is intentionally repeatable by recreating an empty target database
and running it again. It never changes the SQLite source.

## Recover

If import or validation fails, stop the PostgreSQL-backed service. Restore the
unchanged SQLite backup and the previous service configuration, then start the
previous application version. Keep the failed PostgreSQL database for diagnosis;
do not retry into that partially used database. No source data needs to be
restored because the importer only opens it in read-only mode.
