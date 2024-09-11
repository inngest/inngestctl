[![](https://godoc.org/github.com/jackc/pglogrepl?status.svg)](https://godoc.org/github.com/jackc/pglogrepl)
[![CI](https://github.com/jackc/pglogrepl/actions/workflows/ci.yml/badge.svg)](https://github.com/jackc/pglogrepl/actions/workflows/ci.yml)

# pglogrepl

pglogrepl is a Go package for PostgreSQL logical replication.

pglogrepl uses package github.com/jackc/pgx/v5/pgconn as its underlying PostgreSQL connection.

Proper use of this package requires understanding the underlying PostgreSQL concepts. See
https://www.postgresql.org/docs/current/protocol-replication.html.

## Example

In `example/pglogrepl_demo`, there is an example demo program that connects to a database and logs all messages sent over logical replication.
In `example/pgphysrepl_demo`, there is an example demo program that connects to a database and logs all messages sent over physical replication.

## Testing

Testing requires a user with replication permission, a database to replicate, access allowed in `pg_hba.conf`, and
logical replication enabled in `postgresql.conf`.

Create a database:

```
create database pglogrepl;
```

Create a user:

```
create user pglogrepl with replication password 'secret';
```

If you're using PostgreSQL 15 or newer grant access to the public schema, just for these tests:

```
grant all on schema public to pglogrepl;
```

Add a replication line to your pg_hba.conf:

```
host replication pglogrepl 127.0.0.1/32 md5
```

Change the following settings in your postgresql.conf:

```
wal_level=logical
max_wal_senders=5
max_replication_slots=5
```

To run the tests set `PGLOGREPL_TEST_CONN_STRING` environment variable with a replication connection string (URL or DSN).

Since the base backup would request postgres to create a backup tar and stream it, this test cn be disabled with
```
PGLOGREPL_SKIP_BASE_BACKUP=true
```

Example:

```
PGLOGREPL_TEST_CONN_STRING=postgres://pglogrepl:secret@127.0.0.1/pglogrepl?replication=database go test
```
