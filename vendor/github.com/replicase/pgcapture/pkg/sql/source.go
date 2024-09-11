package sql

var QueryAttrTypeOID = `SELECT nspname, relname, attname, atttypid, relreplident
FROM pg_catalog.pg_namespace n
JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid AND c.relkind = 'r'
JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 and a.attisdropped = false
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pglogical') AND n.nspname !~ '^pg_toast';`

var QueryIdentityKeys = `SELECT
	nspname,
	relname,
	array(select attname from pg_catalog.pg_attribute where attrelid = i.indrelid AND attnum > 0 AND attnum = ANY(i.indkey)) as keys,
	array(select column_name::text from information_schema.columns where table_schema = n.nspname AND table_name = c.relname AND identity_generation IS NOT NULL) as identity_generation_columns,
	array(select column_name::text from information_schema.columns where table_schema = n.nspname AND table_name = c.relname AND is_generated = 'ALWAYS') as generated_columns
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_class c ON c.oid = i.indrelid AND c.relkind = 'r'
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pglogical') AND n.nspname !~ '^pg_toast'
WHERE (i.indisprimary OR i.indisunique) AND i.indisvalid AND i.indpred IS NULL ORDER BY indisprimary;`

var CreateLogicalSlot = `SELECT pg_create_logical_replication_slot($1, $2);`

var CreatePublication = `CREATE PUBLICATION %s FOR ALL TABLES;`

var InstallExtension = `CREATE EXTENSION IF NOT EXISTS pgcapture;`

var ServerVersionNum = `SHOW server_version_num;`

var QueryReplicationSlot = `SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1;`
