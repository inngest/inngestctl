package schema

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/replicase/pgcapture/pkg/sql"
)

var QueryTableOIDs = `SELECT oid, relname, reltuples
FROM pg_class
WHERE relname NOT IN ('pg_catalog', 'information_schema')
AND relname NOT LIKE 'pg_toast%' AND relkind = 'r';`

func NewPGXSchemaLoader(conn *pgx.Conn) *PGXSchemaLoader {
	return &PGXSchemaLoader{
		conn:   conn,
		types:  make(TypeCache),
		iKeys:  make(KeysCache),
		tables: make(TableCache),
	}
}

type PGXSchemaLoader struct {
	conn   *pgx.Conn
	types  TypeCache
	iKeys  KeysCache
	tables TableCache
}

func (p *PGXSchemaLoader) Refresh() error {
	if err := p.RefreshType(); err != nil {
		return err
	}
	if err := p.RefreshColumnInfo(); err != nil {
		return err
	}
	if err := p.RefreshTables(); err != nil {
		return err
	}
	return nil
}

func (p *PGXSchemaLoader) String() string {
	sb := &strings.Builder{}
	for schema, tables := range p.types {

		n := 0
		for table, cols := range tables {
			_, _ = sb.WriteString(schema + "." + table + " (\n")
			for col, typ := range cols {
				sb.WriteString("\t" + col + " " + OIDToTypeName(typ.OID) + "\n")
			}
			sb.WriteString(")")
			n++
			if n < len(tables) {
				sb.WriteString("\n")
			}
		}
	}
	return sb.String()
}

func (p *PGXSchemaLoader) GetTableFromOID(oid uint32) (*TableInfo, bool) {
	ti, ok := p.tables[oid]
	if !ok {
		return nil, false
	}
	return &ti, true
}

func (p *PGXSchemaLoader) GetTypeInfo(namespace, table, field string) (*TypeInfo, error) {
	if tbls, ok := p.types[namespace]; !ok {
		return nil, fmt.Errorf("%s.%s %w", namespace, table, ErrSchemaNamespaceMissing)
	} else if cols, ok := tbls[table]; !ok {
		return nil, fmt.Errorf("%s.%s %w", namespace, table, ErrSchemaTableMissing)
	} else if typeInfo, ok := cols[field]; !ok {
		return nil, fmt.Errorf("%s.%s.%s %w", namespace, table, field, ErrSchemaColumnMissing)
	} else {
		return &typeInfo, nil
	}
}

func (p *PGXSchemaLoader) GetColumnInfo(namespace, table string) (*ColumnInfo, error) {
	if tbls, ok := p.iKeys[namespace]; !ok {
		return nil, fmt.Errorf("%s.%s %w", namespace, table, ErrSchemaIdentityMissing)
	} else if info, ok := tbls[table]; !ok {
		return nil, fmt.Errorf("%s.%s %w", namespace, table, ErrSchemaIdentityMissing)
	} else {
		return &info, nil
	}
}

func (p *PGXSchemaLoader) GetTableKey(namespace, table string) (keys []string, err error) {
	if tbls, ok := p.iKeys[namespace]; !ok {
		return nil, fmt.Errorf("%s.%s %w", namespace, table, ErrSchemaIdentityMissing)
	} else if info, ok := tbls[table]; !ok {
		return nil, fmt.Errorf("%s.%s %w", namespace, table, ErrSchemaIdentityMissing)
	} else {
		return info.ListKeys(), nil
	}
}

func (p *PGXSchemaLoader) GetVersion() (version int64, err error) {
	var versionInfo string
	if err = p.conn.QueryRow(context.Background(), sql.ServerVersionNum).Scan(&versionInfo); err != nil {
		return -1, err
	}
	svn, err := strconv.ParseInt(versionInfo, 10, 64)
	if err != nil {
		return -1, err
	}
	return svn, nil
}

func (p *PGXSchemaLoader) RefreshTables() error {
	rows, err := p.conn.Query(context.Background(), QueryTableOIDs)
	if err != nil {
		return err
	}
	defer rows.Close()

	var (
		oid      uint32
		relname  string
		rowcount int64
	)
	for rows.Next() {
		if err := rows.Scan(&oid, &relname, &rowcount); err != nil {
			return err
		}
		p.tables[oid] = TableInfo{
			OID:            oid,
			Name:           relname,
			ApproxRowCount: uint64(rowcount),
		}
	}
	return nil
}

func (p *PGXSchemaLoader) RefreshType() error {
	rows, err := p.conn.Query(context.Background(), sql.QueryAttrTypeOID)
	if err != nil {
		return err
	}
	defer rows.Close()

	var (
		nspname, relname, attname string
		atttypid                  uint32
		relreplident              ReplicaIdentity
	)
	for rows.Next() {
		if err := rows.Scan(&nspname, &relname, &attname, &atttypid, &relreplident); err != nil {
			return err
		}
		tbls, ok := p.types[nspname]
		if !ok {
			tbls = make(map[string]map[string]TypeInfo)
			p.types[nspname] = tbls
		}
		cols, ok := tbls[relname]
		if !ok {
			cols = make(map[string]TypeInfo)
			tbls[relname] = cols
		}
		cols[attname] = TypeInfo{
			OID:             atttypid,
			ReplicaIdentity: relreplident,
		}
	}
	return nil
}

func (p *PGXSchemaLoader) RefreshColumnInfo() error {
	rows, err := p.conn.Query(context.Background(), sql.QueryIdentityKeys)
	if err != nil {
		return err
	}
	defer rows.Close()

	var nspname, relname string
	for rows.Next() {
		var (
			keys                      pgtype.Array[pgtype.Text]
			identityGenerationColumns pgtype.Array[pgtype.Text]
			generatedColumns          pgtype.Array[pgtype.Text]
		)
		if err := rows.Scan(&nspname, &relname, &keys, &identityGenerationColumns, &generatedColumns); err != nil {
			return err
		}
		tbls, ok := p.iKeys[nspname]
		if !ok {
			tbls = make(map[string]ColumnInfo)
			p.iKeys[nspname] = tbls
		}

		tbls[relname] = ColumnInfo{
			keys:                   fieldSetWithList(keys),
			identityGenerationList: fieldSetWithList(identityGenerationColumns),
			generatedList:          fieldSetWithList(generatedColumns),
		}
	}
	return nil
}

var (
	ErrSchemaNamespaceMissing = errors.New("namespace missing")
	ErrSchemaTableMissing     = errors.New("table missing")
	ErrSchemaColumnMissing    = errors.New("column missing")
	ErrSchemaIdentityMissing  = errors.New("table identity keys missing")
)
