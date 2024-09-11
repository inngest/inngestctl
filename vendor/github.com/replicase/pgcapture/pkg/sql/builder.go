package sql

import (
	"strconv"
	"strings"

	"github.com/replicase/pgcapture/pkg/pb"
)

func DeleteQuery(namespace, table string, fields []*pb.Field) string {
	var query strings.Builder
	query.WriteString("delete from \"")
	query.WriteString(namespace)
	query.WriteString("\".\"")
	query.WriteString(table)
	query.WriteString("\" where \"")

	for i, field := range fields {
		query.WriteString(field.Name)
		query.WriteString("\"=$" + strconv.Itoa(i+1))
		if i != len(fields)-1 {
			query.WriteString(" and \"")
		}
	}
	return query.String()
}

func UpdateQuery(namespace, table string, sets, keys []*pb.Field) string {
	var query strings.Builder
	query.WriteString("update \"")
	query.WriteString(namespace)
	query.WriteString("\".\"")
	query.WriteString(table)
	query.WriteString("\" set \"")

	var j int
	for ; j < len(sets); j++ {
		field := sets[j]
		query.WriteString(field.Name)
		query.WriteString("\"=$" + strconv.Itoa(j+1))
		if j != len(sets)-1 {
			query.WriteString(",\"")
		}
	}

	query.WriteString(" where \"")

	for i := 0; i < len(keys); i++ {
		k := i + j
		field := keys[i]

		query.WriteString(field.Name)
		query.WriteString("\"=$" + strconv.Itoa(k+1))
		if i != len(keys)-1 {
			query.WriteString(" and \"")
		}
	}

	return query.String()
}

type InsertOption struct {
	Namespace string
	Table     string
	Count     int
	Keys      []string
	Fields    []*pb.Field
	PGVersion int64
}

func InsertQuery(opt InsertOption) string {
	var query strings.Builder
	query.WriteString("insert into \"")
	query.WriteString(opt.Namespace)
	query.WriteString("\".\"")
	query.WriteString(opt.Table)
	query.WriteString("\"(\"")

	fields := opt.Fields
	for i, field := range fields {
		query.WriteString(field.Name)
		if i == len(fields)-1 {
			query.WriteString("\")")
		} else {
			query.WriteString("\",\"")
		}
	}

	if opt.PGVersion >= 100000 {
		// to handle the case where the target table contains the GENERATED ALWAYS constraint;
		// according the SQL standard, the OVERRIDING SYSTEM VALUE can only be specified if an identity column that is generated always exists,
		// but PG will allow the clause to be specified even if the target table does not contain such a column.
		// ref: https://www.postgresql.org/docs/10/sql-insert.html
		query.WriteString(" OVERRIDING SYSTEM VALUE")
	}
	query.WriteString(" values (")

	i := 1
	for j := 0; j < opt.Count; j++ {
		for range fields {
			query.WriteString("$" + strconv.Itoa(i))
			if i%len(fields) == 0 {
				query.WriteString(")")
			} else {
				query.WriteString(",")
			}
			i++
		}
		if j < opt.Count-1 {
			query.WriteString(",(")
		}
	}

	keys := opt.Keys
	if len(keys) != 0 {
		query.WriteString(" ON CONFLICT (")
		query.WriteString(strings.Join(keys, ","))
		query.WriteString(") DO NOTHING")
	}

	return query.String()
}
