package schema

import (
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/replicase/pgcapture/pkg/pb"
)

type ReplicaIdentity rune

const (
	ReplicaIdentityDefault ReplicaIdentity = 'd'
	ReplicaIdentityFull    ReplicaIdentity = 'f'
	ReplicaIdentityIndex   ReplicaIdentity = 'i'
	ReplicaIdentityNothing ReplicaIdentity = 'n'
)

// TableInfo represents table information within the introspected DB.  This allows us to
// look up OID -> table names for eg. update messages.
type TableInfo struct {
	OID            uint32
	Name           string
	ApproxRowCount uint64
}

type TypeInfo struct {
	OID             uint32
	ReplicaIdentity ReplicaIdentity
}

type TypeCache map[string]map[string]map[string]TypeInfo
type KeysCache map[string]map[string]ColumnInfo
type TableCache map[uint32]TableInfo

type fieldSet struct {
	set map[string]struct{}
}

func fieldSetWithList(list pgtype.Array[pgtype.Text]) fieldSet {
	s := fieldSet{set: make(map[string]struct{}, len(list.Elements))}
	for _, v := range list.Elements {
		s.append(v.String)
	}
	return s
}

func (s fieldSet) Contains(f string) bool {
	_, ok := s.set[f]
	return ok
}

func (s fieldSet) append(f string) {
	s.set[f] = struct{}{}
}

func (s fieldSet) list() []string {
	list := make([]string, 0, len(s.set))
	for k := range s.set {
		list = append(list, k)
	}
	return list
}

func (s fieldSet) Len() int {
	return len(s.set)
}

type ColumnInfo struct {
	keys                   fieldSet
	identityGenerationList fieldSet
	generatedList          fieldSet
}

func (i ColumnInfo) IsGenerated(f string) bool {
	return i.generatedList.Contains(f)
}

func (i ColumnInfo) IsIdentityGeneration(f string) bool {
	return i.identityGenerationList.Contains(f)
}

func (i ColumnInfo) IsKey(f string) bool {
	return i.keys.Contains(f)
}

func (i ColumnInfo) ListKeys() []string {
	return i.keys.list()
}

func (i ColumnInfo) KeyLength() int {
	return i.keys.Len()
}

func (i ColumnInfo) isEmpty() bool {
	return i.keys.Len() == 0 && i.generatedList.Len() == 0 && i.identityGenerationList.Len() == 0
}

type fieldSelector func(i ColumnInfo, field string) bool

func (i ColumnInfo) Filter(fields []*pb.Field, fieldSelector fieldSelector) (fieldSet, []*pb.Field) {
	if i.isEmpty() {
		return fieldSet{}, fields
	}
	cols := make([]string, 0, len(fields))
	fFields := make([]*pb.Field, 0, len(fields))
	for _, f := range fields {
		if fieldSelector(i, f.Name) {
			cols = append(cols, f.Name)
			fFields = append(fFields, f)
		}
	}

	set := fieldSet{set: make(map[string]struct{}, len(cols))}
	for _, f := range cols {
		set.append(f)
	}
	return set, fFields
}
