package changeset

import (
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
)

type Operation string

const (
	OperationBegin    Operation = "BEGIN"
	OperationCommit   Operation = "COMMIT"
	OperationInsert   Operation = "INSERT"
	OperationUpdate   Operation = "UPDATE"
	OperationDelete   Operation = "DELETE"
	OperationTruncate Operation = "TRUNCATE"
)

// WatermarkCommitter is an interface that commits a given watermark to backing datastores.
type WatermarkCommitter interface {
	// Commit commits the current watermark across the backing datastores - remote
	// and local.  Note that the remote may be committed at specific intervals,
	// so no guarantee of an immediate commit is provided.
	Commit(Watermark)
}

type Changeset struct {
	// Watermark represents the internal watermark for this changeset op.
	Watermark Watermark `json:"watermark"`

	// Operation represents the operation type for this event.
	Operation Operation `json:"operation"`

	// Data represents the actual data for the operation
	Data Data `json:"data"`
}

type Watermark struct {
	PostgresWatermark `json:"pg"`
}

type PostgresWatermark struct {
	// LSN is a Postgres-specific watermark.
	LSN pglogrepl.LSN
	// ServerTime is the optional server time of the watermark.  This is
	// always provided with Postgres watermarks.
	ServerTime time.Time
}

type Data struct {
	// TransactionLSN represents the last LSN of a transaction.
	TxnLSN        uint32     `json:"txn_id,omitempty"`
	TxnCommitTime *time.Time `json:"txn_commit_time,omitempty"`

	Table string       `json:"table,omitempty"`
	Old   UpdateTuples `json:"old,omitempty"`
	New   UpdateTuples `json:"new,omitempty"`

	// TruncatedTables represents the table names truncated in a Truncate operation
	TruncatedTables []string `json:"truncated_tables,omitempty"`
}

type UpdateTuples map[string]ColumnUpdate

type ColumnUpdate struct {
	// Encoding represents the encoding of the data in Data.  This may be one of:
	//
	// - "n", representing null data.
	// - "u", representing the unchagned TOAST data within postgres
	// - "t", representing text-encoded data
	// - "b", representing binary data.
	// - "i", representing an integer
	// - "f", representing a float
	Encoding string `json:"encoding"`
	// Data is the value of the column.  If this is binary data, this data will be
	// base64 encoded.
	Data any `json:"data"`
}

func (o Operation) ToEventVerb() string {
	switch o {
	case OperationBegin:
		return "tx-began"
	case OperationCommit:
		return "tx-committed"
	case OperationInsert:
		return "inserted"
	case OperationUpdate:
		return "updated"
	case OperationDelete:
		return "deleted"
	case OperationTruncate:
		return "truncated"
	default:
		return strings.ToLower(string(o))
	}
}
