package decoder

import "github.com/inngest/dbcap/pkg/changeset"

type Decoder interface {
	// Decode accepts CDC input and updates the changeset after decoding the given
	// input from the database.
	//
	// It returns whether the changeset should propagate an event and any errors when
	// decoding decoding.
	Decode(in []byte, cs *changeset.Changeset) (bool, error)

	// ReplicationPluginArgs returns any arguments used within Postgres' replication plugins.
	ReplicationPluginArgs() []string
}
