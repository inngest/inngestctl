package eventwriter

import (
	"context"

	"github.com/inngest/dbcap/pkg/changeset"
)

// NewCallbackWriter is a simple writer which calls a callback for a given changeset.
//
// This is primarily used for testing.
func NewCallbackWriter(ctx context.Context, onChangeset func(cs *changeset.Changeset)) EventWriter {
	cs := make(chan *changeset.Changeset)
	return &cbWriter{
		cs:          cs,
		onChangeset: onChangeset,
	}
}

type cbWriter struct {
	onChangeset func(cs *changeset.Changeset)
	cs          chan *changeset.Changeset
}

func (w *cbWriter) Listen(ctx context.Context, committer changeset.WatermarkCommitter) chan *changeset.Changeset {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-w.cs:
				if msg == nil {
					continue
				}
				w.onChangeset(msg)
				if committer != nil {
					committer.Commit(msg.Watermark)
				}
			}
		}
	}()
	return w.cs
}

func (w *cbWriter) Wait() {}
