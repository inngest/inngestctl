package eventwriter

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/inngest/dbcap/pkg/changeset"
	"github.com/inngest/inngestgo"
)

func NewAPIClientWriter(
	ctx context.Context,
	client inngestgo.Client,
	batchSize int,
) EventWriter {
	cs := make(chan *changeset.Changeset, batchSize)
	return &apiWriter{
		client:    client,
		cs:        cs,
		batchSize: batchSize,
		wg:        sync.WaitGroup{},
	}
}

// ChangesetToEvent returns a map containing event data for the given changeset.
func ChangesetToEvent(cs changeset.Changeset) map[string]any {

	var name string

	if cs.Data.Table == "" {
		name = fmt.Sprintf("%s/%s", eventPrefix, cs.Operation.ToEventVerb())
	} else {
		name = fmt.Sprintf("%s/%s.%s", eventPrefix, cs.Data.Table, cs.Operation.ToEventVerb())
	}

	return map[string]any{
		"name": name,
		"data": cs.Data,
		"ts":   cs.Watermark.ServerTime.UnixMilli(),
	}
}

type apiWriter struct {
	client    inngestgo.Client
	cs        chan *changeset.Changeset
	batchSize int

	wg sync.WaitGroup
}

func (a *apiWriter) Listen(ctx context.Context, committer changeset.WatermarkCommitter) chan *changeset.Changeset {
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()

		i := 0
		buf := make([]*changeset.Changeset, a.batchSize)

		// sendCtx is an additional uncancelled CTX which will be cancelled
		// 5 seconds after the
		for {
			timer := time.NewTimer(batchTimeout)

			select {
			case <-ctx.Done():
				// Shutting down.  Send the existing batch.
				if err := a.send(buf); err != nil {
					// TODO: Fail.  What do we do here?
				} else {
					committer.Commit(buf[i-1].Watermark)
				}
				return
			case <-timer.C:
				// Force sending current batch
				if i == 0 {
					timer.Reset(batchTimeout)
					continue
				}

				// We have events after a timeout - send them.
				if err := a.send(buf); err != nil {
					// TODO: Fail.  What do we do here?
				} else {
					// Commit the last LSN.
					committer.Commit(buf[i-1].Watermark)
				}

				// reset the buffer
				buf = make([]*changeset.Changeset, a.batchSize)
				i = 0
			case msg := <-a.cs:
				if i == a.batchSize {
					// send this batch, as we're full.
					if err := a.send(buf); err != nil {
						// TODO: Fail.  What do we do here?
					} else {
						committer.Commit(buf[i-1].Watermark)
					}
					// reset the buffer
					buf = make([]*changeset.Changeset, a.batchSize)
					i = 0
					continue
				}
				// Appoend the
				buf[i] = msg
				i++
				// Send this batch after at least 5 seconds
				timer.Reset(batchTimeout)
			}
		}
	}()
	return a.cs
}

func (a *apiWriter) Wait() {
	a.wg.Wait()
}

func (a *apiWriter) send(batch []*changeset.Changeset) error {
	// Always use a new cancel here so that when we quit polling
	// the HTTP request continues.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	evts := make([]any, len(batch))
	for i, cs := range batch {
		if cs == nil {
			evts = evts[0:i]
			break
		}
		evts[i] = ChangesetToEvent(*cs)
	}

	if len(evts) == 0 {
		return nil
	}

	_, err := a.client.SendMany(ctx, evts)
	return err
}
