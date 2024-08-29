package redis_state

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	osqueue "github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/state"
	"github.com/oklog/ulid/v2"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

func TestQueuePartitionConcurrency(t *testing.T) {
	r := miniredis.RunT(t)

	rc, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:  []string{r.Addr()},
		DisableCache: true,
	})
	require.NoError(t, err)
	defer rc.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	limit_1 := uuid.New()
	limit_10 := uuid.New()
	workflowIDs := []uuid.UUID{limit_1, limit_10}

	// Limit function concurrency by workflow ID.
	pkf := func(ctx context.Context, p QueuePartition) (string, int) {
		switch p.WorkflowID {
		case limit_1:
			return p.WorkflowID.String(), 1
		case limit_10:
			return p.WorkflowID.String(), 10
		default:
			// No concurrency, which means use the default concurrency limits.
			return "", 0
		}
	}

	// Create a new lifecycle listener.  This should be invoked each time we hit limits.
	ll := testLifecycleListener{
		l:           &sync.Mutex{},
		concurrency: map[uuid.UUID]int{},
	}

	q := NewQueue(
		NewQueueClient(rc, QueueDefaultKey),
		WithNumWorkers(100),
		WithPartitionConcurrencyKeyGenerator(pkf),
		WithQueueLifecycles(ll),
	)

	var (
		counter_1   int32
		counter_10  int32
		jobDuration = 2 * time.Second
	)

	// Run the queue.
	go func() {
		_ = q.Run(ctx, func(ctx context.Context, _ osqueue.RunInfo, item osqueue.Item) (bool, error) {
			<-time.After(jobDuration / 2)
			// each job takes 2 seconds to complete.
			switch item.Identifier.WorkflowID {
			case limit_1:
				fmt.Println("Single concurrency item hit", time.Now().Truncate(time.Millisecond))
				atomic.AddInt32(&counter_1, 1)
			case limit_10:
				atomic.AddInt32(&counter_10, 1)
			}
			<-time.After(jobDuration / 2)
			return false, nil
		})
	}()

	at := time.Now().Add(time.Second).Truncate(time.Second)

	// Schedule 10 jobs;  it should take 20 seconds for limit_1 to finish,
	// and 2 seconds for limit_10 to finish, given each job takes 2 seconds.
	start := time.Now()
	for i := 0; i < 10; i++ {
		for _, id := range workflowIDs {
			err := q.Enqueue(ctx, osqueue.Item{
				Identifier: state.Identifier{
					WorkflowID: id,
					RunID:      ulid.MustNew(ulid.Now(), rand.Reader),
				},
			}, at)
			require.NoError(t, err)
		}
	}

	<-time.After(jobDuration)

	require.EqualValues(t, 10, atomic.LoadInt32(&counter_10), "Should have hit all 10 items with a concurrency limit of 10")
	require.EqualValues(t, int32(1), atomic.LoadInt32(&counter_1), "Should have only run a single job")

	// TODO: Assert that the counterPartitionConcurrencyLimitReached counter isn't crazy high - we
	// don't want to be churning on the partition.
	for i := 0; i <= 100; i++ {
		<-time.After(500 * time.Millisecond)
		if atomic.LoadInt32(&counter_1) == 10 {
			break
		}
	}

	require.NotZero(t, ll.concurrency[limit_1])

	diff := time.Since(start).Seconds()
	require.Greater(t, int(diff), 10, "10 jobs should have taken at least 10 seconds")
	require.Less(t, int(diff), 40, "10 jobs should have taken fewer than 40 seconds") // an extra 2x latency due to race checker
}

type testLifecycleListener struct {
	l           *sync.Mutex
	concurrency map[uuid.UUID]int
}

func (t testLifecycleListener) OnConcurrencyLimitReached(ctx context.Context, fnID uuid.UUID) {
	t.l.Lock()
	i := t.concurrency[fnID]
	t.concurrency[fnID] = i + 1
	t.l.Unlock()
}
