package redis_state

import (
	"context"
	"crypto/rand"
	"fmt"
	mrand "math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	osqueue "github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/state"
	"github.com/inngest/inngest/pkg/logger"
	"github.com/oklog/ulid/v2"
	"github.com/redis/rueidis"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestQueueRunSequential(t *testing.T) {
	r := miniredis.RunT(t)

	rc, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:  []string{r.Addr()},
		DisableCache: true,
	})
	require.NoError(t, err)
	defer rc.Close()

	ctx := context.Background()

	q1ctx, q1cancel := context.WithCancel(ctx)
	q2ctx, q2cancel := context.WithCancel(ctx)
	defer q2cancel()

	q1 := NewQueue(
		NewQueueClient(rc, QueueDefaultKey),
		WithNumWorkers(10),
	)
	q2 := NewQueue(
		NewQueueClient(rc, QueueDefaultKey),
		WithNumWorkers(10),
	)

	// Run the queue.  After running this worker should claim the sequential lease.
	go func() {
		_ = q1.Run(q1ctx, func(ctx context.Context, _ osqueue.RunInfo, item osqueue.Item) (bool, error) {
			time, ok := GetItemStart(ctx)
			require.True(t, ok)
			require.NotZero(t, time)
			return false, nil
		})
	}()
	go func() {
		<-time.After(100 * time.Millisecond)
		_ = q2.Run(q2ctx, func(ctx context.Context, _ osqueue.RunInfo, item osqueue.Item) (bool, error) {
			return false, nil
		})
	}()

	<-time.After(110 * time.Millisecond)
	// Q1 gets lease, as it started first.
	require.NotNil(t, q1.sequentialLease())
	// Lease is in the future.
	require.True(t, ulid.Time(q1.sequentialLease().Time()).After(time.Now()))
	// Q2 has no lease.
	require.Nil(t, q2.sequentialLease())

	<-time.After(ConfigLeaseDuration)

	// Q1 retains lease.
	require.NotNil(t, q1.sequentialLease())
	require.Nil(t, q2.sequentialLease())

	// Cancel q1, temrinating the queue with the sequential lease.
	q1cancel()

	<-time.After(ConfigLeaseDuration * 2)

	// Q2 obtains lease.
	require.NotNil(t, q2.sequentialLease())
	// And that the previous lease has expired.
	lease := q1.sequentialLease()
	require.True(t, lease == nil || ulid.Time(lease.Time()).Before(time.Now()))
}

func max(i int) *int {
	return &i
}

func TestQueueRunBasic(t *testing.T) {
	customQueueName := "custom-queue-name"

	r := miniredis.RunT(t)

	rc, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:  []string{r.Addr()},
		DisableCache: true,
	})
	require.NoError(t, err)
	defer rc.Close()

	q := NewQueue(
		NewQueueClient(rc, QueueDefaultKey),
		// We can't add more than 8128 goroutines when detecting race conditions.
		WithNumWorkers(10),
		// Test custom queue names
		WithKindToQueueMapping(map[string]string{
			"test-kind": customQueueName,
		}),
	)
	ctx, cancel := context.WithCancel(context.Background())

	idA, idB := uuid.New(), uuid.New()
	items := []QueueItem{
		{
			WorkflowID: idA,
			Data: osqueue.Item{
				Kind:        osqueue.KindEdge,
				MaxAttempts: max(3),
				Identifier: state.Identifier{
					WorkflowID: idA,
					RunID:      ulid.MustNew(ulid.Now(), rand.Reader),
				},
			},
		},
		{
			WorkflowID: idB,
			Data: osqueue.Item{
				Kind:        osqueue.KindEdge,
				MaxAttempts: max(1),
				Identifier: state.Identifier{
					WorkflowID: idB,
					RunID:      ulid.MustNew(ulid.Now(), rand.Reader),
				},
			},
		},
		{
			WorkflowID: idB,
			QueueName:  &customQueueName,
			Data: osqueue.Item{
				Kind:        "test-kind",
				MaxAttempts: max(1),
				Identifier: state.Identifier{
					WorkflowID: idB,
					RunID:      ulid.MustNew(ulid.Now(), rand.Reader),
				},
			},
		},
	}

	var handled int32
	go func() {
		_ = q.Run(ctx, func(ctx context.Context, _ osqueue.RunInfo, item osqueue.Item) (bool, error) {
			logger.From(ctx).Debug().Interface("item", item).Msg("received item")
			atomic.AddInt32(&handled, 1)
			id := osqueue.JobIDFromContext(ctx)
			require.NotEmpty(t, id, "No job ID was passed via context")
			return false, nil
		})
	}()

	for n, item := range items {
		at := time.Now()
		if n == len(items)-1 {
			at = time.Now().Add(10 * time.Second)
		}
		_, err := q.EnqueueItem(ctx, item, at)
		require.NoError(t, err)
	}

	<-time.After(12 * time.Second)
	require.EqualValues(t, int32(len(items)), atomic.LoadInt32(&handled))
	cancel()

	<-time.After(time.Second)

	r.Close()
	rc.Close()

	// Assert queue items have been processed
	// Assert queue items have been dequeued, and peek is nil for workflows.
	// Assert metrics are correct.
}

func TestQueueRunRetry(t *testing.T) {
	r := miniredis.RunT(t)

	rc, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:  []string{r.Addr()},
		DisableCache: true,
	})
	require.NoError(t, err)
	defer rc.Close()

	q := NewQueue(
		NewQueueClient(rc, QueueDefaultKey),
		// We can't add more than 8128 goroutines when detecting race conditions.
		WithNumWorkers(10),
	)
	ctx, cancel := context.WithCancel(context.Background())

	idA := uuid.New()
	items := []QueueItem{
		{
			WorkflowID: idA,
			Data: osqueue.Item{
				Kind:        osqueue.KindEdge,
				MaxAttempts: max(3),
				Identifier: state.Identifier{
					WorkflowID: idA,
					RunID:      ulid.MustNew(ulid.Now(), rand.Reader),
				},
			},
		},
	}

	var counter int32
	go func() {
		_ = q.Run(ctx, func(ctx context.Context, _ osqueue.RunInfo, item osqueue.Item) (bool, error) {
			logger.From(ctx).Debug().Interface("item", item).Msg("received item")
			atomic.AddInt32(&counter, 1)
			if atomic.LoadInt32(&counter) == 1 {
				return false, fmt.Errorf("retry this step once")
			}
			return false, nil
		})
	}()

	for _, item := range items {
		_, err := q.EnqueueItem(ctx, item, time.Now())
		require.NoError(t, err)
	}

	<-time.After(2*time.Second + 45*time.Second)
	require.EqualValues(t, 2, atomic.LoadInt32(&counter), r.Dump())
	cancel()

	<-time.After(time.Second)

	r.Close()
	rc.Close()
}

// TestQueueRunExtended runs an extended in-memory test which:
// - Enqueues 1-150 jobs every 0-100ms, for one of 1,0000 random functions
// - Each job can be scheduled from now -> 10s in the future
// - Each job can take from 0->7500ms to complete.
//
// We randomly kill workers and assert that jobs are complete.
//
// NOTE: When this runs with the race decetor (--race), the throughput of goroutines
// is severely limited.  This means that we need to extend the time in which we can
// process jobs.
func TestQueueRunExtended(t *testing.T) {
	var handled int64

	jobCompleteMax := int32(12_500) // ms
	delayMax := int32(15_000)       // ms

	l := logger.From(context.Background()).Level(zerolog.InfoLevel)
	r := miniredis.RunT(t)

	rc, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:  []string{r.Addr()},
		DisableCache: true,
	})
	require.NoError(t, err)
	defer rc.Close()

	// In this test, shards must be leased rapidly, as we randomly close and terminate workers
	// after a minimum of 10 seconds.
	ShardTickTime = 5 * time.Second
	ShardLeaseTime = 5 * time.Second

	sf := func(ctx context.Context, queueName string, wsID uuid.UUID) *QueueShard {
		// For nil UUIDs, return a shard.
		if wsID == uuid.Nil {
			return &QueueShard{
				Name:               "test-shard",
				GuaranteedCapacity: 1,
			}
		}
		return nil
	}

	q := NewQueue(
		NewQueueClient(rc, QueueDefaultKey),
		// We can't add more than 8128 goroutines when detecting race conditions,
		// so lower the number of workers.
		WithNumWorkers(200),
		WithLogger(&l),
		WithShardFinder(sf),
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		for {
			if ctx.Err() != nil {
				return
			}

			go func() {

				defer func() {
					if err := recover(); err != nil {
						fmt.Println(err)
					}
				}()

				// Create new queues every 5 seconds that bring up workers and fail
				// randomly, between 1 and 10 seconds in.
				ctx, cancel := context.WithCancel(context.Background())
				q := NewQueue(
					NewQueueClient(rc, QueueDefaultKey),
					// We can't add more than 8128 goroutines when detecting race conditions,
					// so lower the number of workers.
					WithNumWorkers(200),
					WithLogger(&l),
					WithShardFinder(sf),
				)

				go func() {
					_ = q.Run(ctx, func(ctx context.Context, _ osqueue.RunInfo, item osqueue.Item) (bool, error) {
						// Wait up to N seconds to complete.
						<-time.After(time.Duration(mrand.Int31n(atomic.LoadInt32(&jobCompleteMax))) * time.Millisecond)
						// Increase handled when job is done.
						atomic.AddInt64(&handled, 1)
						return false, nil
					})
				}()

				<-time.After(time.Duration(mrand.Int31n(30)+10) * time.Second)
				fmt.Println("Cancelling worker")
				cancel()
				if mrand.Int31n(30) == 1 {
					fmt.Println("Panicking worker")
					panic("fail")
				}
			}()

			<-time.After(5 * time.Second)
		}

	}()

	mrand.Seed(time.Now().UnixMicro())

	funcs := make([]uuid.UUID, 1000)
	for n := range funcs {
		funcs[n] = uuid.New()
	}

	go func() {
		_ = q.Run(ctx, func(ctx context.Context, _ osqueue.RunInfo, item osqueue.Item) (bool, error) {
			// Wait up to N seconds to complete.
			<-time.After(time.Duration(mrand.Int31n(atomic.LoadInt32(&jobCompleteMax))) * time.Millisecond)
			// Increase handled when job is done.
			atomic.AddInt64(&handled, 1)
			return false, nil
		})
	}()

	enqueueDuration := 30 * time.Second

	var added int64
	go func(duration time.Duration) {
		// For N seconds enqueue items.
		after := time.After(duration)
		for {
			sleep := mrand.Intn(250)
			select {
			case <-after:
				return
			case <-time.After(time.Duration(sleep) * time.Millisecond):
				// Enqueue 1-25 N jobs
				n := mrand.Intn(24) + 1
				for i := 0; i < n; i++ {
					// Have a 1% chance of using a nil UUID, sharded.
					id := funcs[mrand.Intn(len(funcs))]
					if mrand.Intn(100) == 0 {
						id = uuid.UUID{}
					}

					item := QueueItem{
						WorkflowID:  id,
						WorkspaceID: id,
					}

					// Enqueue with a delay.
					diff := mrand.Int31n(atomic.LoadInt32(&delayMax))

					_, err := q.EnqueueItem(ctx, item, time.Now().Add(time.Duration(diff)*time.Millisecond))
					require.NoError(t, err)
					atomic.AddInt64(&added, 1)
				}
			}
		}
	}(enqueueDuration)

	go func() {
		t := time.Tick(1000 * time.Millisecond)
		prev := atomic.LoadInt64(&handled)
		for {
			select {
			case <-ctx.Done():
				return
			case <-t:
				next := atomic.LoadInt64(&handled)
				added := atomic.LoadInt64(&added)
				fmt.Printf(
					"Handled: %d \t Handled delta: %d \t Added: %d \t Remaining: %d\n",
					next,
					next-prev,
					added,
					added-next,
				)
				latencySem.Lock()
				// NOTE: RUNNING THIS WITH THE RACE CHECKER SIGNIFICANTLY INCREASES LATENCY.
				// The actual latency should be checked without --race on.
				fmt.Printf("AVG LATENCY: %dms\n", time.Duration(latencyAvg.Value()).Milliseconds())
				latencySem.Unlock()
				prev = next
			}
		}
	}()

	// Wait for all items to complete
	<-time.After(enqueueDuration)

	// The default wait
	wait := atomic.LoadInt32(&delayMax) + atomic.LoadInt32(&jobCompleteMax) + 100
	// Increasing, because of the race detector
	wait = wait * 3

	// We enqueue jobs up to delayMax, and they can take up to jobCompleteMax, so add
	// 100ms of buffer.
	<-time.After(time.Duration(wait) * time.Millisecond)

	a := atomic.LoadInt64(&added)
	h := atomic.LoadInt64(&handled)

	fmt.Printf("Added %d items\n", a)
	fmt.Printf("Handled %d items\n", h)

	require.EqualValues(t, a, h, "Added %d, handled %d (delta: %d)", a, h, a-h)

	cancel()

	<-time.After(time.Second)
	r.Close()
}

func TestRunPriorityFactor(t *testing.T) {
	r := miniredis.RunT(t)

	rc, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:  []string{r.Addr()},
		DisableCache: true,
	})
	require.NoError(t, err)
	defer rc.Close()

	q := NewQueue(
		NewQueueClient(rc, QueueDefaultKey),
		// We can't add more than 8128 goroutines when detecting race conditions.
		WithNumWorkers(10),
	)
	ctx, cancel := context.WithCancel(context.Background())

	idA, idB := uuid.New(), uuid.New()
	factor2 := int64(2)
	items := []osqueue.Item{
		{
			WorkspaceID: idA,
			Kind:        osqueue.KindEdge,
			MaxAttempts: max(1),
			Identifier: state.Identifier{
				WorkflowID: idA,
				RunID:      ulid.MustNew(ulid.Now(), rand.Reader),
			},
		},
		{
			WorkspaceID: idB,
			Kind:        osqueue.KindEdge,
			MaxAttempts: max(1),
			Identifier: state.Identifier{
				WorkflowID: idB,
				RunID:      ulid.MustNew(ulid.Now(), rand.Reader),
				// Enqueue 2 seconds prior to the actual At time
				PriorityFactor: &factor2,
			},
		},
	}

	var handled int32
	go func() {
		_ = q.Run(ctx, func(ctx context.Context, _ osqueue.RunInfo, item osqueue.Item) (bool, error) {
			atomic.AddInt32(&handled, 1)
			return false, nil
		})
	}()

	// Run at the next 2 second mark
	at := time.Now().Add(2 * time.Second)

	for _, item := range items {
		err := q.Enqueue(ctx, item, at)
		require.NoError(t, err)
	}

	<-time.After(500 * time.Millisecond)
	// Immediately we should run the task with a higher priority
	require.EqualValues(t, 1, atomic.LoadInt32(&handled))

	<-time.After(2 * time.Second)
	require.EqualValues(t, 2, atomic.LoadInt32(&handled))

	// Nothing else runs after 5 seconds
	<-time.After(5 * time.Second)
	require.EqualValues(t, 2, atomic.LoadInt32(&handled))

	cancel()
	r.Close()
	rc.Close()

	// Assert queue items have been processed
	// Assert queue items have been dequeued, and peek is nil for workflows.
	// Assert metrics are correct.
}
