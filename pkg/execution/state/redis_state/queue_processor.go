package redis_state

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/inngest/inngest/pkg/enums"

	"github.com/VividCortex/ewma"
	osqueue "github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/state"
	"github.com/inngest/inngest/pkg/inngest/log"
	"github.com/inngest/inngest/pkg/telemetry/metrics"
	"github.com/inngest/inngest/pkg/telemetry/redis_telemetry"
	"github.com/oklog/ulid/v2"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"gonum.org/v1/gonum/stat/sampleuv"
)

const (
	minWorkersFree = 5
)

var (
	// ShardTickTime is the duration in which we periodically check shards for
	// lease information, etc.
	ShardTickTime = 15 * time.Second
	// ShardLeaseTime is how long shards are leased.
	ShardLeaseTime = 10 * time.Second

	maxShardLeaseAttempts = 10
)

var (
	latencyAvg ewma.MovingAverage
	latencySem *sync.Mutex

	startedAtKey = startedAtCtxKey{}
	sojournKey   = sojournCtxKey{}
	latencyKey   = latencyCtxKey{}

	errProcessNoCapacity   = fmt.Errorf("no capacity")
	errProcessStopIterator = fmt.Errorf("stop iterator")
)

func init() {
	latencyAvg = ewma.NewMovingAverage()
	latencySem = &sync.Mutex{}
}

// startedAtCtxKey is a context key which records when the queue item starts,
// available via context.
type startedAtCtxKey struct{}

// latencyCtxKey is a context key which records when the queue item starts,
// available via context.
type latencyCtxKey struct{}

// sojournCtxKey is a context key which records when the queue item starts,
// available via context.
type sojournCtxKey struct{}

func GetItemStart(ctx context.Context) (time.Time, bool) {
	t, ok := ctx.Value(startedAtKey).(time.Time)
	return t, ok
}

func GetItemSystemLatency(ctx context.Context) (time.Duration, bool) {
	t, ok := ctx.Value(latencyKey).(time.Duration)
	return t, ok
}

func GetItemConcurrencyLatency(ctx context.Context) (time.Duration, bool) {
	t, ok := ctx.Value(sojournKey).(time.Duration)
	return t, ok
}

func (q *queue) Enqueue(ctx context.Context, item osqueue.Item, at time.Time) error {
	// propagate
	if item.Metadata == nil {
		item.Metadata = map[string]string{}
	}

	id := ""
	if item.JobID != nil {
		id = *item.JobID
	}

	var queueName *string
	if name, ok := q.queueKindMapping[item.Kind]; ok {
		queueName = &name
	}
	// item.QueueName takes precedence if not nil
	if item.QueueName != nil {
		queueName = item.QueueName
	}

	metrics.IncrQueueItemStatusCounter(ctx, metrics.CounterOpt{
		PkgName: pkgName,
		Tags: map[string]any{
			"status": "enqueued",
			"kind":   item.Kind,
		},
	})

	qi := QueueItem{
		ID:          id,
		AtMS:        at.UnixMilli(),
		WorkspaceID: item.WorkspaceID,
		WorkflowID:  item.Identifier.WorkflowID,
		Data:        item,
		// Only use the queue name if provided by queueKindMapping.
		// Otherwise, this defaults to WorkflowID.
		QueueName:  queueName,
		WallTimeMS: at.UnixMilli(),
	}

	// Use the queue item's score, ensuring we process older function runs first
	// (eg. before at)
	next := time.UnixMilli(qi.Score(q.clock.Now()))

	if factor := qi.Data.GetPriorityFactor(); factor != 0 {
		// Ensure we mutate the AtMS time by the given priority factor.
		qi.AtMS -= factor
	}

	_, err := q.EnqueueItem(ctx, qi, next)
	if err != nil {
		return err
	}

	return nil
}

func (q *queue) Run(ctx context.Context, f osqueue.RunFunc) error {
	for i := int32(0); i < q.numWorkers; i++ {
		go q.worker(ctx, f)
	}

	go q.claimShards(ctx)
	go q.claimSequentialLease(ctx)
	go q.runScavenger(ctx)
	go q.runInstrumentation(ctx)

	tick := q.clock.NewTicker(q.pollTick)

	q.logger.Debug().
		Str("poll", q.pollTick.String()).
		Msg("starting queue worker")

LOOP:
	for {
		select {
		case <-ctx.Done():
			// Kill signal
			tick.Stop()
			break LOOP
		case err := <-q.quit:
			// An inner function received an error which was deemed irrecoverable, so
			// we're quitting the queue.
			q.logger.Error().Err(err).Msg("quitting runner internally")
			tick.Stop()
			break LOOP
		case <-tick.Chan():
			if q.capacity() < minWorkersFree {
				// Wait until we have more workers free.  This stops us from
				// claiming a partition to work on a single job, ensuring we
				// have capacity to run at least MinWorkersFree concurrent
				// QueueItems.  This reduces latency of enqueued items when
				// there are lots of enqueued and available jobs.
				continue
			}

			if err := q.scan(ctx); err != nil {
				// On scan errors, halt the worker entirely.
				if errors.Unwrap(err) != context.Canceled {
					q.logger.Error().Err(err).Msg("error scanning partition pointers")
				}
				break LOOP
			}
		}
	}

	// Wait for all in-progress items to complete.
	q.logger.Info().Msg("queue waiting to quit")
	q.wg.Wait()

	return nil
}

func (q *queue) claimShards(ctx context.Context) {
	if q.sf == nil {
		// TODO: Inspect denylists and whether this worker is capable of leasing
		// shards.  Note that shards should only be created for SDK-based workers;
		// if you use the queue for anything else other than step jobs, the worker
		// cannot lease jobs.  To this point, we should make this opt-in instead of
		// opt-out to prevent errors.
		//
		// For now, you have to provide a shardFinder to lease shards.
		q.logger.Info().Msg("no shard finder;  skipping shard claiming")
		return
	}

	scanTick := q.clock.NewTicker(ShardTickTime)
	leaseTick := q.clock.NewTicker(ShardLeaseTime / 2)

	// records whether we're leasing
	var leasing int32

	for {
		if q.isSequential() {
			// Sequential workers never lease shards.  They always run in order
			// on the global partition queue.
			<-scanTick.Chan()
			continue
		}

		select {
		case <-ctx.Done():
			// TODO: Remove leases immediately from backing store.
			scanTick.Stop()
			leaseTick.Stop()
			return
		case <-scanTick.Chan():
			go func() {
				if !atomic.CompareAndSwapInt32(&leasing, 0, 1) {
					// Only one lease can occur at once.
					q.logger.Debug().Msg("already leasing shards")
					return
				}

				// Always reset the leasing op to zero, allowing us to lease again.
				defer func() { atomic.StoreInt32(&leasing, 0) }()

				// Retry claiming leases until all shards have been taken.  All operations
				// must succeed, even if it leaves us spinning.  Note that scanShards filters
				// out unnecessary leases and shards that have already been leased.
				retry := true
				n := 0
				for retry && n < maxShardLeaseAttempts {
					n++
					var err error
					retry, err = q.scanShards(ctx)
					if err != nil {
						q.logger.Error().Err(err).Msg("error scanning and leasing shards")
						return
					}
					if retry {
						<-q.clock.After(time.Duration(rand.Intn(50)) * time.Millisecond)
					}
				}
			}()
		case <-leaseTick.Chan():
			// Copy the slice to prevent locking/concurrent access.
			existingLeases := q.getShardLeases()

			for _, s := range existingLeases {
				// Attempt to lease all ASAP, even if the backing store is single threaded.
				go func(ls leasedShard) {
					nextLeaseID, err := q.renewShardLease(ctx, &ls.Shard, ShardLeaseTime, ls.Lease)
					if err != nil {
						q.logger.Error().Err(err).Msg("error renewing shard lease")
						return
					}
					q.logger.Debug().Interface("shard", ls).Msg("renewed shard lease")
					// Update the lease ID so that we have this stored appropriately for
					// the next renewal.
					q.addLeasedShard(ctx, &ls.Shard, *nextLeaseID)
				}(s)
			}
		}
	}
}

func (q *queue) scanShards(ctx context.Context) (retry bool, err error) {
	// TODO: Make instances of *queue register worker information when calling
	//       Run().
	//       Fetch this information, and correctly assign workers to shard maps
	//       based on the distribution of items in the queue here.  This lets
	//       us oversubscribe appropriately.
	shardMap, err := q.getShards(ctx)
	if err != nil {
		q.logger.Error().Err(err).Msg("error fetching shards")
		return
	}
	shards, err := q.filterShards(ctx, shardMap)
	if err != nil {
		q.logger.Error().Err(err).Msg("error filtering shards")
		return
	}

	if len(shards) == 0 {
		return
	}

	for _, shard := range shards {
		leaseID, err := q.leaseShard(ctx, shard, ShardLeaseTime, len(shard.Leases))
		if err == nil {
			// go q.counter(ctx, "queue_shard_lease_success_total", 1, map[string]any{
			// 	"shard_name": shard.Name,
			// })
			q.addLeasedShard(ctx, shard, *leaseID)
			q.logger.Debug().Interface("shard", shard).Str("lease_id", leaseID.String()).Msg("leased shard")
		} else {
			q.logger.Debug().Interface("shard", shard).Err(err).Msg("failed to lease shard")
		}

		// go q.counter(ctx, "queue_shard_lease_conflict_total", 1, map[string]any{
		// 	"shard_name": shard.Name,
		// })

		switch err {
		case errShardNotFound:
			// This is okay;  the shard was removed when trying to lease
			continue
		case errShardIndexLeased:
			// This is okay;  another worker grabbed the lease.  No need to retry
			// as another worker grabbed this.
			continue
		case errShardIndexInvalid:
			// A lease expired while trying to lease — try again.
			retry = true
		default:
			return true, err
		}
	}

	return retry, nil
}

func (q *queue) addLeasedShard(ctx context.Context, shard *QueueShard, lease ulid.ULID) {
	for i, n := range q.shardLeases {
		if n.Shard.Name == shard.Name {
			// Updated in place.
			q.shardLeaseLock.Lock()
			q.shardLeases[i] = leasedShard{
				Lease: lease,
				Shard: *shard,
			}
			q.shardLeaseLock.Unlock()
			return
		}
	}
	// Not updated in place, so add to the list and return.
	q.shardLeaseLock.Lock()
	q.shardLeases = append(q.shardLeases, leasedShard{
		Lease: lease,
		Shard: *shard,
	})
	q.shardLeaseLock.Unlock()
}

// filterShards filters shards during assignment, removing any shards that this worker
// has already leased;  any shards that have already had their leasing requirements met;
// and priority shuffles shards to lease in a non-deterministic (but prioritized) order.
//
// The returned shards are safe to be leased, and should be attempted in-order.
func (q *queue) filterShards(ctx context.Context, shards map[string]*QueueShard) ([]*QueueShard, error) {
	if len(shards) == 0 {
		return nil, nil
	}

	// Copy the slice to prevent locking/concurrent access.
	for _, v := range q.getShardLeases() {
		delete(shards, v.Shard.Name)
	}

	weights := []float64{}
	shuffleIdx := []*QueueShard{}
	for _, v := range shards {
		// XXX: Here we can add latency targets, etc.

		validLeases := []ulid.ULID{}
		for _, l := range v.Leases {
			if time.UnixMilli(int64(l.Time())).After(q.clock.Now()) {
				validLeases = append(validLeases, l)
			}
		}
		// Replace leases with the # of valid leases.
		v.Leases = validLeases

		if len(validLeases) >= int(v.GuaranteedCapacity) {
			continue
		}

		weights = append(weights, float64(v.Priority))
		shuffleIdx = append(shuffleIdx, v)
	}

	if len(shuffleIdx) == 1 {
		return shuffleIdx, nil
	}

	// Reduce the likelihood of all workers attempting to claim shards by
	// randomly shuffling.  Note that high priority shards will still be
	// likely to come first with some contention.
	w := sampleuv.NewWeighted(weights, rnd)
	result := make([]*QueueShard, len(weights))
	for n := range result {
		idx, ok := w.Take()
		if !ok && len(result) < len(weights)-1 {
			return result, ErrWeightedSampleRead
		}
		result[n] = shuffleIdx[idx]
	}

	return result, nil
}

// claimSequentialLease is a process which continually runs while listening to the queue,
// attempting to claim a lease on sequential processing.  Only one worker is allowed to
// work on partitions sequentially;  this reduces contention.
func (q *queue) claimSequentialLease(ctx context.Context) {
	// Workers with an allowlist can never claim sequential queues.
	if len(q.allowQueues) > 0 {
		return
	}

	// Attempt to claim the lease immediately.
	leaseID, err := q.ConfigLease(ctx, q.u.kg.Sequential(), ConfigLeaseDuration, q.sequentialLease())
	if err != ErrConfigAlreadyLeased && err != nil {
		q.quit <- err
		return
	}

	q.seqLeaseLock.Lock()
	q.seqLeaseID = leaseID
	q.seqLeaseLock.Unlock()

	tick := q.clock.NewTicker(ConfigLeaseDuration / 3)
	for {
		select {
		case <-ctx.Done():
			tick.Stop()
			return
		case <-tick.Chan():
			leaseID, err := q.ConfigLease(ctx, q.u.kg.Sequential(), ConfigLeaseDuration, q.sequentialLease())
			if err == ErrConfigAlreadyLeased {
				// This is expected; every time there is > 1 runner listening to the
				// queue there will be contention.
				q.seqLeaseLock.Lock()
				q.seqLeaseID = nil
				q.seqLeaseLock.Unlock()
				continue
			}
			if err != nil {
				q.logger.Error().Err(err).Msg("error claiming sequential lease")
				q.seqLeaseLock.Lock()
				q.seqLeaseID = nil
				q.seqLeaseLock.Unlock()
				continue
			}

			q.seqLeaseLock.Lock()
			if q.seqLeaseID == nil {
				// Only track this if we're creating a new lease, not if we're renewing
				// a lease.
				metrics.IncrQueueSequentialLeaseClaimsCounter(ctx, metrics.CounterOpt{PkgName: pkgName})
			}
			q.seqLeaseID = leaseID
			q.seqLeaseLock.Unlock()
		}
	}
}

func (q *queue) runScavenger(ctx context.Context) {
	// Attempt to claim the lease immediately.
	leaseID, err := q.ConfigLease(ctx, q.u.kg.Scavenger(), ConfigLeaseDuration, q.scavengerLease())
	if err != ErrConfigAlreadyLeased && err != nil {
		q.quit <- err
		return
	}

	q.scavengerLeaseLock.Lock()
	q.scavengerLeaseID = leaseID // no-op if not leased
	q.scavengerLeaseLock.Unlock()

	tick := q.clock.NewTicker(ConfigLeaseDuration / 3)
	scavenge := q.clock.NewTicker(30 * time.Second)

	for {
		select {
		case <-ctx.Done():
			tick.Stop()
			scavenge.Stop()
			return
		case <-scavenge.Chan():
			// Scavenge the items
			if q.isScavenger() {
				count, err := q.Scavenge(ctx)
				if err != nil {
					q.logger.Error().Err(err).Msg("error scavenging")
				}
				if count > 0 {
					q.logger.Info().Int("len", count).Msg("scavenged lost jobs")
				}
			}
		case <-tick.Chan():
			// Attempt to re-lease the lock.
			leaseID, err := q.ConfigLease(ctx, q.u.kg.Scavenger(), ConfigLeaseDuration, q.scavengerLease())
			if err == ErrConfigAlreadyLeased {
				// This is expected; every time there is > 1 runner listening to the
				// queue there will be contention.
				q.scavengerLeaseLock.Lock()
				q.scavengerLeaseID = nil
				q.scavengerLeaseLock.Unlock()
				continue
			}
			if err != nil {
				q.logger.Error().Err(err).Msg("error claiming scavenger lease")
				q.scavengerLeaseLock.Lock()
				q.scavengerLeaseID = nil
				q.scavengerLeaseLock.Unlock()
				continue
			}

			q.scavengerLeaseLock.Lock()
			if q.scavengerLeaseID == nil {
				// Only track this if we're creating a new lease, not if we're renewing
				// a lease.
				metrics.IncrQueueSequentialLeaseClaimsCounter(ctx, metrics.CounterOpt{PkgName: pkgName})
			}
			q.scavengerLeaseID = leaseID
			q.scavengerLeaseLock.Unlock()
		}
	}
}

func (q *queue) runInstrumentation(ctx context.Context) {
	ctx = redis_telemetry.WithScope(redis_telemetry.WithOpName(ctx, "Instrument"), redis_telemetry.ScopeQueue)

	leaseID, err := q.ConfigLease(ctx, q.u.kg.Instrumentation(), ConfigLeaseMax, q.instrumentationLease())
	if err != ErrConfigAlreadyLeased && err != nil {
		q.quit <- err
		return
	}

	setLease := func(lease *ulid.ULID) {
		q.instrumentationLeaseLock.Lock()
		defer q.instrumentationLeaseLock.Unlock()
		q.instrumentationLeaseID = lease

		if lease != nil && q.instrumentationLeaseID == nil {
			metrics.IncrInstrumentationLeaseClaimsCounter(ctx, metrics.CounterOpt{PkgName: pkgName})
		}
	}

	setLease(leaseID)

	tick := q.clock.NewTicker(ConfigLeaseMax / 3)
	instr := q.clock.NewTicker(20 * time.Second)

	for {
		select {
		case <-ctx.Done():
			tick.Stop()
			instr.Stop()
			return
		case <-instr.Chan():
			if q.isInstrumentator() {
				if err := q.Instrument(ctx); err != nil {
					q.logger.Error().Err(err).Msg("error running instrumentation")
				}
			}
		case <-tick.Chan():
			metrics.GaugeWorkerQueueCapacity(ctx, int64(q.numWorkers), metrics.GaugeOpt{PkgName: pkgName})

			leaseID, err := q.ConfigLease(ctx, q.u.kg.Instrumentation(), ConfigLeaseMax, q.instrumentationLease())
			if err == ErrConfigAlreadyLeased {
				setLease(nil)
				continue
			}

			if err != nil {
				q.logger.Error().Err(err).Msg("error claiming instrumentation lease")
				setLease(nil)
				continue
			}

			setLease(leaseID)
		}
	}
}

// worker runs a blocking process that listens to items being pushed into the
// worker channel.  This allows us to process an individual item from a queue.
func (q *queue) worker(ctx context.Context, f osqueue.RunFunc) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-q.quit:
			return
		case i := <-q.workers:
			// Create a new context which isn't cancelled by the parent, when quit.
			// XXX: When jobs can have their own cancellation signals, move this into
			// process itself.
			processCtx, cancel := context.WithCancel(context.Background())
			err := q.process(
				processCtx,
				i.P,
				i.I,
				i.PCtr,
				i.S,
				f,
			)
			q.sem.Release(1)
			metrics.WorkerQueueCapacityCounter(ctx, -1, metrics.CounterOpt{PkgName: pkgName})
			cancel()
			if err == nil {
				continue
			}

			// We handle the error individually within process, requeueing
			// the item into the queue.  Here, the worker can continue as
			// usual to process the next item.
			q.logger.Error().Err(err).Msg("error processing queue item")
		}
	}
}

func (q *queue) scan(ctx context.Context) error {
	if q.capacity() == 0 {
		return nil
	}

	// Store the shard that we processed, allowing us to eventually pass this
	// down to the job for stat tracking.
	var (
		shard           *QueueShard
		metricShardName = "<global>" // default global name for metrics in this function
	)

	eg := errgroup.Group{}

	// If we have continued partitions, process those immediately.
	q.continuesLock.Lock()
	for _, c := range q.continues {
		cont := c
		eg.Go(func() error {
			p := cont.partition
			if q.capacity() == 0 {
				// no longer any available workers for partition, so we can skip
				// work
				metrics.IncrQueueScanNoCapacityCounter(ctx, metrics.CounterOpt{PkgName: pkgName})
				return nil
			}
			if p.PartitionType != int(enums.PartitionTypeDefault) {
				return nil
			}

			// TODO: Continuation metric
			if err := q.processPartition(ctx, p, cont.count, shard); err != nil {
				if err == ErrPartitionNotFound || err == ErrPartitionGarbageCollected {
					// Another worker grabbed the partition, or the partition was deleted
					// during the scan by an another worker.
					// TODO: Increase internal metrics
					return nil
				}
				if errors.Unwrap(err) != context.Canceled {
					q.logger.Error().Err(err).Msg("error processing partition")
				}
				return err
			}

			metrics.IncrQueuePartitionProcessedCounter(ctx, metrics.CounterOpt{
				PkgName: pkgName,
				Tags:    map[string]any{"shard": metricShardName},
			})
			return nil
		})
	}
	q.continuesLock.Unlock()

	// By default, use the global partition
	partitionKey := q.u.kg.GlobalPartitionIndex()

	// If this worker has leased shards, those take priority 95% of the time.  There's a 5% chance that the
	// worker still works on the global queue.
	existingLeases := q.getShardLeases()

	if len(existingLeases) > 0 {
		// Pick a random item between the shards.
		i := rand.Intn(len(existingLeases))
		shard = &existingLeases[i].Shard
		// Use the shard partition
		partitionKey = q.u.kg.ShardPartitionIndex(shard.Name)
		metricShardName = "<shard>:" + shard.Name
	}

	// Peek 1s into the future to pull jobs off ahead of time, minimizing 0 latency
	partitions, err := duration(ctx, "partition_peek", q.clock.Now(), func(ctx context.Context) ([]*QueuePartition, error) {
		return q.partitionPeek(ctx, partitionKey, q.isSequential(), q.clock.Now().Add(PartitionLookahead), PartitionPeekMax)
	})
	if err != nil {
		return err
	}

	for _, ptr := range partitions {
		p := *ptr
		eg.Go(func() error {
			if q.capacity() == 0 {
				// no longer any available workers for partition, so we can skip
				// work
				metrics.IncrQueueScanNoCapacityCounter(ctx, metrics.CounterOpt{PkgName: pkgName})
				return nil
			}
			if p.PartitionType != int(enums.PartitionTypeDefault) {
				return nil
			}

			// This process is starting from a peek, so pass 0 as the continuation counter - we haven't yet
			// done a continuation for sequential processing.
			if err := q.processPartition(ctx, &p, 0, shard); err != nil {
				if err == ErrPartitionNotFound || err == ErrPartitionGarbageCollected {
					// Another worker grabbed the partition, or the partition was deleted
					// during the scan by an another worker.
					// TODO: Increase internal metrics
					return nil
				}
				if errors.Unwrap(err) != context.Canceled {
					q.logger.Error().Err(err).Msg("error processing partition")
				}
				return err
			}

			metrics.IncrQueuePartitionProcessedCounter(ctx, metrics.CounterOpt{
				PkgName: pkgName,
				Tags:    map[string]any{"shard": metricShardName},
			})
			return nil
		})
	}

	return eg.Wait()
}

// NOTE: Shard is only passed as a reference if the partition was peeked from
// a shard.  It exists for accounting and tracking purposes only, eg. to report shard metrics.
func (q *queue) processPartition(ctx context.Context, p *QueuePartition, pCtr uint, shard *QueueShard) error {
	// Attempt to lease items.  This checks partition-level concurrency limits
	//
	// For oprimization, because this is the only thread that can be leasing
	// jobs for this partition, we store the partition limit and current count
	// as a variable and iterate in the loop without loading keys from the state
	// store.
	//
	// There's no way to know when queue items finish processing;  we don't
	// store average runtimes for queue items (and we don't know because
	// items are dynamic generators).  This means that we have to delay
	// processing the partition by N seconds, meaning the latency is increased by
	// up to this period for scheduled items behind the concurrency limits.
	_, err := duration(ctx, "partition_lease", q.clock.Now(), func(ctx context.Context) (*ulid.ULID, error) {
		return q.PartitionLease(ctx, p, PartitionLeaseDuration)
	})
	if err == ErrPartitionConcurrencyLimit {
		for _, l := range q.lifecycles {
			// Track lifecycles; this function hit a partition limit ahead of
			// even being leased, meaning the function is at max capacity and we skio
			// scanning of jobs altogether.
			go l.OnConcurrencyLimitReached(context.WithoutCancel(ctx), p.WorkflowID)
		}
		metrics.IncrQueuePartitionConcurrencyLimitCounter(ctx, metrics.CounterOpt{PkgName: pkgName})
		return q.PartitionRequeue(ctx, p, q.clock.Now().Truncate(time.Second).Add(PartitionConcurrencyLimitRequeueExtension), true)
	}
	if err == ErrPartitionAlreadyLeased {
		metrics.IncrQueuePartitionLeaseContentionCounter(ctx, metrics.CounterOpt{PkgName: pkgName})
		return nil
	}
	if err == ErrPartitionNotFound {
		// Another worker must have pocessed this partition between
		// this worker's peek and process.  Increase partition
		// contention metric and continue.  This is unsolvable.
		metrics.IncrPartitionGoneCounter(ctx, metrics.CounterOpt{PkgName: pkgName})
		return nil
	}
	if err != nil {
		return fmt.Errorf("error leasing partition: %w", err)
	}

	begin := q.clock.Now()
	defer func() {
		metrics.HistogramProcessPartitionDuration(ctx, q.clock.Since(begin).Milliseconds(), metrics.HistogramOpt{
			PkgName: pkgName,
		})
	}()

	// Ensure that peek doesn't take longer than the partition lease, to
	// reduce contention.
	peekCtx, cancel := context.WithTimeout(ctx, PartitionLeaseDuration)
	defer cancel()

	// We need to round ourselves up to the nearest second, then add another second
	// to peek for jobs in the next <= 1999 milliseconds.
	//
	// There's a really subtle issue:  if two jobs contend for a pause and are scheduled
	// within 5ms of each other, we fetch them in order but we may process them out of
	// order, depending on how long it takes for the item to pass through the channel
	// to the worker, how long Redis takes to lease the item, etc.
	fetch := q.clock.Now().Truncate(time.Second).Add(PartitionLookahead)

	queue, err := duration(peekCtx, "peek", q.clock.Now(), func(ctx context.Context) ([]*QueueItem, error) {
		peek := q.peekSize(ctx, p)
		// NOTE: would love to instrument this value to see it over time per function but
		// it's likely too high of a cardinality
		go metrics.HistogramQueuePeekEWMA(ctx, peek, metrics.HistogramOpt{PkgName: pkgName})
		return q.Peek(peekCtx, p.Queue(), fetch, peek)
	})
	if err != nil {
		return err
	}
	metrics.HistogramQueuePeekSize(ctx, int64(len(queue)), metrics.HistogramOpt{PkgName: pkgName})

	// Record the number of partitions we're leasing.
	metrics.IncrQueuePartitionLeasedCounter(ctx, metrics.CounterOpt{PkgName: pkgName})

	// parallel all queue names with internal mappings for now.
	// XXX: Allow parallel partitions for all functions except for fns opting into FIFO
	_, parallel := q.queueKindMapping[p.Queue()]

	iter := processor{
		partition:            p,
		partitionContinueCtr: pCtr,
		items:                queue,
		shard:                shard,
		queue:                q,
		denies:               newLeaseDenyList(),
		staticTime:           q.clock.Now(),
		parallel:             parallel,
	}

	if processErr := iter.iterate(ctx); processErr != nil {
		// Report the eerror.
		q.logger.Error().Err(processErr).Interface("partition", p).Msg("error iterating queue items")
		return processErr

	}

	if err := q.setPeekEWMA(ctx, p.WorkflowID, int64(iter.ctrConcurrency+iter.ctrRateLimit)); err != nil {
		log.From(ctx).Warn().Err(err).Msg("error recording concurrency limit for EWMA")
	}

	// If we've hit concurrency issues OR we've only hit rate limit issues, re-enqueue the partition
	// with a force:  ensure that we won't re-scan it until 2 seconds in the future.
	if iter.isRequeuable() {
		for _, l := range q.lifecycles {
			go l.OnConcurrencyLimitReached(context.WithoutCancel(ctx), p.WorkflowID)
		}
		// Requeue this partition as we hit concurrency limits.
		metrics.IncrQueuePartitionConcurrencyLimitCounter(ctx, metrics.CounterOpt{PkgName: pkgName})
		return q.PartitionRequeue(ctx, p, q.clock.Now().Truncate(time.Second).Add(PartitionConcurrencyLimitRequeueExtension), true)
	}

	// XXX: If we haven't been able to lease a single item, ensure we enqueue this
	// for a minimum of 5 seconds.

	// Requeue the partition, which reads the next unleased job or sets a time of
	// 30 seconds.  This is why we have to lease items above, else this may return an item that is
	// about to be leased and processed by the worker.
	_, err = duration(ctx, "partition_requeue", q.clock.Now(), func(ctx context.Context) (any, error) {
		err = q.PartitionRequeue(ctx, p, q.clock.Now().Add(PartitionRequeueExtension), false)
		return nil, err
	})
	if err == ErrPartitionGarbageCollected {
		// Safe;  we're preventing this from wasting cycles in the future.
		return nil
	}
	if err != nil {
		return err
	}
	return nil
}

// process processes a job within a partition.
func (q *queue) process(
	ctx context.Context,
	p QueuePartition,
	qi QueueItem,
	partitionCtr uint, // the number of times the partition has been continued
	s *QueueShard,
	f osqueue.RunFunc,
) error {
	var err error
	leaseID := qi.LeaseID

	// Allow the main runner to block until this work is done
	q.wg.Add(1)
	defer q.wg.Done()

	// Continually the lease while this job is being processed.
	extendLeaseTick := q.clock.NewTicker(QueueLeaseDuration / 2)
	defer extendLeaseTick.Stop()

	errCh := make(chan error)
	doneCh := make(chan struct{})

	// Continually extend lease in the background while we're working on this job
	go func() {
		for {
			select {
			case <-doneCh:
				return
			case <-extendLeaseTick.Chan():
				if ctx.Err() != nil {
					// Don't extend lease when the ctx is done.
					return
				}
				if leaseID == nil {
					log.From(ctx).Error().Msg("cannot extend lease since lease ID is nil")
					// Don't extend lease since one doesn't exist
					return
				}
				leaseID, err = q.ExtendLease(ctx, p, qi, *leaseID, QueueLeaseDuration)
				if err != nil && err != ErrQueueItemNotFound && errors.Unwrap(err) != context.Canceled {
					// XXX: Increase counter here.
					q.logger.Error().Err(err).Msg("error extending lease")
					errCh <- fmt.Errorf("error extending lease while processing: %w", err)
					return
				}
			}
		}
	}()

	// XXX: Add a max job time here, configurable.
	jobCtx, jobCancel := context.WithCancel(ctx)
	defer jobCancel()
	// Add the job ID to the queue context.  This allows any logic that handles the run function
	// to inspect job IDs, eg. for tracing or logging, without having to thread this down as
	// arguments.
	jobCtx = osqueue.WithJobID(jobCtx, qi.ID)
	// Same with the group ID, if it exists.
	if qi.Data.GroupID != "" {
		jobCtx = state.WithGroupID(jobCtx, qi.Data.GroupID)
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				// Always retry this job.
				stack := debug.Stack()
				q.logger.Error().Err(fmt.Errorf("%v", r)).Str("stack", string(stack)).Msg("job panicked")
				errCh <- osqueue.AlwaysRetryError(fmt.Errorf("job panicked: %v", r))
			}
		}()

		// This job may be up to 1999 ms in the future, as explained in processPartition.
		// Just... wait until the job is available.
		delay := time.UnixMilli(qi.AtMS).Sub(q.clock.Now())

		if delay > 0 {
			<-q.clock.After(delay)
			q.logger.Trace().
				Int64("at", qi.AtMS).
				Int64("ms", delay.Milliseconds()).
				Msg("delaying job in memory")
		}

		n := q.clock.Now()

		// Track the sojourn (concurrency) latency.
		var sojourn time.Duration
		if qi.EarliestPeekTime > 0 {
			sojourn = n.Sub(time.UnixMilli(qi.EarliestPeekTime))
		}
		jobCtx = context.WithValue(jobCtx, sojournKey, sojourn)

		// Track the latency on average globally.  Do this in a goroutine so that it doesn't
		// at all delay the job during concurrenty locking contention.
		if qi.WallTimeMS == 0 {
			qi.WallTimeMS = qi.AtMS // backcompat while WallTimeMS isn't valid.
		}
		latency := n.Sub(time.UnixMilli(qi.WallTimeMS)) - sojourn
		jobCtx = context.WithValue(jobCtx, latencyKey, latency)

		// store started at and latency in ctx
		jobCtx = context.WithValue(jobCtx, startedAtKey, n)

		go func() {
			// Update the ewma
			latencySem.Lock()
			latencyAvg.Add(float64(latency))
			metrics.GaugeQueueItemLatencyEWMA(ctx, int64(latencyAvg.Value()/1e6), metrics.GaugeOpt{
				PkgName: pkgName,
				Tags:    map[string]any{"kind": qi.Data.Kind},
			})
			latencySem.Unlock()

			// Set the metrics historgram and gauge, which reports the ewma value.
			metrics.HistogramQueueItemLatency(ctx, latency.Milliseconds(), metrics.HistogramOpt{
				PkgName: pkgName,
			})
		}()

		metrics.IncrQueueItemStatusCounter(ctx, metrics.CounterOpt{
			PkgName: pkgName,
			Tags:    map[string]any{"status": "started"},
		})

		runInfo := osqueue.RunInfo{
			Latency:       latency,
			SojournDelay:  sojourn,
			Priority:      p.Priority,
			ShardName:     "<global>",
			ContinueCount: partitionCtr,
		}
		if s != nil {
			runInfo.ShardName = s.Name
		}

		// Call the run func.
		isContinuation, err := f(jobCtx, runInfo, qi.Data)
		extendLeaseTick.Stop()
		if err != nil {
			metrics.IncrQueueItemStatusCounter(ctx, metrics.CounterOpt{
				PkgName: pkgName,
				Tags:    map[string]any{"status": "errored"},
			})
			errCh <- err
			return
		}
		metrics.IncrQueueItemStatusCounter(ctx, metrics.CounterOpt{
			PkgName: pkgName,
			Tags:    map[string]any{"status": "completed"},
		})

		if isContinuation {
			// Add the partition to be continued again.  Note that if we've already
			// continued beyond the limit this is a noop.
			q.addContinue(&p, partitionCtr+1)
		}

		// Closing this channel prevents the goroutine which extends lease from leaking,
		// and dequeues the job
		close(doneCh)
	}()

	select {
	case err := <-errCh:
		// Job errored or extending lease errored.  Cancel the job ASAP.
		jobCancel()

		if osqueue.ShouldRetry(err, qi.Data.Attempt, qi.Data.GetMaxAttempts()) {
			at := q.backoffFunc(qi.Data.Attempt)

			// Attempt to find any RetryAtSpecifier in the error tree.
			unwrapped := err
			for unwrapped != nil {
				// If the error contains a NextRetryAt method, use that to indicate
				// when we should retry.
				if specifier, ok := unwrapped.(osqueue.RetryAtSpecifier); ok {
					next := specifier.NextRetryAt()
					if next != nil {
						at = *next
					}
					break
				}
				unwrapped = errors.Unwrap(unwrapped)
			}

			qi.Data.Attempt += 1
			qi.AtMS = at.UnixMilli()
			if err := q.Requeue(context.WithoutCancel(ctx), p, qi, at); err != nil {
				q.logger.Error().Err(err).Interface("item", qi).Msg("error requeuing job")
				return err
			}
			if _, ok := err.(osqueue.QuitError); ok {
				q.quit <- err
				return err
			}
			return nil
		}

		// Dequeue this entirely, as this permanently failed.
		// XXX: Increase permanently failed counter here.
		if err := q.Dequeue(context.WithoutCancel(ctx), p, qi); err != nil {
			if err == ErrQueueItemNotFound {
				// Safe. The executor may have dequeued.
				return nil
			}
			return err
		}

		if _, ok := err.(osqueue.QuitError); ok {
			q.logger.Warn().Err(err).Msg("received queue quit error")
			q.quit <- err
			return err
		}

	case <-doneCh:
		if err := q.Dequeue(context.WithoutCancel(ctx), p, qi); err != nil {
			if err == ErrQueueItemNotFound {
				// Safe. The executor may have dequeued.
				return nil
			}
			return err
		}
	}

	return nil
}

// sequentialLease is a helper method for concurrently reading the sequential
// lease ID.
func (q *queue) sequentialLease() *ulid.ULID {
	q.seqLeaseLock.RLock()
	defer q.seqLeaseLock.RUnlock()
	if q.seqLeaseID == nil {
		return nil
	}
	copied := *q.seqLeaseID
	return &copied
}

// instrumentationLease is a helper method for concurrently reading the
// instrumentation lease ID.
func (q *queue) instrumentationLease() *ulid.ULID {
	q.instrumentationLeaseLock.RLock()
	defer q.instrumentationLeaseLock.RUnlock()
	if q.instrumentationLeaseID == nil {
		return nil
	}
	copied := *q.instrumentationLeaseID
	return &copied
}

// scavengerLease is a helper method for concurrently reading the sequential
// lease ID.
func (q *queue) scavengerLease() *ulid.ULID {
	q.scavengerLeaseLock.RLock()
	defer q.scavengerLeaseLock.RUnlock()
	if q.scavengerLeaseID == nil {
		return nil
	}
	copied := *q.scavengerLeaseID
	return &copied
}

func (q *queue) capacity() int64 {
	return int64(q.numWorkers) - atomic.LoadInt64(&q.sem.counter)
}

// peekSize returns the number of items to peek for the queue based on a couple of factors
// 1. EWMA of concurrency limit hits
// 2. configured min, max of peek size range
// 3. worker capacity
func (q *queue) peekSize(ctx context.Context, p *QueuePartition) int64 {
	// retrieve the EWMA value
	ewma, err := q.peekEWMA(ctx, p.WorkflowID)
	if err != nil {
		// return the minimum if there's an error
		return q.peekMin
	}

	// set multiplier
	multiplier := q.peekCurrMultiplier
	if multiplier == 0 {
		multiplier = QueuePeekCurrMultiplier
	}

	// set ranges
	pmin := q.peekMin
	if pmin == 0 {
		pmin = QueuePeekMin
	}
	pmax := q.peekMax
	if pmax == 0 {
		pmax = QueuePeekMax
	}

	// calculate size with EWMA and multiplier
	size := ewma * multiplier
	switch {
	case size < pmin:
		size = pmin
	case size > pmax:
		size = pmax
	}

	dur := time.Hour * 24
	qsize, _ := q.partitionSize(ctx, q.u.kg.QueueIndex(p.Queue()), q.clock.Now().Add(dur))
	if qsize > size {
		size = qsize
	}

	// add 10% expecting for some workflow that will finish in the mean time
	cap := int64(float64(q.capacity()) * 1.1)
	if size > cap {
		size = cap
	}

	return size
}

func (q *queue) isSequential() bool {
	l := q.sequentialLease()
	if l == nil {
		return false
	}
	return ulid.Time(l.Time()).After(q.clock.Now())
}

func (q *queue) isScavenger() bool {
	l := q.scavengerLease()
	if l == nil {
		return false
	}
	return ulid.Time(l.Time()).After(q.clock.Now())
}

func (q *queue) isInstrumentator() bool {
	l := q.instrumentationLease()
	if l == nil {
		return false
	}
	return ulid.Time(l.Time()).After(q.clock.Now())
}

// duration is a helper function to record durations of queue operations.
func duration[T any](ctx context.Context, op string, start time.Time, f func(ctx context.Context) (T, error)) (T, error) {
	if start.IsZero() {
		start = time.Now()
	}

	res, err := f(ctx)
	metrics.HistogramQueueOperationDuration(
		ctx,
		time.Since(start).Milliseconds(),
		metrics.HistogramOpt{
			PkgName: pkgName,
			Tags: map[string]any{
				"operation": op,
			},
		},
	)
	return res, err
}

// trackingSemaphore returns a semaphore that tracks closely - but not atomically -
// the total number of items in the semaphore.  This is best effort, and is loosely
// accurate to reduce further contention.
//
// This is only used as an indicator as to whether to scan.
type trackingSemaphore struct {
	*semaphore.Weighted
	counter int64
}

func (t *trackingSemaphore) TryAcquire(n int64) bool {
	if !t.Weighted.TryAcquire(n) {
		return false
	}
	atomic.AddInt64(&t.counter, n)
	return true
}

func (t *trackingSemaphore) Acquire(ctx context.Context, n int64) error {
	if err := t.Weighted.Acquire(ctx, n); err != nil {
		return err
	}
	atomic.AddInt64(&t.counter, n)
	return nil
}

func (t *trackingSemaphore) Release(n int64) {
	t.Weighted.Release(n)
	atomic.AddInt64(&t.counter, -n)
}

type processor struct {
	items     []*QueueItem
	shard     *QueueShard
	partition *QueuePartition
	// partitionContinueCtr is the number of times the partition has currently been continued
	// already in the chain.
	partitionContinueCtr uint

	// queue is the queue that owns this processor.
	queue *queue

	// denies records a denylist as keys hit concurrency and throttling limits.
	// this lets us prevent lease attempts for consecutive keys, as soon as the first
	// key is denied.
	denies *leaseDenies

	// error returned when processing
	err error

	// staticTime is used as the processing time for all items in the queue.
	// We process queue items sequentially, and time progresses linearly as each
	// queue item is processed.  We want to use a static time to prevent out-of-order
	// processing with regards to things like rate limiting;  if we use time.Now(),
	// queue items later in the array may be processed before queue items earlier in
	// the array depending on eg. a rate limit becoming available half way through
	// iteration.
	staticTime time.Time

	// parallel indicates whether the partition's jobs can be processed in parallel.
	// parallel processing breaks best effort fifo but increases throughput.
	parallel bool

	// These flags are used to handle partition rqeueueing.
	ctrSuccess     int32
	ctrConcurrency int32
	ctrRateLimit   int32
}

func (p *processor) iterate(ctx context.Context) error {
	var err error

	eg := errgroup.Group{}
	for _, i := range p.items {
		if p.parallel {
			item := *i
			eg.Go(func() error {
				return p.process(ctx, &item)
			})
			continue
		}

		// non-parallel (sequential fifo) processing.
		if err = p.process(ctx, i); err != nil {
			// always break on the first error;  if processing returns an error we
			// always assume that we stop iterating.
			//
			// we return errors when:
			// * there's no capacity (so dont continue, because FIFO)
			// * we hit fn concurrency limits (so don't continue, because FIFO too)
			// * some other error, which means something went wrong.
			break
		}
	}

	if p.parallel {
		// normalize errors from parallel
		err = eg.Wait()
	}

	if errors.Is(err, errProcessStopIterator) {
		// This is safe;  it's stopping safely but isn't an error.
		return nil
	}
	if errors.Is(err, errProcessNoCapacity) {
		// This is safe;  it's stopping safely but isn't an error.
		return nil
	}

	// someting went wrong.  report the error.
	return err
}

func (p *processor) process(ctx context.Context, item *QueueItem) error {
	// TODO: Create an in-memory mapping of rate limit keys that have been hit,
	//       and don't bother to process if the queue item has a limited key.  This
	//       lessens work done in the queue, as we can `continue` immediately.
	if item.IsLeased(p.queue.clock.Now()) {
		metrics.IncrQueueItemProcessedCounter(ctx, metrics.CounterOpt{
			PkgName: pkgName,
			Tags:    map[string]any{"status": "lease_contention"},
		})
		return nil
	}

	// Cbeck if there's capacity from our local workers atomically prior to leasing our tiems.
	if !p.queue.sem.TryAcquire(1) {
		metrics.IncrQueuePartitionProcessNoCapacityCounter(ctx, metrics.CounterOpt{PkgName: pkgName})
		// Break the entire loop to prevent out of order work.
		return errProcessNoCapacity
	}

	metrics.WorkerQueueCapacityCounter(ctx, 1, metrics.CounterOpt{PkgName: pkgName})

	// Attempt to lease this item before passing this to a worker.  We have to do this
	// synchronously as we need to lease prior to requeueing the partition pointer. If
	// we don't do this here, the workers may not lease the items before calling Peek
	// to re-enqeueu the pointer, which then increases contention - as we requeue a
	// pointer too early.
	//
	// This is safe:  only one process runs scan(), and we guard the total number of
	// available workers with the above semaphore.
	leaseID, err := duration(ctx, "lease", p.queue.clock.Now(), func(ctx context.Context) (*ulid.ULID, error) {
		return p.queue.Lease(ctx, *p.partition, *item, QueueLeaseDuration, p.staticTime, p.denies)
	})

	// NOTE: If this loop ends in an error, we must _always_ release an item from the
	// semaphore to free capacity.  This will happen automatically when the worker
	// finishes processing a queue item on success.
	if err != nil {
		// Continue on and handle the error below.
		p.queue.sem.Release(1)
		metrics.WorkerQueueCapacityCounter(ctx, -1, metrics.CounterOpt{PkgName: pkgName})
	}

	// Check the sojourn delay for this item in the queue. Tracking system latency vs
	// sojourn latency from concurrency is important.
	//
	// Firstly, we check:  does the job store the first peek time?  If so, the
	// delta between now and that time is the sojourn latency.  If not, this is either
	// one of two cases:
	//   - This is a new job in the queue, and we're peeking it for the first time.
	//     Sojourn latency is 0.  Easy.
	//   - We've peeked the queue since adding the job.  At this point, the only
	//     conclusion is that the job wasn't peeked because of concurrency/capacity
	//     issues, so the delta between now - job added is sojourn latency.
	//
	// NOTE: You might see that we use tracking semaphores and the worker itself has
	// a maximum capacity.  We must ALWAYS peek the available capacity in our worker
	// via the above Peek() call so that worker capacity doesn't prevent us from accessing
	// all jobs in a peek.  This would break sojourn latency:  it only works if we know
	// we're quitting early because of concurrency issues in a user's function, NOT because
	// of capacity issues in our system.
	//
	// Anyway, here we set the first peek item to the item's start time if there was a
	// peek since the job was added.
	if p.partition.Last > 0 && p.partition.Last > item.AtMS {
		// Fudge the earliest peek time because we know this wasn't peeked and so
		// the peek time wasn't set;  but, as we were still processing jobs after
		// the job was added this item was concurrency-limited.
		item.EarliestPeekTime = item.AtMS
	}

	// We may return a keyError, which masks the actual error underneath.  If so,
	// grab the cause.
	cause := err
	var key keyError
	if errors.As(err, &key) {
		cause = key.cause
	}

	switch cause {
	case ErrQueueItemThrottled:
		// Here we denylist each throttled key that's been limited here, then ignore
		// any other jobs from being leased as we continue to iterate through the loop.
		// This maintains FIFO ordering amongst all custom concurrency keys.
		p.denies.addThrottled(err)

		p.ctrRateLimit++
		metrics.IncrQueueItemProcessedCounter(ctx, metrics.CounterOpt{
			PkgName: pkgName,
			Tags:    map[string]any{"status": "throttled"},
		})
		return nil
	case ErrPartitionConcurrencyLimit, ErrAccountConcurrencyLimit:
		p.ctrConcurrency++
		// Since the queue is at capacity on a fn or account level, no
		// more jobs in this loop should be worked on - so break.
		//
		// Even if we have capacity for the next job in the loop we do NOT
		// want to claim the job, as this breaks ordering guarantees.  The
		// only safe thing to do when we hit a function or account level
		// concurrency key.
		var status string
		switch cause {
		case ErrPartitionConcurrencyLimit:
			status = "partition_concurrency_limit"
		case ErrAccountConcurrencyLimit:
			status = "account_concurrency_limit"
		}

		metrics.IncrQueueItemProcessedCounter(ctx, metrics.CounterOpt{
			PkgName: pkgName,
			Tags:    map[string]any{"status": status},
		})

		return fmt.Errorf("concurrency hit: %w", errProcessStopIterator)
	case ErrConcurrencyLimitCustomKey0, ErrConcurrencyLimitCustomKey1:
		p.ctrConcurrency++

		// Custom concurrency keys are different.  Each job may have a different key,
		// so we cannot break the loop in case the next job has a different key and
		// has capacity.
		//
		// Here we denylist each concurrency key that's been limited here, then ignore
		// any other jobs from being leased as we continue to iterate through the loop.
		p.denies.addConcurrency(err)

		metrics.IncrQueueItemProcessedCounter(ctx, metrics.CounterOpt{
			PkgName: pkgName,
			Tags:    map[string]any{"status": "custom_key_concurrency_limit"},
		})
		return nil
	case ErrQueueItemNotFound:
		// This is an okay error.  Move to the next job item.
		p.ctrSuccess++ // count as a success for stats purposes.
		metrics.IncrQueueItemProcessedCounter(ctx, metrics.CounterOpt{
			PkgName: pkgName,
			Tags:    map[string]any{"status": "success"},
		})
		return nil
	case ErrQueueItemAlreadyLeased:
		// This is an okay error.  Move to the next job item.
		p.ctrSuccess++ // count as a success for stats purposes.
		metrics.IncrQueueItemProcessedCounter(ctx, metrics.CounterOpt{
			PkgName: pkgName,
			Tags:    map[string]any{"status": "success"},
		})
		return nil
	}

	// Handle other errors.
	if err != nil {
		p.err = fmt.Errorf("error leasing in process: %w", err)
		metrics.IncrQueueItemProcessedCounter(ctx, metrics.CounterOpt{
			PkgName: pkgName,
			Tags:    map[string]any{"status": "error"},
		})
		return p.err
	}

	// Assign the lease ID and pass this to be handled by the available worker.
	// There should always be capacity on this queue as we track capacity via
	// a semaphore.
	item.LeaseID = leaseID

	// increase success counter.
	p.ctrSuccess++
	metrics.IncrQueueItemProcessedCounter(ctx, metrics.CounterOpt{
		PkgName: pkgName,
		Tags:    map[string]any{"status": "success"},
	})
	p.queue.workers <- processItem{P: *p.partition, I: *item, S: p.shard}
	return nil
}

func (p *processor) isRequeuable() bool {
	// if we have concurrency OR we hit rate limiting/throttling.
	return p.ctrConcurrency > 0 || (p.ctrRateLimit > 0 && p.ctrConcurrency == 0 && p.ctrSuccess == 0)
}
