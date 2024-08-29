package redis_state

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/VividCortex/ewma"
	"github.com/cespare/xxhash/v2"
	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
	"github.com/jonboulle/clockwork"
	"github.com/oklog/ulid/v2"
	"github.com/redis/rueidis"
	"github.com/rs/zerolog"
	"golang.org/x/sync/semaphore"
	"gonum.org/v1/gonum/stat/sampleuv"
	"lukechampine.com/frand"

	"github.com/inngest/inngest/pkg/backoff"
	"github.com/inngest/inngest/pkg/consts"
	osqueue "github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/state"
	"github.com/inngest/inngest/pkg/logger"
	"github.com/inngest/inngest/pkg/telemetry/metrics"
	"github.com/inngest/inngest/pkg/telemetry/redis_telemetry"
	"github.com/inngest/inngest/pkg/util"
)

const (
	PartitionSelectionMax = int64(100)
	PartitionPeekMax      = PartitionSelectionMax * 3
)

const (
	pkgName = "redis_state.state.execution.inngest"

	// PartitionLeaseDuration dictates how long a worker holds the lease for
	// a partition.  This gives the worker a right to scan all queue items
	// for that partition to schedule the execution of jobs.
	//
	// Right now, this must be short enough to reduce contention but long enough
	// to account for the latency of peeking QueuePeekMax jobs from Redis.
	PartitionLeaseDuration = 4 * time.Second
	// PartitionRequeueExtension is the length of time that we extend a partition's
	// vesting time when requeueing by default.
	PartitionRequeueExtension = 30 * time.Second

	// PartitionConcurrencyLimitRequeueExtension is the length of time that a partition
	// is requeued if there is no global or partition(function) capacity because of
	// concurrency limits.
	//
	// This is short, as there are still functions that are due to run (ie vesting < now),
	// but long enough to reduce thrash.
	//
	// This means that jobs not started because of concurrency limits incur up to this amount
	// of additional latency.
	PartitionConcurrencyLimitRequeueExtension = 2 * time.Second
	PartitionLookahead                        = time.Second

	// default values
	QueuePeekMin            int64 = 300
	QueuePeekMax            int64 = 5000
	QueuePeekCurrMultiplier int64 = 4 // threshold 25%
	QueuePeekEWMALen        int   = 10
	QueueLeaseDuration            = 20 * time.Second
	ConfigLeaseDuration           = 10 * time.Second
	ConfigLeaseMax                = 20 * time.Second

	PriorityMax     uint = 0
	PriorityDefault uint = 5
	PriorityMin     uint = 9

	// FunctionStartScoreBufferTime is the grace period used to compare function start
	// times to edge enqueue times.
	FunctionStartScoreBufferTime = 10 * time.Second

	defaultNumWorkers           = 100
	defaultPollTick             = 10 * time.Millisecond
	defaultIdempotencyTTL       = 12 * time.Hour
	defaultPartitionConcurrency = 100 // TODO: add function to override.
)

var (
	ErrQueueItemExists               = fmt.Errorf("queue item already exists")
	ErrQueueItemNotFound             = fmt.Errorf("queue item not found")
	ErrQueueItemAlreadyLeased        = fmt.Errorf("queue item already leased")
	ErrQueueItemLeaseMismatch        = fmt.Errorf("item lease does not match")
	ErrQueueItemNotLeased            = fmt.Errorf("queue item is not leased")
	ErrQueuePeekMaxExceedsLimits     = fmt.Errorf("peek exceeded the maximum limit of %d", QueuePeekMax)
	ErrPriorityTooLow                = fmt.Errorf("priority is too low")
	ErrPriorityTooHigh               = fmt.Errorf("priority is too high")
	ErrWeightedSampleRead            = fmt.Errorf("error reading from weighted sample")
	ErrPartitionNotFound             = fmt.Errorf("partition not found")
	ErrPartitionAlreadyLeased        = fmt.Errorf("partition already leased")
	ErrPartitionPeekMaxExceedsLimits = fmt.Errorf("peek exceeded the maximum limit of %d", PartitionPeekMax)
	ErrPartitionGarbageCollected     = fmt.Errorf("partition garbage collected")
	ErrPartitionPaused               = fmt.Errorf("partition is paused")
	ErrConfigAlreadyLeased           = fmt.Errorf("config scanner already leased")
	ErrConfigLeaseExceedsLimits      = fmt.Errorf("config lease duration exceeds the maximum of %d seconds", int(ConfigLeaseMax.Seconds()))
	ErrPartitionConcurrencyLimit     = fmt.Errorf("At partition concurrency limit")
	ErrAccountConcurrencyLimit       = fmt.Errorf("At account concurrency limit")

	// ErrConcurrencyLimitCustomKeyN represents a concurrency limit being hit for *some*, but *not all*
	// jobs in a queue, via custom concurrency keys which are evaluated to a specific string.

	ErrConcurrencyLimitCustomKey0 = fmt.Errorf("At concurrency limit 0")
	ErrConcurrencyLimitCustomKey1 = fmt.Errorf("At concurrency limit 1")

	// internal shard errors
	errShardNotFound     = fmt.Errorf("shard not found")
	errShardIndexLeased  = fmt.Errorf("shard index is already leased")
	errShardIndexInvalid = fmt.Errorf("shard lease index is too high (a lease just expired)")
)

var (
	rnd *frandRNG
)

func init() {
	// For weighted shuffles generate a new rand.
	rnd = &frandRNG{RNG: frand.New(), lock: &sync.Mutex{}}
}

type QueueManager interface {
	osqueue.JobQueueReader
	osqueue.Queue

	Dequeue(ctx context.Context, p QueuePartition, i QueueItem) error
	Requeue(ctx context.Context, p QueuePartition, i QueueItem, at time.Time) error
	RequeueByJobID(ctx context.Context, partitionName string, jobID string, at time.Time) error
}

// PriorityFinder returns the priority for a given queue item.
type PriorityFinder func(ctx context.Context, item QueueItem) uint

// ShardFinder returns the given shard for a workspace ID, or nil if we should
// not shard for the workspace.  We use a workspace ID because each individual
// job AND partition/function lease requires this to be called.
//
// NOTE: This is called frequently:  for every enqueue, lease, partition lease, and so on.
// Expect this to be called tens of thousands of times per second.
type ShardFinder func(ctx context.Context, queueName string, workspaceID uuid.UUID) *QueueShard

type QueueOpt func(q *queue)

func WithName(name string) func(q *queue) {
	return func(q *queue) {
		q.name = name
	}
}

func WithQueueContinuationLimit(limit uint) QueueOpt {
	return func(q *queue) {
		q.continuationLimit = limit
	}
}

func WithQueueLifecycles(l ...QueueLifecycleListener) QueueOpt {
	return func(q *queue) {
		q.lifecycles = l
	}
}

func WithPriorityFinder(pf PriorityFinder) QueueOpt {
	return func(q *queue) {
		q.pf = pf
	}
}

func WithShardFinder(sf ShardFinder) QueueOpt {
	return func(q *queue) {
		q.sf = sf
	}
}

func WithIdempotencyTTL(t time.Duration) QueueOpt {
	return func(q *queue) {
		q.idempotencyTTL = t
	}
}

// WithIdempotencyTTLFunc returns custom idempotecy durations given a QueueItem.
// This allows customization of the idempotency TTL based off of specific jobs.
func WithIdempotencyTTLFunc(f func(context.Context, QueueItem) time.Duration) QueueOpt {
	return func(q *queue) {
		q.idempotencyTTLFunc = f
	}
}

func WithNumWorkers(n int32) QueueOpt {
	return func(q *queue) {
		q.numWorkers = n
	}
}

func WithPeekSizeRange(min int64, max int64) QueueOpt {
	return func(q *queue) {
		q.peekMin = min
		q.peekMax = max
	}
}

func WithPeekConcurrencyMultiplier(m int64) QueueOpt {
	return func(q *queue) {
		q.peekCurrMultiplier = m
	}
}

func WithPeekEWMALength(l int) QueueOpt {
	return func(q *queue) {
		q.peekEWMALen = l
	}
}

// WithPollTick specifies the interval at which the queue will poll the backing store
// for available partitions.
func WithPollTick(t time.Duration) QueueOpt {
	return func(q *queue) {
		q.pollTick = t
	}
}

func WithQueueItemIndexer(i QueueItemIndexer) QueueOpt {
	return func(q *queue) {
		q.itemIndexer = i
	}
}

// WithDenyQueueNames specifies that the worker cannot select jobs from queue partitions
// within the given list of names.  This means that the worker will never work on jobs
// in the specified queues.
//
// NOTE: If this is set and this worker claims the sequential lease, there is no guarantee
// on latency or fairness in the denied queue partitions.
func WithDenyQueueNames(queues ...string) func(q *queue) {
	return func(q *queue) {
		q.denyQueues = queues
		q.denyQueueMap = make(map[string]*struct{})
		q.denyQueuePrefixes = make(map[string]*struct{})
		for _, i := range queues {
			q.denyQueueMap[i] = &struct{}{}
			// If WithDenyQueueNames includes "user:*", trim the asterisc and use
			// this as a prefix match.
			if strings.HasSuffix(i, "*") {
				q.denyQueuePrefixes[strings.TrimSuffix(i, "*")] = &struct{}{}
			}
		}
	}
}

// WithAllowQueueNames specifies that the worker can only select jobs from queue partitions
// within the given list of names.  This means that the worker will never work on jobs in
// other queues.
func WithAllowQueueNames(queues ...string) func(q *queue) {
	return func(q *queue) {
		q.allowQueues = queues
		q.allowQueueMap = make(map[string]*struct{})
		q.allowQueuePrefixes = make(map[string]*struct{})
		for _, i := range queues {
			q.allowQueueMap[i] = &struct{}{}
			// If WithAllowQueueNames includes "user:*", trim the asterisc and use
			// this as a prefix match.
			if strings.HasSuffix(i, "*") {
				q.allowQueuePrefixes[strings.TrimSuffix(i, "*")] = &struct{}{}
			}
		}
	}
}

// WithKindToQueueMapping maps queue.Item.Kind strings to queue names.  For example,
// when pushing a queue.Item with a kind of PayloadEdge, this job can be mapped to
// a specific queue name here.
//
// The mapping must be provided in terms of item kind to queue name.  If the item
// kind doesn't exist in the mapping the job's queue name will be left nil.  This
// means that the item will be placed in the workflow ID's queue.
func WithKindToQueueMapping(mapping map[string]string) func(q *queue) {
	// XXX: Refactor osqueue.Item and this package to resolve these interfaces
	// and clean up this function.
	return func(q *queue) {
		q.queueKindMapping = mapping
	}
}

func WithLogger(l *zerolog.Logger) func(q *queue) {
	return func(q *queue) {
		q.logger = l
	}
}

// WithCustomConcurrencyKeyGenerator assigns a function that returns concurrency keys
// for a given queue item, eg. a step in a function.
func WithCustomConcurrencyKeyGenerator(f QueueItemConcurrencyKeyGenerator) func(q *queue) {
	return func(q *queue) {
		q.customConcurrencyGen = f
	}
}

// WithPartitionConcurrencyKeyGenerator assigns a function that returns concurrency keys
// for a given partition.
func WithPartitionConcurrencyKeyGenerator(f PartitionConcurrencyKeyGenerator) func(q *queue) {
	return func(q *queue) {
		q.partitionConcurrencyGen = f
	}
}

func WithAccountConcurrencyKeyGenerator(f AccountConcurrencyKeyGenerator) func(q *queue) {
	return func(q *queue) {
		q.accountConcurrencyGen = f
	}
}

func WithBackoffFunc(f backoff.BackoffFunc) func(q *queue) {
	return func(q *queue) {
		q.backoffFunc = f
	}
}

// WithClock allows replacing the queue's default (real) clock by a mock, for testing.
func WithClock(c clockwork.Clock) func(q *queue) {
	return func(q *queue) {
		q.clock = c
	}
}

// QueueItemConcurrencyKeyGenerator returns concurrenc keys given a queue item to limits.
//
// Each queue item can have its own concurrency keys.  For example, you can define
// concurrency limits for steps within a function.  This ensures that there will never be
// more than N concurrent items running at once.
type QueueItemConcurrencyKeyGenerator func(ctx context.Context, i QueueItem) []state.CustomConcurrency

// AccountConcurrencyKeyGenerator returns a concurrency key given the queue item's account
// identifier.
type AccountConcurrencyKeyGenerator func(ctx context.Context, i QueueItem) (string, int)

// PartitionConcurrencyKeyGenerator returns a concurrency key and limit for a given partition
// (function).
//
// This allows partitions (read: functions) to set their own concurrency limits.
type PartitionConcurrencyKeyGenerator func(ctx context.Context, p QueuePartition) (string, int)

func NewQueue(u *QueueClient, opts ...QueueOpt) *queue {
	q := &queue{
		u: u,
		pf: func(ctx context.Context, item QueueItem) uint {
			return PriorityDefault
		},
		numWorkers:               defaultNumWorkers,
		wg:                       &sync.WaitGroup{},
		seqLeaseLock:             &sync.RWMutex{},
		scavengerLeaseLock:       &sync.RWMutex{},
		instrumentationLeaseLock: &sync.RWMutex{},
		pollTick:                 defaultPollTick,
		idempotencyTTL:           defaultIdempotencyTTL,
		queueKindMapping:         make(map[string]string),
		logger:                   logger.From(context.Background()),
		partitionConcurrencyGen: func(ctx context.Context, p QueuePartition) (string, int) {
			return p.Queue(), 10_000
		},
		itemIndexer:       QueueItemIndexerFunc,
		backoffFunc:       backoff.DefaultBackoff,
		shardLeases:       []leasedShard{},
		shardLeaseLock:    &sync.Mutex{},
		clock:             clockwork.NewRealClock(),
		continuesLock:     &sync.Mutex{},
		continues:         map[string]continuation{},
		continuationLimit: consts.DefaultQueueContinueLimit,
	}

	for _, opt := range opts {
		opt(q)
	}

	q.sem = &trackingSemaphore{Weighted: semaphore.NewWeighted(int64(q.numWorkers))}
	q.workers = make(chan processItem, q.numWorkers)

	return q
}

type queue struct {
	// name is the identifiable name for this worker, for logging.
	name string

	// redis stores the redis connection to use.
	u  *QueueClient
	pf PriorityFinder
	sf ShardFinder

	lifecycles []QueueLifecycleListener

	accountConcurrencyGen   AccountConcurrencyKeyGenerator
	partitionConcurrencyGen PartitionConcurrencyKeyGenerator
	customConcurrencyGen    QueueItemConcurrencyKeyGenerator

	// idempotencyTTL is the default or static idempotency duration apply to jobs,
	// if idempotencyTTLFunc is not defined.
	idempotencyTTL time.Duration
	// idempotencyTTLFunc returns an time.Duration representing how long job IDs
	// remain idempotent.
	idempotencyTTLFunc func(context.Context, QueueItem) time.Duration
	// pollTick is the interval between each scan for jobs.
	pollTick time.Duration
	// quit is a channel that any method can send on to trigger termination
	// of the Run loop.  This typically accepts an error, but a nil error
	// will still quit the runner.
	quit chan error
	// wg stores a waitgroup for all in-progress jobs
	wg *sync.WaitGroup
	// numWorkers stores the number of workers available to concurrently process jobs.
	numWorkers int32
	// peek min & max sets the range for partitions to peek for items
	peekMin int64
	peekMax int64
	// peekCurrMultiplier is a multiplier used for calculating the dynamic peek size
	// based on the EWMA values
	peekCurrMultiplier int64
	// peekEWMALen is the size of the list to hold the most recent values
	peekEWMALen int
	// workers is a buffered channel which allows scanners to send queue items
	// to workers to be processed
	workers chan processItem
	// sem stores a semaphore controlling the number of jobs currently
	// being processed.  This lets us check whether there's capacity in the queue
	// prior to leasing items.
	sem *trackingSemaphore
	// queueKindMapping stores a map of job kind => queue names
	queueKindMapping map[string]string
	logger           *zerolog.Logger

	// itemIndexer returns indexes for a given queue item.
	itemIndexer QueueItemIndexer

	// denyQueues provides a denylist ensuring that the queue will never claim
	// this partition, meaning that no jobs from this queue will run on this worker.
	denyQueues        []string
	denyQueueMap      map[string]*struct{}
	denyQueuePrefixes map[string]*struct{}

	// allowQueues provides an allowlist, ensuring that the queue only peeks the specified
	// partitions.  jobs from other partitions will never be scanned or processed.
	allowQueues   []string
	allowQueueMap map[string]*struct{}
	// allowQueuePrefixes are memoized prefixes that can be allowed.
	allowQueuePrefixes map[string]*struct{}

	// seqLeaseID stores the lease ID if this queue is the sequential processor.
	// all runners attempt to claim this lease automatically.
	seqLeaseID *ulid.ULID
	// seqLeaseLock ensures that there are no data races writing to
	// or reading from seqLeaseID in parallel.
	seqLeaseLock *sync.RWMutex

	// instrumentationLeaseID stores the lease ID if executor is running queue
	// instrumentations
	instrumentationLeaseID *ulid.ULID
	// instrumentationLeaseLock ensures that there are no data races writing to or
	// reading from instrumentationLeaseID
	instrumentationLeaseLock *sync.RWMutex

	// scavengerLeaseID stores the lease ID if this queue is the scavenger processor.
	// all runners attempt to claim this lease automatically.
	scavengerLeaseID *ulid.ULID
	// scavengerLeaseLock ensures that there are no data races writing to
	// or reading from scavengerLeaseID in parallel.
	scavengerLeaseLock *sync.RWMutex

	// shardLeases represents shards that are leased by the current queue worker.
	shardLeases    []leasedShard
	shardLeaseLock *sync.Mutex

	// backoffFunc is the backoff function to use when retrying operations.
	backoffFunc backoff.BackoffFunc

	clock clockwork.Clock

	// continues stores a map of all partition IDs to continues for a partition.
	// this lets us optimize running consecutive steps for a function, as a continuation, to a specific limit.
	continues map[string]continuation
	// continuesLock protects the continues map.
	continuesLock     *sync.Mutex
	continuationLimit uint
}

// continuation represents a partition continuation, forcung the queue to continue working
// on a partition once a job from a partition has been processed.
type continuation struct {
	partition *QueuePartition
	// count is stored and incremented each time the partition is enqueued.
	count uint
}

// processItem references the queue partition and queue item to be processed by a worker.
// both items need to be passed to a worker as both items are needed to generate concurrency
// keys to extend leases and dequeue.
type processItem struct {
	P QueuePartition
	I QueueItem
	S *QueueShard
	// PCtr represents the number of times the partition has been continued.
	PCtr uint
}

// QueueShard represents a sub-partition for a group of functions.  Shards maintain their
// own partition queues for the functions within the shard.  Note that functions also
// exist within the global partition queue.
type QueueShard struct {
	// Shard name, eg. the company name for isolated execution
	Name string `json:"n"`
	// Priority represents the priority for this shard.
	Priority uint `json:"p"`
	// GuaranteedCapacity represents the minimum number of workers that must
	// always scan this shard.  If zero, there is no guaranteed capacity for
	// the shard.
	GuaranteedCapacity uint `json:"gc"`
	// Leases stores the lease IDs from the workers which are currently leasing the
	// shard.  The workers currently leasing the shard are almost guaranteed to use
	// the shard's partition queue as their source of work.
	Leases []ulid.ULID `json:"leases"`
}

// leasedShard represents a shard leased by a queue.
type leasedShard struct {
	Shard QueueShard
	Lease ulid.ULID
}

// Partition returns the partition name for use when managing the pointer queue to
// individual queues within the shard
func (q QueueShard) Partition() string {
	return q.Name
}

// QueuePartition represents an individual queue for a workflow.  It stores the
// time of the earliest job within the workflow.
type QueuePartition struct {
	QueueName *string `json:"queue,omitempty"`

	// PartitionType is the int-value of the enums.PartitionType for this
	// partition.  By default, partitions are function-scoped without any
	// custom keys.
	PartitionType int `json:"pt,omitempty"`

	WorkflowID  uuid.UUID `json:"wid"`
	WorkspaceID uuid.UUID `json:"wsID"`

	Priority uint `json:"p"`
	Paused   bool `json:"off"`

	// Last represents the time that this partition was last leased, as a millisecond
	// unix epoch.  In essence, we need this to track how frequently we're leasing and
	// attempting to run items in the partition's queue.
	//
	// Without this, we cannot track sojourn latency.
	Last int64 `json:"last"`

	// ForcedAtMS records the time that the partition is forced to, in milliseconds, if
	// the partition has been forced into the future via concurrency issues. This means
	// that it was requeued due to concurrency issues and should not be brought forward
	// when a new step is enqueued, if now < ForcedAtMS.
	ForceAtMS int64 `json:"forceAtMS"`

	// LeaseID represents a lease on this partition.  If the LeaseID is not nil,
	// this partition can be claimed by a shared-nothing worker to work on the
	// queue items within this partition.
	//
	// A lease is shortly held (eg seconds).  It should last long enough for
	// workers to claim QueueItems only.
	LeaseID *ulid.ULID `json:"leaseID"`
}

func (q QueuePartition) Queue() string {
	if q.QueueName == nil {
		return q.WorkflowID.String()
	}
	return *q.QueueName
}

func (q QueuePartition) MarshalBinary() ([]byte, error) {
	return json.Marshal(q)
}

// QueueItem represents an individually queued work scheduled for some time in the
// future.
type QueueItem struct {
	// ID represents a unique identifier for the queue item.  This can be any
	// unique string and will be hashed.  Using the same ID provides idempotency
	// guarantees within the queue's IdempotencyTTL.
	ID string `json:"id"`
	// EarliestPeekTime stores the earliest time that the job was peeked as a
	// millisecond epoch timestamp.
	//
	// This lets us easily track sojourn latency.
	EarliestPeekTime int64 `json:"pt"`
	// AtMS represents the score for the queue item - usually, the current time
	// that this QueueItem needs to be executed at, as a millisecond epoch.
	//
	// Note that due to priority factors and function FIFO manipulation, if we're
	// scheduling a job to run at `Now()` AtMS may be a time in the past to bump
	// the item in the queue.
	//
	// This is necessary for rescoring partitions and checking latencies.
	AtMS int64 `json:"at"`

	// WallTimeMS represents the actual wall time in which the job should run, used to
	// check latencies.  This is NOT used for scoring or ordering and is for internal
	// accounting only.
	//
	// This is set when enqueueing or requeueing a job.
	WallTimeMS int64 `json:"wt"`

	// WorkflowID is the workflow ID that this job belongs to.
	WorkflowID uuid.UUID `json:"wfID"`
	// WorkspaceID is the workspace that this job belongs to.
	WorkspaceID uuid.UUID `json:"wsID"`
	// LeaseID is a ULID which embeds a timestamp denoting when the lease expires.
	LeaseID *ulid.ULID `json:"leaseID,omitempty"`
	// Data represents the enqueued data, eg. the edge to process or the pause
	// to resume.
	Data osqueue.Item `json:"data"`
	// QueueName allows placing this job into a specific queue name.  If the QueueName
	// is nil, the WorkflowID will be used as the queue name.  This allows us to
	// automatically create partitioned queues for each function within Inngest.
	//
	// This should almost always be nil.
	QueueName *string `json:"queueID,omitempty"`
	// IdempotencyPerioud allows customizing the idempotency period for this queue
	// item.  For example, after a debounce queue has been consumed we want to remove
	// the idempotency key immediately;  the same debounce key should become available
	// for another debounced function run.
	IdempotencyPeriod *time.Duration `json:"ip,omitempty"`
}

func (q *QueueItem) SetID(ctx context.Context, str string) {
	q.ID = HashID(ctx, str)
}

// Score returns the score (time that the item should run) for the queue item.
//
// NOTE: In order to prioritize finishing older function runs with a busy function
// queue, we sometimes use the function run's "started at" time to enqueue edges which
// run steps.  This lets us push older function steps to the beginning of the queue,
// ensuring they run before other newer function runs.
//
// We can ONLY do this for the first attempt, and we can ONLY do this for edges that
// are not sleeps (eg. immediate runs)
func (q QueueItem) Score(now time.Time) int64 {
	if now.IsZero() {
		now = time.Now()
	}

	// If this is not a start/simple edge/edge error, we can ignore this.
	if (q.Data.Kind != osqueue.KindStart &&
		q.Data.Kind != osqueue.KindEdge &&
		q.Data.Kind != osqueue.KindEdgeError) || q.Data.Attempt > 0 {
		return q.AtMS
	}

	// If this is > 2 seconds in the future, don't mess with the time.
	// This prevents any accidental fudging of future run times, even if the
	// kind is edge (which should never exist... but, better to be safe).
	if q.AtMS > now.Add(consts.FutureAtLimit).UnixMilli() {
		return q.AtMS
	}

	// Get the time for the function, based off of the run ID.
	startAt := int64(q.Data.Identifier.RunID.Time())

	if startAt == 0 {
		return q.AtMS
	}

	// Remove the PriorityFactor from the time to push higher priority work
	// earlier.
	return startAt - q.Data.GetPriorityFactor()
}

func (q QueueItem) MarshalBinary() ([]byte, error) {
	return json.Marshal(q)
}

// Queue returns the queue name for this queue item.  This is the
// workflow ID of the QueueItem unless the QueueName is specifically
// set.
func (q QueueItem) Queue() string {
	if q.QueueName == nil {
		return q.WorkflowID.String()
	}
	return *q.QueueName
}

// IsLeased checks if the QueueItem is currently already leased or not
// based on the time passed in.
func (q QueueItem) IsLeased(time time.Time) bool {
	return q.LeaseID != nil && ulid.Time(q.LeaseID.Time()).After(time)
}

// RunJobs returns a list of jobs that are due to run for a given run ID.
func (q *queue) RunJobs(ctx context.Context, workspaceID, workflowID uuid.UUID, runID ulid.ULID, limit, offset int64) ([]osqueue.JobResponse, error) {
	if limit > 1000 || limit <= 0 {
		limit = 1000
	}

	ctx = redis_telemetry.WithScope(redis_telemetry.WithOpName(ctx, "RunJobs"), redis_telemetry.ScopeQueue)

	cmd := q.u.unshardedRc.B().Zscan().Key(q.u.kg.RunIndex(runID)).Cursor(uint64(offset)).Count(limit).Build()
	jobIDs, err := q.u.unshardedRc.Do(ctx, cmd).AsScanEntry()
	if err != nil {
		return nil, fmt.Errorf("error reading index: %w", err)
	}

	if len(jobIDs.Elements) == 0 {
		return []osqueue.JobResponse{}, nil
	}

	// Get all job items.
	jsonItems, err := q.u.unshardedRc.Do(ctx, q.u.unshardedRc.B().Hmget().Key(q.u.kg.QueueItem()).Field(jobIDs.Elements...).Build()).AsStrSlice()
	if err != nil {
		return nil, fmt.Errorf("error reading jobs: %w", err)
	}

	resp := []osqueue.JobResponse{}
	for _, str := range jsonItems {
		if len(str) == 0 {
			continue
		}
		qi := &QueueItem{}

		if err := json.Unmarshal([]byte(str), qi); err != nil {
			return nil, fmt.Errorf("error unmarshalling queue item: %w", err)
		}
		if qi.Data.Identifier.WorkspaceID != workspaceID {
			continue
		}
		cmd := q.u.unshardedRc.B().Zrank().Key(q.u.kg.QueueIndex(workflowID.String())).Member(qi.ID).Build()
		pos, err := q.u.unshardedRc.Do(ctx, cmd).AsInt64()
		if !rueidis.IsRedisNil(err) && err != nil {
			return nil, fmt.Errorf("error reading queue position: %w", err)
		}
		resp = append(resp, osqueue.JobResponse{
			At:       time.UnixMilli(qi.AtMS),
			Position: pos,
			Kind:     qi.Data.Kind,
			Attempt:  qi.Data.Attempt,
			Raw:      qi,
		})
	}

	return resp, nil
}

func (q *queue) OutstandingJobCount(ctx context.Context, workspaceID, workflowID uuid.UUID, runID ulid.ULID) (int, error) {
	ctx = redis_telemetry.WithScope(redis_telemetry.WithOpName(ctx, "OutstandingJobCount"), redis_telemetry.ScopeQueue)

	cmd := q.u.unshardedRc.B().Zcard().Key(q.u.kg.RunIndex(runID)).Build()
	count, err := q.u.unshardedRc.Do(ctx, cmd).AsInt64()
	if err != nil {
		return 0, fmt.Errorf("error counting index cardinality: %w", err)
	}
	return int(count), nil
}

func (q *queue) StatusCount(ctx context.Context, workflowID uuid.UUID, status string) (int64, error) {
	ctx = redis_telemetry.WithScope(redis_telemetry.WithOpName(ctx, "StatusCount"), redis_telemetry.ScopeQueue)

	key := q.u.kg.Status(status, workflowID)
	cmd := q.u.unshardedRc.B().Zcount().Key(key).Min("-inf").Max("+inf").Build()
	count, err := q.u.unshardedRc.Do(ctx, cmd).AsInt64()
	if err != nil {
		return 0, fmt.Errorf("error inspecting function queue status: %w", err)
	}
	return count, nil
}

func (q *queue) RunningCount(ctx context.Context, workflowID uuid.UUID) (int64, error) {
	ctx = redis_telemetry.WithScope(redis_telemetry.WithOpName(ctx, "RunningCount"), redis_telemetry.ScopeQueue)

	// Load the partition for a given queue.  This allows us to generate the concurrency
	// key properly via the given function.
	//
	// TODO: Remove the ability to change keys based off of initialized inputs.  It's more trouble than
	// it's worth, and ends up meaning we have more queries to write (such as this) in order to load
	// relevant data.
	cmd := q.u.unshardedRc.B().Hget().Key(q.u.kg.PartitionItem()).Field(workflowID.String()).Build()
	enc, err := q.u.unshardedRc.Do(ctx, cmd).AsBytes()
	if rueidis.IsRedisNil(err) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("error fetching partition: %w", err)
	}
	item := &QueuePartition{}
	if err = json.Unmarshal(enc, item); err != nil {
		return 0, fmt.Errorf("error reading partition item: %w", err)
	}

	// Fetch the concurrency via the partition concurrency name.
	pk, _ := q.partitionConcurrencyGen(ctx, *item)
	key := q.u.kg.Concurrency("p", pk)
	cmd = q.u.unshardedRc.B().Zcard().Key(key).Build()
	count, err := q.u.unshardedRc.Do(ctx, cmd).AsInt64()
	if err != nil {
		return 0, fmt.Errorf("error inspecting running job count: %w", err)
	}
	return count, nil
}

// SetFunctionPaused sets the "Paused" flag (represented in JSON as "off") for the given
// function ID's queue partition.
func (q *queue) SetFunctionPaused(ctx context.Context, fnID uuid.UUID, paused bool) error {
	ctx = redis_telemetry.WithScope(redis_telemetry.WithOpName(ctx, "SetFunctionPaused"), redis_telemetry.ScopeQueue)

	pausedArg := "0"
	if paused {
		pausedArg = "1"
	}
	args, err := StrSlice([]any{
		fnID.String(),
		pausedArg,
	})
	if err != nil {
		return err
	}

	keys := []string{q.u.kg.PartitionItem()}
	status, err := scripts["queue/partitionSetPaused"].Exec(
		redis_telemetry.WithScriptName(ctx, "partitionSetPaused"),
		q.u.unshardedRc,
		keys,
		args,
	).AsInt64()
	if err != nil {
		return fmt.Errorf("error updating paused state: %w", err)
	}
	switch status {
	case 0:
		return nil
	case 1:
		return ErrPartitionNotFound
	default:
		return fmt.Errorf("unknown response updating paused state: %d", status)
	}
}

// EnqueueItem enqueues a QueueItem.  It creates a QueuePartition for the workspace
// if a partition does not exist.
//
// The QueueItem's ID can be a zero UUID;  if the ID is a zero value a new ID
// will be created for the queue item.
//
// The queue score must be added in milliseconds to process sub-second items in order.
func (q *queue) EnqueueItem(ctx context.Context, i QueueItem, at time.Time) (QueueItem, error) {
	ctx = redis_telemetry.WithScope(redis_telemetry.WithOpName(ctx, "EnqueueItem"), redis_telemetry.ScopeQueue)

	if len(i.ID) == 0 {
		i.SetID(ctx, ulid.MustNew(ulid.Now(), rnd).String())
	} else {
		// Hash the ID.
		// TODO: What if this is already hashed?
		i.ID = HashID(ctx, i.ID)
	}

	// TODO: If the length of ID >= max, error.

	priority := PriorityMin
	if q.pf != nil {
		priority = q.pf(ctx, i)
	}

	if priority > PriorityMin {
		return i, ErrPriorityTooLow
	}
	if priority < PriorityMax {
		return i, ErrPriorityTooHigh
	}

	if i.WallTimeMS == 0 {
		i.WallTimeMS = at.UnixMilli()
	}

	if at.Before(q.clock.Now()) {
		// Normalize to now to minimize latency.
		i.WallTimeMS = q.clock.Now().UnixMilli()
	}

	// Add the At timestamp, if not included.
	if i.AtMS == 0 {
		i.AtMS = at.UnixMilli()
	}

	if i.Data.JobID == nil {
		i.Data.JobID = &i.ID
	}

	partitionTime := at
	if at.Before(q.clock.Now()) {
		// We don't want to enqueue partitions (pointers to fns) before now.
		// Doing so allows users to stay at the front of the queue for
		// leases.
		partitionTime = q.clock.Now()
	}

	// Get the queue name from the queue item.  This allows utilization of
	// the partitioned queue for jobs with custom queue names, vs utilizing
	// workflow IDs in every case.
	qn := i.Queue()

	qp := QueuePartition{
		QueueName:   i.QueueName,
		WorkflowID:  i.WorkflowID,
		WorkspaceID: i.WorkspaceID,
		Priority:    priority,
	}

	var (
		shard     *QueueShard
		shardName string
	)
	if q.sf != nil {
		shard = q.sf(ctx, i.Queue(), i.WorkspaceID)
		if shard != nil {
			shardName = shard.Name
			shard.Leases = []ulid.ULID{}
		}
	}

	keys := []string{
		q.u.kg.QueueItem(),                    // Queue item
		q.u.kg.QueueIndex(qn),                 // Queue sorted set
		q.u.kg.PartitionItem(),                // Partition item, map
		q.u.kg.PartitionMeta(qn),              // Partition item
		q.u.kg.GlobalPartitionIndex(),         // Global partition queue
		q.u.kg.ShardPartitionIndex(shardName), // Shard queue
		q.u.kg.Shards(),
		q.u.kg.Idempotency(i.ID),
	}
	// Append indexes
	for _, idx := range q.itemIndexer(ctx, i, q.u.kg) {
		if idx != "" {
			keys = append(keys, idx)
		}
	}

	args, err := StrSlice([]any{
		i,
		i.ID,
		at.UnixMilli(),
		qn,
		qp,
		partitionTime.Unix(),
		shard,
		shardName,
		q.clock.Now().UnixMilli(),
	})

	if err != nil {
		return i, err
	}
	status, err := scripts["queue/enqueue"].Exec(
		redis_telemetry.WithScriptName(ctx, "enqueue"),
		q.u.Client(),
		keys,
		args,
	).AsInt64()
	if err != nil {
		return i, fmt.Errorf("error enqueueing item: %w", err)
	}
	switch status {
	case 0:
		return i, nil
	case 1:
		return i, ErrQueueItemExists
	default:
		return i, fmt.Errorf("unknown response enqueueing item: %v (%T)", status, status)
	}
}

// Peek takes n items from a queue, up until QueuePeekMax.  For peeking workflow/
// function jobs the queue name must be the ID of the workflow;  each workflow has
// its own queue of jobs using its ID as the queue name.
//
// If limit is -1, this will return the first unleased item - representing the next available item in the
// queue.
func (q *queue) Peek(ctx context.Context, queueName string, until time.Time, limit int64) ([]*QueueItem, error) {
	ctx = redis_telemetry.WithScope(redis_telemetry.WithOpName(ctx, "Peek"), redis_telemetry.ScopeQueue)

	// Check whether limit is -1, peeking next available time
	isPeekNext := limit == -1

	if limit > QueuePeekMax {
		// Lua's max unpack() length is 8000; don't allow users to peek more than
		// 1k at a time regardless.
		limit = QueuePeekMax
	}
	if limit <= 0 {
		limit = QueuePeekMin
	}
	if isPeekNext {
		limit = 1
	}

	args, err := StrSlice([]any{
		until.UnixMilli(),
		limit,
	})
	if err != nil {
		return nil, err
	}
	res, err := scripts["queue/peek"].Exec(
		redis_telemetry.WithScriptName(ctx, "peek"),
		q.u.unshardedRc,
		[]string{
			q.u.kg.QueueIndex(queueName),
			q.u.kg.QueueItem(),
		},
		args,
	).ToAny()
	if err != nil {
		return nil, fmt.Errorf("error peeking queue items: %w", err)
	}
	items, ok := res.([]any)
	if !ok {
		return nil, nil
	}
	if len(items) == 0 {
		return nil, nil
	}

	if isPeekNext {
		i, err := q.decodeQueueItemFromPeek(items[0].(string), q.clock.Now())
		if err != nil {
			return nil, err
		}
		return []*QueueItem{i}, nil
	}

	now := q.clock.Now()
	return util.ParallelDecode(items, func(val any) (*QueueItem, error) {
		str, _ := val.(string)
		return q.decodeQueueItemFromPeek(str, now)
	})
}

func (q *queue) decodeQueueItemFromPeek(str string, now time.Time) (*QueueItem, error) {
	qi := &QueueItem{}
	if err := json.Unmarshal(unsafe.Slice(unsafe.StringData(str), len(str)), qi); err != nil {
		return nil, fmt.Errorf("error unmarshalling peeked queue item: %w", err)
	}
	if qi.IsLeased(now) {
		// Leased item, don't return.
		return nil, nil
	}
	// The nested osqueue.Item never has an ID set;  always re-set it
	qi.Data.JobID = &qi.ID
	return qi, nil
}

// RequeueByJobID requeues a job for a specific time given a partition name and job ID.
//
// If the queue item referenced by the job ID is not outstanding (ie. it has a lease, is in
// progress, or doesn't exist) this returns an error.
func (q *queue) RequeueByJobID(ctx context.Context, partitionName string, jobID string, at time.Time) error {
	ctx = redis_telemetry.WithScope(redis_telemetry.WithOpName(ctx, "RequeueByJobID"), redis_telemetry.ScopeQueue)

	jobID = HashID(ctx, jobID)

	// Find the queue item so that we can fetch the shard info.
	qi := &QueueItem{}
	if err := q.u.unshardedRc.Do(ctx, q.u.unshardedRc.B().Hget().Key(q.u.kg.QueueItem()).Field(jobID).Build()).DecodeJSON(qi); err != nil {
		return err
	}

	var shardName string
	if q.sf != nil {
		if shard := q.sf(ctx, qi.Queue(), qi.WorkspaceID); shard != nil {
			shardName = shard.Name
		}
	}

	keys := []string{
		q.u.kg.QueueIndex(partitionName),
		q.u.kg.QueueItem(),
		q.u.kg.GlobalPartitionIndex(),         // Global partition queue
		q.u.kg.ShardPartitionIndex(shardName), // Shard partition queue
		q.u.kg.PartitionItem(),                // Partition hash
	}
	status, err := scripts["queue/requeueByID"].Exec(
		redis_telemetry.WithScriptName(ctx, "requeueByID"),
		q.u.unshardedRc,
		keys,
		[]string{
			jobID,
			strconv.Itoa(int(at.UnixMilli())),
			partitionName,
			strconv.Itoa(int(q.clock.Now().UnixMilli())),
		},
	).AsInt64()
	if err != nil {
		return fmt.Errorf("error requeueing item: %w", err)
	}
	switch status {
	case 0:
		return nil
	case -1:
		return ErrQueueItemNotFound
	case -2:
		return ErrQueueItemAlreadyLeased
	default:
		return fmt.Errorf("unknown requeue by id response: %d", status)
	}

}

// Lease temporarily dequeues an item from the queue by obtaining a lease, preventing
// other workers from working on this queue item at the same time.
//
// Obtaining a lease updates the vesting time for the queue item until now() +
// lease duration. This returns the newly acquired lease ID on success.
func (q *queue) Lease(ctx context.Context, p QueuePartition, item QueueItem, duration time.Duration, now time.Time, denies *leaseDenies) (*ulid.ULID, error) {
	ctx = redis_telemetry.WithScope(redis_telemetry.WithOpName(ctx, "Lease"), redis_telemetry.ScopeQueue)

	var (
		ak, pk string // account, partition concurrency key
		ac, pc int    // account, partiiton concurrency max

		customKeys   = make([]string, 2)
		customLimits = make([]int, 2)
	)

	if item.Data.Throttle != nil && denies != nil && denies.denyThrottle(item.Data.Throttle.Key) {
		return nil, ErrQueueItemThrottled
	}

	// Required.
	//
	// This should be found by calling function.ConcurrencyLimit() to return
	// the lowest concurrency limit available.  It limits the capacity of all
	// runs for the given function.
	pk, pc = q.partitionConcurrencyGen(ctx, p)
	// Check to see if this key has already been denied in the lease iteration.
	// If so, fail early.
	if denies != nil && denies.denyConcurrency(pk) {
		// Note that we do not need to wrap the key as the key is already present.
		return nil, ErrPartitionConcurrencyLimit
	}

	// optional
	if q.accountConcurrencyGen != nil {
		ak, ac = q.accountConcurrencyGen(ctx, item)
		// Check to see if this key has already been denied in the lease iteration.
		// If so, fail early.
		if denies != nil && denies.denyConcurrency(ak) {
			return nil, ErrAccountConcurrencyLimit
		}
	}
	if q.customConcurrencyGen != nil {
		// Get the custom concurrency key, if available.
		for i, item := range q.customConcurrencyGen(ctx, item) {
			if i >= 2 {
				// We only support two concurrency keys right now.
				break
			}

			// Check to see if this key has already been denied in the lease iteration.
			// If so, fail early.
			if denies != nil && denies.denyConcurrency(item.Key) {
				if i == 0 {
					return nil, ErrConcurrencyLimitCustomKey0
				}
				return nil, ErrConcurrencyLimitCustomKey1
			}

			customKeys[i] = item.Key
			customLimits[i] = item.Limit
		}
	}

	leaseID, err := ulid.New(ulid.Timestamp(q.clock.Now().Add(duration).UTC()), rnd)
	if err != nil {
		return nil, fmt.Errorf("error generating id: %w", err)
	}

	var shardName string
	if q.sf != nil {
		if shard := q.sf(ctx, item.Queue(), item.WorkspaceID); shard != nil {
			shardName = shard.Name
		}
	}

	keys := []string{
		q.u.kg.QueueItem(),
		q.u.kg.QueueIndex(item.Queue()),
		q.u.kg.PartitionMeta(item.Queue()),
		q.u.kg.Concurrency("account", ak),
		q.u.kg.Concurrency("p", pk),
		q.u.kg.Concurrency("custom", customKeys[0]),
		q.u.kg.Concurrency("custom", customKeys[1]),
		q.u.kg.ConcurrencyIndex(),
		q.u.kg.GlobalPartitionIndex(),
		q.u.kg.ShardPartitionIndex(shardName),
		q.u.kg.ThrottleKey(item.Data.Throttle),
	}
	args, err := StrSlice([]any{
		item.ID,
		leaseID.String(),
		now.UnixMilli(),
		ac,
		pc,
		customLimits[0],
		customLimits[1],
		p.Queue(),
	})
	if err != nil {
		return nil, err
	}
	status, err := scripts["queue/lease"].Exec(
		redis_telemetry.WithScriptName(ctx, "lease"),
		q.u.unshardedRc,
		keys,
		args,
	).ToInt64()
	if err != nil {
		return nil, fmt.Errorf("error leasing queue item: %w", err)
	}
	switch status {
	case 0:
		return &leaseID, nil
	case 1:
		return nil, ErrQueueItemNotFound
	case 2:
		return nil, ErrQueueItemAlreadyLeased
	case 3:
		// fn limit relevant to all runs in the fn
		return nil, newKeyError(ErrPartitionConcurrencyLimit, pk)
	case 4:
		return nil, newKeyError(ErrAccountConcurrencyLimit, ak)
	case 5:
		return nil, newKeyError(ErrConcurrencyLimitCustomKey0, customKeys[0])
	case 6:
		return nil, newKeyError(ErrConcurrencyLimitCustomKey1, customKeys[1])
	case 7:
		return nil, newKeyError(ErrQueueItemThrottled, item.Data.Throttle.Key)
	default:
		return nil, fmt.Errorf("unknown response leasing item: %d", status)
	}
}

// ExtendLease extens the lease for a given queue item, given the queue item is currently
// leased with the given ID.  This returns a new lease ID if the lease is successfully ended.
//
// The existing lease ID must be passed in so that we can guarantee that the worker
// renewing the lease still owns the lease.
//
// Renewing a lease updates the vesting time for the queue item until now() +
// lease duration. This returns the newly acquired lease ID on success.
func (q *queue) ExtendLease(ctx context.Context, p QueuePartition, i QueueItem, leaseID ulid.ULID, duration time.Duration) (*ulid.ULID, error) {
	ctx = redis_telemetry.WithScope(redis_telemetry.WithOpName(ctx, "ExtendLease"), redis_telemetry.ScopeQueue)

	var (
		ak, pk     string // account, partition, custom concurrency key
		customKeys = make([]string, 2)
	)
	// required
	pk, _ = q.partitionConcurrencyGen(ctx, p)
	// optional
	if q.accountConcurrencyGen != nil {
		ak, _ = q.accountConcurrencyGen(ctx, i)
	}
	if q.customConcurrencyGen != nil {
		// Get the custom concurrency key, if available.
		for n, item := range q.customConcurrencyGen(ctx, i) {
			if n >= 2 {
				// We only support two concurrency keys right now.
				break
			}
			customKeys[n] = item.Key
		}
	}

	newLeaseID, err := ulid.New(ulid.Timestamp(q.clock.Now().Add(duration).UTC()), rnd)
	if err != nil {
		return nil, fmt.Errorf("error generating id: %w", err)
	}

	keys := []string{
		q.u.kg.QueueItem(),
		q.u.kg.QueueIndex(i.Queue()),
		q.u.kg.GlobalPartitionIndex(),
		q.u.kg.Concurrency("account", ak),
		q.u.kg.Concurrency("p", pk),
		q.u.kg.Concurrency("custom", customKeys[0]),
		q.u.kg.Concurrency("custom", customKeys[1]),
	}

	args, err := StrSlice([]any{
		i.ID,
		leaseID.String(),
		newLeaseID.String(),
		p.Queue(),
	})
	if err != nil {
		return nil, err
	}
	status, err := scripts["queue/extendLease"].Exec(
		redis_telemetry.WithScriptName(ctx, "extendLease"),
		q.u.unshardedRc,
		keys,
		args,
	).AsInt64()
	if err != nil {
		return nil, fmt.Errorf("error extending lease: %w", err)
	}
	switch status {
	case 0:
		return &newLeaseID, nil
	case 1:
		return nil, ErrQueueItemNotFound
	case 2:
		return nil, ErrQueueItemNotLeased
	case 3:
		return nil, ErrQueueItemLeaseMismatch
	default:
		return nil, fmt.Errorf("unknown response extending lease: %d", status)
	}
}

// Dequeue removes an item from the queue entirely.
func (q *queue) Dequeue(ctx context.Context, p QueuePartition, i QueueItem) error {
	ctx = redis_telemetry.WithScope(redis_telemetry.WithOpName(ctx, "Dequeue"), redis_telemetry.ScopeQueue)

	var (
		ak, pk     string // account, partition, custom concurrency key
		customKeys = make([]string, 2)
	)
	// required
	pk, _ = q.partitionConcurrencyGen(ctx, p)
	// optional
	if q.accountConcurrencyGen != nil {
		ak, _ = q.accountConcurrencyGen(ctx, i)
	}
	if q.customConcurrencyGen != nil {
		// Get the custom concurrency key, if available.
		for n, item := range q.customConcurrencyGen(ctx, i) {
			if n >= 2 {
				// We only support two concurrency keys right now.
				break
			}
			customKeys[n] = item.Key
		}
	}

	qn := i.Queue()
	keys := []string{
		q.u.kg.QueueItem(),
		q.u.kg.QueueIndex(qn),
		q.u.kg.PartitionMeta(qn),
		q.u.kg.Idempotency(i.ID),
		q.u.kg.Concurrency("account", ak),
		q.u.kg.Concurrency("p", pk),
		q.u.kg.Concurrency("custom", customKeys[0]),
		q.u.kg.Concurrency("custom", customKeys[1]),
		q.u.kg.ConcurrencyIndex(),
	}
	// Append indexes
	for _, idx := range q.itemIndexer(ctx, i, q.u.kg) {
		if idx != "" {
			keys = append(keys, idx)
		}
	}

	idempotency := q.idempotencyTTL
	if q.idempotencyTTLFunc != nil {
		idempotency = q.idempotencyTTLFunc(ctx, i)
	}

	args, err := StrSlice([]any{
		i.ID,
		int(idempotency.Seconds()),
		p.Queue(),
	})
	if err != nil {
		return err
	}
	status, err := scripts["queue/dequeue"].Exec(
		redis_telemetry.WithScriptName(ctx, "dequeue"),
		q.u.unshardedRc,
		keys,
		args,
	).AsInt64()
	if err != nil {
		return fmt.Errorf("error dequeueing item: %w", err)
	}
	switch status {
	case 0:
		return nil
	case 1:
		return ErrQueueItemNotFound
	default:
		return fmt.Errorf("unknown response dequeueing item: %d", status)
	}
}

// Requeue requeues an item in the future.
func (q *queue) Requeue(ctx context.Context, p QueuePartition, i QueueItem, at time.Time) error {
	ctx = redis_telemetry.WithScope(redis_telemetry.WithOpName(ctx, "Requeue"), redis_telemetry.ScopeQueue)

	priority := PriorityMin
	if q.pf != nil {
		priority = q.pf(ctx, i)
	}

	if priority > PriorityMin {
		return ErrPriorityTooLow
	}

	var (
		ak, pk     string // account, partition, custom concurrency key
		customKeys = make([]string, 2)
	)

	// required
	pk, _ = q.partitionConcurrencyGen(ctx, p)
	// optional
	if q.accountConcurrencyGen != nil {
		ak, _ = q.accountConcurrencyGen(ctx, i)
	}
	if q.customConcurrencyGen != nil {
		// Get the custom concurrency key, if available.
		for n, item := range q.customConcurrencyGen(ctx, i) {
			if n >= 2 {
				// We only support two concurrency keys right now.
				break
			}
			customKeys[n] = item.Key
		}
	}

	// Unset any lease ID as this is requeued.
	i.LeaseID = nil
	// Update the At timestamp.
	// NOTE: This does no priority factorization or FIFO for function ordering,
	// eg. adjusting AtMS based off of function run time.
	i.AtMS = at.UnixMilli()
	// Update the wall time that this should run at.
	i.WallTimeMS = at.UnixMilli()

	var shardName string
	if q.sf != nil {
		if shard := q.sf(ctx, i.Queue(), i.WorkspaceID); shard != nil {
			shardName = shard.Name
		}
	}

	keys := []string{
		q.u.kg.QueueItem(),
		q.u.kg.QueueIndex(i.Queue()),
		q.u.kg.PartitionMeta(i.Queue()),
		q.u.kg.GlobalPartitionIndex(),
		q.u.kg.Concurrency("account", ak),
		q.u.kg.Concurrency("p", pk),
		q.u.kg.Concurrency("custom", customKeys[0]),
		q.u.kg.Concurrency("custom", customKeys[1]),
		q.u.kg.ConcurrencyIndex(),
		q.u.kg.ShardPartitionIndex(shardName),
	}
	// Append indexes
	for _, idx := range q.itemIndexer(ctx, i, q.u.kg) {
		if idx != "" {
			keys = append(keys, idx)
		}
	}

	args, err := StrSlice([]any{
		i,
		i.ID,
		at.UnixMilli(),
		i.Queue(),
	})
	if err != nil {
		return err
	}
	status, err := scripts["queue/requeue"].Exec(
		redis_telemetry.WithScriptName(ctx, "requeue"),
		q.u.unshardedRc,
		keys,
		args,
	).AsInt64()
	if err != nil {
		return fmt.Errorf("error requeueing item: %w", err)
	}
	switch status {
	case 0:
		return nil
	case 1:
		// This should only ever happen if a run is cancelled and all queue items
		// are deleted before requeueing.
		return ErrQueueItemNotFound
	default:
		return fmt.Errorf("unknown response requeueing item: %v (%T)", status, status)
	}
}

// PartitionLease leases a partition for a given workflow ID.  It returns the new lease ID.
//
// NOTE: This does not check the queue/partition name against allow or denylists;  it assumes
// that the worker always wants to lease the given queue.  Filtering must be done when peeking
// when running a worker.
func (q *queue) PartitionLease(ctx context.Context, p *QueuePartition, duration time.Duration) (*ulid.ULID, error) {
	ctx = redis_telemetry.WithScope(redis_telemetry.WithOpName(ctx, "PartitionLease"), redis_telemetry.ScopeQueue)

	var (
		concurrencyKey string
		concurrency    = defaultPartitionConcurrency
	)
	if q.partitionConcurrencyGen != nil {
		concurrencyKey, concurrency = q.partitionConcurrencyGen(ctx, *p)
	}

	// XXX: Check for function throttling prior to leasing;  if it's throttled we can requeue
	// the pointer and back off.  A question here is enqueuing new items onto the partition
	// will reset the pointer update, leading to thrash.
	now := q.clock.Now()
	leaseExpires := now.Add(duration).UTC().Truncate(time.Millisecond)
	leaseID, err := ulid.New(ulid.Timestamp(leaseExpires), rnd)
	if err != nil {
		return nil, fmt.Errorf("error generating id: %w", err)
	}

	var shardName string
	if q.sf != nil {
		if shard := q.sf(ctx, p.Queue(), p.WorkspaceID); shard != nil {
			shardName = shard.Name
		}
	}

	keys := []string{
		q.u.kg.PartitionItem(),
		q.u.kg.GlobalPartitionIndex(),
		q.u.kg.ShardPartitionIndex(shardName),
		q.u.kg.Concurrency("p", concurrencyKey),
	}

	args, err := StrSlice([]any{
		p.Queue(),
		leaseID.String(),
		now.UnixMilli(),
		leaseExpires.Unix(),
		concurrency,
	})
	if err != nil {
		return nil, err
	}
	result, err := scripts["queue/partitionLease"].Exec(
		redis_telemetry.WithScriptName(ctx, "partitionLease"),
		q.u.unshardedRc,
		keys,
		args,
		// TODO: Partition concurrency defer amount

	).AsInt64()
	if err != nil {
		return nil, fmt.Errorf("error leasing partition: %w", err)
	}
	switch result {
	case -1:
		return nil, ErrPartitionConcurrencyLimit
	case -2:
		return nil, ErrPartitionNotFound
	case -3:
		return nil, ErrPartitionAlreadyLeased
	case -4:
		return nil, ErrPartitionPaused
	default:
		// Update the partition's last indicator.
		if result > p.Last {
			p.Last = result
		}

		// If there's no concurrency limit for this partition, return a default
		// amount so that processing the partition has reasonable limits.
		if concurrency == 0 {
			return &leaseID, nil
		}

		// result is the available concurrency within this partition
		return &leaseID, nil
	}
}

// GlobalPartitionPeek returns up to PartitionSelectionMax partition items from the queue. This
// returns the indexes of partitions.
//
// If sequential is set to true this returns partitions in order from earliest to latest
// available lease times. Otherwise, this shuffles all partitions and picks partitions
// randomly, with higher priority partitions more likely to be selected.  This reduces
// lease contention amongst multiple shared-nothing workers.
func (q *queue) PartitionPeek(ctx context.Context, sequential bool, until time.Time, limit int64) ([]*QueuePartition, error) {
	return q.partitionPeek(ctx, q.u.kg.GlobalPartitionIndex(), sequential, until, limit)
}

func (q *queue) partitionSize(ctx context.Context, partitionKey string, until time.Time) (int64, error) {
	ctx = redis_telemetry.WithScope(redis_telemetry.WithOpName(ctx, "partitionSize"), redis_telemetry.ScopeQueue)

	cmd := q.u.Client().B().Zcount().Key(partitionKey).Min("-inf").Max(strconv.Itoa(int(until.Unix()))).Build()
	return q.u.Client().Do(ctx, cmd).AsInt64()
}

func (q *queue) partitionPeek(ctx context.Context, partitionKey string, sequential bool, until time.Time, limit int64) ([]*QueuePartition, error) {
	ctx = redis_telemetry.WithScope(redis_telemetry.WithOpName(ctx, "partitionPeek"), redis_telemetry.ScopeQueue)

	if limit > PartitionPeekMax {
		return nil, ErrPartitionPeekMaxExceedsLimits
	}
	if limit <= 0 {
		limit = PartitionPeekMax
	}

	// TODO(tony): If this is an allowlist, only peek the given partitions.  Use ZMSCORE
	// to fetch the scores for all allowed partitions, then filter where score <= until.
	// Call an HMGET to get the partitions.
	ms := until.UnixMilli()

	isSequential := 0
	if sequential {
		isSequential = 1
	}

	args, err := StrSlice([]any{
		ms,
		limit,
		isSequential,
	})
	if err != nil {
		return nil, err
	}

	encoded, err := scripts["queue/partitionPeek"].Exec(
		redis_telemetry.WithScriptName(ctx, "partitionPeek"),
		q.u.Client(),
		[]string{
			q.u.kg.GlobalPartitionIndex(),
			q.u.kg.PartitionItem(),
		},
		args,
	).AsStrSlice()
	if err != nil {
		return nil, fmt.Errorf("error peeking partition items: %w", err)
	}

	weights := []float64{}
	items := make([]*QueuePartition, len(encoded))

	ignored := 0
	for n, i := range encoded {
		if i == "" {
			ignored++
			continue
		}

		item := &QueuePartition{}
		if err = json.Unmarshal([]byte(i), item); err != nil {
			return nil, fmt.Errorf("error reading partition item: %w", err)
		}

		if item.Paused {
			ignored++
			continue
		}

		// NOTE: The queue does two conflicting things:  we peek ahead of now() to fetch partitions
		// shortly available, and we also requeue partitions if there are concurrency conflicts.
		//
		// We want to ignore any partitions requeued because of conflicts, as this will cause needless
		// churn every peek MS.
		if item.ForceAtMS > ms {
			ignored++
			continue
		}

		// If we have an allowlist, only accept this partition if its in the allowlist.
		if len(q.allowQueues) > 0 && !checkList(item.Queue(), q.allowQueueMap, q.allowQueuePrefixes) {
			// This is not in the allowlist specified, so do not allow this partition to be used.
			ignored++
			continue
		}

		// Ignore any denied queues if they're explicitly in the denylist.  Because
		// we allocate the len(encoded) amount, we also want to track the number of
		// ignored queues to use the correct index when setting our items;  this ensures
		// that we don't access items with an index and get nil pointers.
		if len(q.denyQueues) > 0 && checkList(item.Queue(), q.denyQueueMap, q.denyQueuePrefixes) {
			// This is in the denylist explicitly set, so continue
			ignored++
			continue
		}

		items[n-ignored] = item
		weights = append(weights, float64(10-item.Priority))
	}

	// Remove any ignored items from the slice.
	items = items[0 : len(items)-ignored]

	// Some scanners run sequentially, ensuring we always work on the functions with
	// the oldest run at times in order, no matter the priority.
	if sequential {
		n := int(math.Min(float64(len(items)), float64(PartitionSelectionMax)))
		return items[0:n], nil
	}

	// We want to weighted shuffle the resulting array random.  This means that many
	// shared nothing scanners can query for outstanding partitions and receive a
	// randomized order favouring higher-priority queue items.  This reduces the chances
	// of contention when leasing.
	w := sampleuv.NewWeighted(weights, rnd)
	result := make([]*QueuePartition, len(items))
	for n := range result {
		idx, ok := w.Take()
		if !ok {
			return nil, ErrWeightedSampleRead
		}
		result[n] = items[idx]
	}

	return result, nil
}

func checkList(check string, exact, prefixes map[string]*struct{}) bool {
	for k := range exact {
		if check == k {
			return true
		}
	}
	for k := range prefixes {
		if strings.HasPrefix(check, k) {
			return true
		}
	}
	return false
}

// PartitionRequeue requeues a parition with a new score, ensuring that the partition will be
// read at (or very close to) the given time.
//
// This is used after peeking and passing all queue items onto workers; we then take the next
// unleased available time for the queue item and requeue the partition.
//
// forceAt is used to enforce the given queue time.  This is used when partitions are at a
// concurrency limit;  we don't want to scan the partition next time, so we force the partition
// to be at a specific time instead of taking the earliest available queue item time
func (q *queue) PartitionRequeue(ctx context.Context, p *QueuePartition, at time.Time, forceAt bool) error {
	ctx = redis_telemetry.WithScope(redis_telemetry.WithOpName(ctx, "PartitionRequeue"), redis_telemetry.ScopeQueue)

	var shardName string
	if q.sf != nil {
		if shard := q.sf(ctx, p.Queue(), p.WorkspaceID); shard != nil {
			shardName = shard.Name
		}
	}

	keys := []string{
		q.u.kg.PartitionItem(),
		q.u.kg.GlobalPartitionIndex(),
		q.u.kg.ShardPartitionIndex(shardName),
		q.u.kg.PartitionMeta(p.Queue()),
		q.u.kg.QueueIndex(p.Queue()),
		q.u.kg.QueueItem(),
		q.u.kg.Concurrency("p", p.Queue()),
	}
	force := 0
	if forceAt {
		force = 1
	}
	args, err := StrSlice([]any{
		p.Queue(),
		at.UnixMilli(),
		force,
	})
	if err != nil {
		return err
	}
	status, err := scripts["queue/partitionRequeue"].Exec(
		redis_telemetry.WithScriptName(ctx, "partitionRequeue"),
		q.u.unshardedRc,
		keys,
		args,
	).AsInt64()
	if err != nil {
		return fmt.Errorf("error requeueing partition: %w", err)
	}
	switch status {
	case 0:
		return nil
	case 1:
		return ErrPartitionNotFound
	case 2:
		return ErrPartitionGarbageCollected
	default:
		return fmt.Errorf("unknown response requeueing item: %d", status)
	}
}

// PartitionDequeue removes a partition pointer from the queue.  This is used when peeking and
// receiving zero items to run.
func (q *queue) PartitionDequeue(ctx context.Context, queueName string, at time.Time) error {
	panic("unimplemented: requeueing partitions handles this.")
}

// PartitionReprioritize reprioritizes a workflow's QueueItems within the queue.
func (q *queue) PartitionReprioritize(ctx context.Context, queueName string, priority uint) error {
	ctx = redis_telemetry.WithScope(redis_telemetry.WithOpName(ctx, "PartitionReprioritize"), redis_telemetry.ScopeQueue)

	if priority > PriorityMin {
		return ErrPriorityTooLow
	}
	if priority < PriorityMax {
		return ErrPriorityTooHigh
	}

	args, err := StrSlice([]any{
		queueName,
		priority,
	})
	if err != nil {
		return err
	}

	keys := []string{q.u.kg.PartitionItem()}
	status, err := scripts["queue/partitionReprioritize"].Exec(
		redis_telemetry.WithScriptName(ctx, "partitionReprioritize"),
		q.u.unshardedRc,
		keys,
		args,
	).AsInt64()
	if err != nil {
		return fmt.Errorf("error enqueueing item: %w", err)
	}
	switch status {
	case 0:
		return nil
	case 1:
		return ErrPartitionNotFound
	default:
		return fmt.Errorf("unknown response reprioritizing partition: %d", status)
	}
}

func (q *queue) InProgress(ctx context.Context, prefix string, concurrencyKey string) (int64, error) {
	ctx = redis_telemetry.WithScope(redis_telemetry.WithOpName(ctx, "InProgress"), redis_telemetry.ScopeQueue)

	s := q.clock.Now().UnixMilli()
	cmd := q.u.unshardedRc.B().Zcount().
		Key(q.u.kg.Concurrency(prefix, concurrencyKey)).
		Min(fmt.Sprintf("%d", s)).
		Max("+inf").
		Build()
	return q.u.unshardedRc.Do(ctx, cmd).AsInt64()
}

func (q *queue) Instrument(ctx context.Context) error {
	// other queue instrumentation
	go func(ctx context.Context) {
		// Shard instrumentations
		shards, err := q.getShards(ctx)
		if err != nil {
			q.logger.Error().Err(err).Msg("error retrieving shards")
		}

		metrics.GaugeQueueShardCount(ctx, int64(len(shards)), metrics.GaugeOpt{PkgName: pkgName})
		for _, shard := range shards {
			tags := map[string]any{"shard_name": shard.Name}

			metrics.GaugeQueueShardGuaranteedCapacityCount(ctx, int64(shard.GuaranteedCapacity), metrics.GaugeOpt{
				PkgName: pkgName,
				Tags:    tags,
			})
			metrics.GaugeQueueShardLeaseCount(ctx, int64(len(shard.Leases)), metrics.GaugeOpt{
				PkgName: pkgName,
				Tags:    tags,
			})

			if size, err := q.partitionSize(ctx, q.u.kg.ShardPartitionIndex(shard.Name), q.clock.Now().Add(PartitionLookahead)); err == nil {
				metrics.GaugeQueueShardPartitionAvailableCount(ctx, size, metrics.GaugeOpt{
					PkgName: pkgName,
					Tags:    tags,
				})
			}
		}
	}(ctx)

	// Check on global partition and queue partition sizes
	var offset, total int64
	chunkSize := int64(1000)

	r := q.u.unshardedRc
	// iterate through all the partitions in the global partitions in chunks
	wg := sync.WaitGroup{}
	for {
		// grab the global partition by chunks
		cmd := r.B().Zrange().
			Key(q.u.kg.GlobalPartitionIndex()).
			Min("-inf").
			Max("+inf").
			Byscore().
			Limit(offset, chunkSize).
			Build()

		pkeys, err := r.Do(ctx, cmd).AsStrSlice()
		if err != nil {
			return fmt.Errorf("error retrieving partitions for instrumentation: %w", err)
		}

		for _, pk := range pkeys {
			wg.Add(1)

			// check each partition concurrently
			go func(ctx context.Context, pkey string) {
				defer wg.Done()

				cntCmd := r.B().Zcount().Key(q.u.kg.QueueIndex(pkey)).Min("-inf").Max("+inf").Build()
				count, err := q.u.unshardedRc.Do(ctx, cntCmd).AsInt64()
				if err != nil {
					q.logger.Warn().Err(err).Str("pkey", pkey).Str("context", "instrumentation").Msg("error checking partition count")
					return
				}

				metrics.GaugePartitionSize(ctx, count, metrics.GaugeOpt{
					PkgName: pkgName,
					Tags: map[string]any{
						// NOTE: potentially high cardinality but this gives better clarify of stuff
						"partition": pkey,
					},
				})

				atomic.AddInt64(&total, 1)
			}(ctx, pk)

		}
		// end of pagination, exit
		if len(pkeys) < int(chunkSize) {
			break
		}

		offset += chunkSize
	}

	// instrument the total count of global partition
	metrics.GaugeGlobalPartitionSize(ctx, atomic.LoadInt64(&total), metrics.GaugeOpt{
		PkgName: pkgName,
	})

	wg.Wait()

	return nil
}

// Scavenge attempts to find jobs that may have been lost due to killed workers.  Workers are shared
// nothing, and each item in a queue has a lease.  If a worker dies, it will not finish the job and
// cannot renew the item's lease.
//
// We scan all partition concurrency queues - queues of leases - to find leases that have expired.
func (q *queue) Scavenge(ctx context.Context) (int, error) {
	ctx = redis_telemetry.WithScope(redis_telemetry.WithOpName(ctx, "Scavenge"), redis_telemetry.ScopeQueue)

	// Find all items that have an expired lease - eg. where the min time for a lease is between
	// (0-now] in unix milliseconds.
	now := fmt.Sprintf("%d", q.clock.Now().UnixMilli())

	cmd := q.u.unshardedRc.B().Zrange().
		Key(q.u.kg.ConcurrencyIndex()).
		Min("-inf").
		Max(now).
		Byscore().
		Limit(0, 100).
		Build()

	pKeys, err := q.u.unshardedRc.Do(ctx, cmd).AsStrSlice()
	if err != nil {
		return 0, fmt.Errorf("error scavenging for lost items: %w", err)
	}

	counter := 0

	// Each of the items is a concurrency queue with lost items.
	var resultErr error
	for _, partition := range pKeys {
		// Fetch the partition.  This uses the concurrency:p: prefix,
		// so remove the prefix from the item.
		partitionJSON, err := q.u.unshardedRc.Do(ctx, q.u.unshardedRc.B().Hget().Key(q.u.kg.PartitionItem()).Field(partition).Build()).AsBytes()
		if err == rueidis.Nil {
			continue
		}
		if err != nil {
			resultErr = multierror.Append(resultErr, fmt.Errorf("error finding partition '%s' during scavenge: %w", partition, err))
			continue
		}

		cmd := q.u.unshardedRc.B().Zrange().
			Key(q.u.kg.Concurrency("p", partition)).
			Min("-inf").
			Max(now).
			Byscore().
			Limit(0, 100).
			Build()
		itemIDs, err := q.u.unshardedRc.Do(ctx, cmd).AsStrSlice()
		if err != nil && err != rueidis.Nil {
			resultErr = multierror.Append(resultErr, fmt.Errorf("error querying partition concurrency queue '%s' during scavenge: %w", partition, err))
			continue
		}
		if len(itemIDs) == 0 {
			continue
		}

		p := QueuePartition{}
		if err := json.Unmarshal([]byte(partitionJSON), &p); err != nil {
			resultErr = multierror.Append(resultErr, fmt.Errorf("error unmarshalling partition '%s': %w", partitionJSON, err))
			continue
		}

		// Fetch the queue item, then requeue.
		cmd = q.u.unshardedRc.B().Hmget().Key(q.u.kg.QueueItem()).Field(itemIDs...).Build()
		jobs, err := q.u.unshardedRc.Do(ctx, cmd).AsStrSlice()
		if err != nil && err != rueidis.Nil {
			resultErr = multierror.Append(resultErr, fmt.Errorf("error fetching jobs for concurrency queue '%s' during scavenge: %w", partition, err))
			continue
		}
		for _, item := range jobs {
			qi := QueueItem{}
			if err := json.Unmarshal([]byte(item), &qi); err != nil {
				resultErr = multierror.Append(resultErr, fmt.Errorf("error unmarshalling job '%s': %w", item, err))
				continue
			}
			if err := q.Requeue(ctx, p, qi, q.clock.Now()); err != nil {
				resultErr = multierror.Append(resultErr, fmt.Errorf("error requeueing job '%s': %w", item, err))
				continue
			}
			counter++
		}
	}

	return counter, resultErr
}

// ConfigLease allows a worker to lease config keys for sequential or scavenger processing.
// Leasing this key works similar to leasing partitions or queue items:
//
//   - If the key isn't leased, a new lease is accepted.
//   - If the lease is expired, a new lease is accepted.
//   - If the key is leased, you must pass in the existing lease ID to renew the lease.  Mismatches do not
//     grant a lease.
//
// This returns the new lease ID on success.
//
// If the sequential key is leased, this allows a worker to peek partitions sequentially.
func (q *queue) ConfigLease(ctx context.Context, key string, duration time.Duration, existingLeaseID ...*ulid.ULID) (*ulid.ULID, error) {
	ctx = redis_telemetry.WithScope(redis_telemetry.WithOpName(ctx, "ConfigLease"), redis_telemetry.ScopeQueue)

	if duration > ConfigLeaseMax {
		return nil, ErrConfigLeaseExceedsLimits
	}

	now := q.clock.Now()
	newLeaseID, err := ulid.New(ulid.Timestamp(now.Add(duration)), rnd)
	if err != nil {
		return nil, err
	}

	var existing string
	if len(existingLeaseID) > 0 && existingLeaseID[0] != nil {
		existing = existingLeaseID[0].String()
	}

	args, err := StrSlice([]any{
		now.UnixMilli(),
		newLeaseID.String(),
		existing,
	})
	if err != nil {
		return nil, err
	}

	status, err := scripts["queue/configLease"].Exec(
		redis_telemetry.WithScriptName(ctx, "configLease"),
		q.u.unshardedRc,
		[]string{key},
		args,
	).AsInt64()
	if err != nil {
		return nil, fmt.Errorf("error claiming config lease: %w", err)
	}
	switch status {
	case 0:
		return &newLeaseID, nil
	case 1:
		return nil, ErrConfigAlreadyLeased
	default:
		return nil, fmt.Errorf("unknown response claiming config lease: %d", status)
	}
}

func (q *queue) getShards(ctx context.Context) (map[string]*QueueShard, error) {
	ctx = redis_telemetry.WithScope(redis_telemetry.WithOpName(ctx, "getShards"), redis_telemetry.ScopeQueue)

	m, err := q.u.unshardedRc.Do(ctx, q.u.unshardedRc.B().Hgetall().Key(q.u.kg.Shards()).Build()).AsMap()
	if rueidis.IsRedisNil(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("error fetching shards: %w", err)
	}
	shards := map[string]*QueueShard{}
	for k, v := range m {
		shard := &QueueShard{}
		if err := v.DecodeJSON(shard); err != nil {
			return nil, fmt.Errorf("error decoding shards: %w", err)
		}
		shards[k] = shard
	}
	return shards, nil
}

// leaseShard leases a shard for the given duration.  Shards can have more than one lease at a time;
// you must provide an index to claim a lease. THe index This prevents multiple workers
// from claiming the same lease index;  if workers A and B see a shard with 0 leases and both attempt
// to claim lease "0", only one will succeed.
func (q *queue) leaseShard(ctx context.Context, shard *QueueShard, duration time.Duration, n int) (*ulid.ULID, error) {
	ctx = redis_telemetry.WithScope(redis_telemetry.WithOpName(ctx, "leaseShard"), redis_telemetry.ScopeQueue)

	now := q.clock.Now()
	leaseID, err := ulid.New(uint64(now.Add(duration).UnixMilli()), rand.Reader)
	if err != nil {
		return nil, err
	}

	keys := []string{q.u.kg.Shards()}
	args, err := StrSlice([]any{
		now.UnixMilli(),
		shard.Name,
		leaseID,
		n,
	})
	if err != nil {
		return nil, err
	}

	status, err := scripts["queue/shardLease"].Exec(
		redis_telemetry.WithScriptName(ctx, "shardLease"),
		q.u.unshardedRc,
		keys,
		args,
	).AsInt64()
	if err != nil {
		return nil, fmt.Errorf("error leasing item: %w", err)
	}
	switch status {
	case int64(-1):
		return nil, errShardNotFound
	case int64(-2):
		return nil, errShardIndexLeased
	case int64(-3):
		return nil, errShardIndexInvalid
	case int64(0):
		return &leaseID, nil
	default:
		return nil, fmt.Errorf("unknown lease return value: %T(%v)", status, status)
	}
}

func (q *queue) renewShardLease(ctx context.Context, shard *QueueShard, duration time.Duration, leaseID ulid.ULID) (*ulid.ULID, error) {
	ctx = redis_telemetry.WithScope(redis_telemetry.WithOpName(ctx, "RunJobs"), redis_telemetry.ScopeQueue)

	now := q.clock.Now()
	newLeaseID, err := ulid.New(uint64(now.Add(duration).UnixMilli()), rand.Reader)
	if err != nil {
		return nil, err
	}

	keys := []string{q.u.kg.Shards()}
	args, err := StrSlice([]any{
		now.UnixMilli(),
		shard.Name,
		leaseID,
		newLeaseID,
	})
	if err != nil {
		return nil, err
	}

	status, err := scripts["queue/renewShardLease"].Exec(
		redis_telemetry.WithScriptName(ctx, "renewShardLease"),
		q.u.unshardedRc,
		keys,
		args,
	).AsInt64()
	if err != nil {
		return nil, fmt.Errorf("error leasing item: %w", err)
	}
	switch status {
	case int64(-1):
		return nil, fmt.Errorf("shard not found")
	case int64(-2):
		return nil, fmt.Errorf("lease not found")
	case int64(0):
		return &newLeaseID, nil
	default:
		return nil, fmt.Errorf("unknown lease renew return value: %T(%v)", status, status)
	}
}

//nolint:all
func (q *queue) getShardLeases() []leasedShard {
	q.shardLeaseLock.Lock()
	existingLeases := make([]leasedShard, len(q.shardLeases))
	for n, i := range q.shardLeases {
		existingLeases[n] = i
	}
	q.shardLeaseLock.Unlock()
	return existingLeases
}

// peekEWMA returns the calculated EWMA value from the list
func (q *queue) peekEWMA(ctx context.Context, fnID uuid.UUID) (int64, error) {
	ctx = redis_telemetry.WithScope(redis_telemetry.WithOpName(ctx, "peekEWMA"), redis_telemetry.ScopeQueue)

	// retrieves the list from redis
	cmd := q.u.Client().B().Lrange().Key(q.u.KeyGenerator().ConcurrencyFnEWMA(fnID)).Start(0).Stop(-1).Build()
	strlist, err := q.u.Client().Do(ctx, cmd).AsStrSlice()
	if err != nil {
		return 0, fmt.Errorf("error reading function concurrency EWMA values: %w", err)
	}

	// return early
	if len(strlist) == 0 {
		return 0, nil
	}

	hasNonZero := false
	vals := make([]float64, len(strlist))
	for i, s := range strlist {
		v, _ := strconv.ParseFloat(s, 64)
		vals[i] = v
		if v > 0 {
			hasNonZero = true
		}
	}

	if !hasNonZero {
		// short-circuit.
		return 0, nil
	}

	// create a simple EWMA, add all the numbers in it and get the final value
	// NOTE: we don't need variable since we don't want to maintain this in memory
	mavg := ewma.NewMovingAverage()
	for _, v := range vals {
		mavg.Add(v)
	}

	// round up to the nearest integer
	return int64(math.Round(mavg.Value())), nil
}

// setPeekEWMA add the new value to the existing list.
// if the length of the list exceeds the predetermined size, pop out the first item
func (q *queue) setPeekEWMA(ctx context.Context, fnID uuid.UUID, val int64) error {
	ctx = redis_telemetry.WithScope(redis_telemetry.WithOpName(ctx, "setPeekEWMA"), redis_telemetry.ScopeQueue)

	listSize := q.peekEWMALen
	if listSize == 0 {
		listSize = QueuePeekEWMALen
	}

	keys := []string{
		q.u.KeyGenerator().ConcurrencyFnEWMA(fnID),
	}
	args, err := StrSlice([]any{
		val,
		listSize,
	})
	if err != nil {
		return err
	}

	_, err = scripts["queue/setPeekEWMA"].Exec(
		redis_telemetry.WithScriptName(ctx, "setPeekEWMA"),
		q.u.Client(),
		keys,
		args,
	).AsInt64()
	if err != nil {
		return fmt.Errorf("error updating function concurrency EWMA: %w", err)
	}

	return nil
}

func (q *queue) addContinue(p *QueuePartition, ctr uint) {
	if ctr >= q.continuationLimit {
		// This is over the limit for conntinuing the partition, so force it to be
		// removed in every case.
		q.continuesLock.Lock()
		delete(q.continues, p.Queue())
		q.continuesLock.Unlock()

		// TODO: Cooldown;  block this partition from having a continuation for the next
		// 5 seconds.

		return
	}

	q.continuesLock.Lock()
	c, ok := q.continues[p.Queue()]
	if !ok || c.count < ctr {
		// Update the continue count if it doesn't exist, or the current counter
		// is higher.  This ensures that we always have the highest continuation
		// count stored for queue processing.
		q.continues[p.Queue()] = continuation{partition: p, count: ctr}
	}
	q.continuesLock.Unlock()
}

func HashID(ctx context.Context, id string) string {
	ui := xxhash.Sum64String(id)
	return strconv.FormatUint(ui, 36)
}

// frandRNG is a fast crypto-secure prng which uses a mutex to guard
// parallel reads.  It also implements the x/exp/rand.Source interface
// by adding a Seed() method which does nothing.
type frandRNG struct {
	*frand.RNG
	lock *sync.Mutex
}

func (f *frandRNG) Read(b []byte) (int, error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.RNG.Read(b)
}

func (f *frandRNG) Uint64() uint64 {
	return f.Uint64n(math.MaxUint64)
}

func (f *frandRNG) Uint64n(n uint64) uint64 {
	// sampled.Take calls Uint64n, which must be guarded by a lock in order
	// to be thread-safe.
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.RNG.Uint64n(n)
}

func (f *frandRNG) Float64() float64 {
	// sampled.Take also calls Float64, which must be guarded by a lock in order
	// to be thread-safe.
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.RNG.Float64()
}

func (f *frandRNG) Seed(seed uint64) {
	// Do nothing.
}

func newLeaseDenyList() *leaseDenies {
	return &leaseDenies{
		lock:        &sync.RWMutex{},
		concurrency: map[string]struct{}{},
		throttle:    map[string]struct{}{},
	}
}

// leaseDenies stores a mapping of keys that must not be leased.
//
// When iterating over a list of peeked queue items, each queue item may have the same
// or different concurrency keys.  As soon as one of these concurrency keys reaches its
// limit, any next queue items with the same keys must _never_ be considered for leasing.
//
// This has two benefits:  we prevent wasted work, and we prevent out of order work.
type leaseDenies struct {
	lock *sync.RWMutex

	concurrency map[string]struct{}
	throttle    map[string]struct{}
}

func (l *leaseDenies) addThrottled(err error) {
	var key keyError
	if !errors.As(err, &key) {
		return
	}
	l.lock.Lock()
	l.throttle[key.key] = struct{}{}
	l.lock.Unlock()
}

func (l *leaseDenies) addConcurrency(err error) {
	var key keyError
	if !errors.As(err, &key) {
		return
	}
	l.lock.Lock()
	l.concurrency[key.key] = struct{}{}
	l.lock.Unlock()
}

func (l *leaseDenies) denyConcurrency(key string) bool {
	l.lock.RLock()
	_, ok := l.concurrency[key]
	l.lock.RUnlock()
	return ok
}

func (l *leaseDenies) denyThrottle(key string) bool {
	l.lock.RLock()
	_, ok := l.throttle[key]
	l.lock.RUnlock()
	return ok
}
