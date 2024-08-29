package consts

import "time"

const (
	// DefaultRetryCount is used when no retry count for a step is specified.
	// Given 4 retries, each step or function is attempted 5 times by default.
	DefaultRetryCount = 4

	// DefaultMaxEventSize represents the maximum size of the event payload we process,
	// currently 512KB.
	DefaultMaxEventSize = 512 * 1024

	// AbsoluteMaxEventSize is the absolute maximum size of the event payload we process.
	AbsoluteMaxEventSize = 3 * 1024 * 1024

	// DefaultMaxStepLimit is the maximum number of steps per function allowed.
	DefaultMaxStepLimit = 1_000

	// AbsoluteMaxStepLimit is the absolute maximium number of steps that an executor can
	// be initialized with.
	AbsoluteMaxStepLimit = 10_000

	// MaxFunctionTimeout represents the longest running function or step allowed within
	// our system.
	MaxFunctionTimeout = 2 * time.Hour

	// MaxBodySize is the maximum payload size read on any HTTP response.
	MaxBodySize = 1024 * 1024 * 4 // 4MB

	// DefaultMaxStateSizeLimit is the maximum number of bytes of output state per function run allowed.
	DefaultMaxStateSizeLimit = 1024 * 1024 * 32 // 32MB

	// MaxRetries represents the maximum number of retries for a particular function or step
	// possible.
	MaxRetries = 30

	// MaxRetryDuration is the furthest a retry can be scheduled.  If retries are scheduled further
	// than now plus this duration, the retry duration will automatically be lowered to this value.
	MaxRetryDuration = time.Hour * 24

	// MinRetryDuration is the soonest a retry can be scheduled.
	MinRetryDuration = time.Second * 1

	// MinDebouncePeriod is the minimum period of time that can be used to configure a debounce.
	MinDebouncePeriod = time.Second

	// MaxDebouncePeriod is the maximum period of time that can be used to configure a debounce.  This
	// lets users delay functions for up to MaxDebouncePeriod when events are received.
	MaxDebouncePeriod = time.Hour * 24 * 7

	// MaxCancellations represents the max automatic cancellation signals per function
	MaxCancellations = 5

	// MaxConcurrencyLimits limits the max concurrency constraints for a specific function.
	MaxConcurrencyLimits = 2

	// MaxTriggers represents the maximum number of triggers a function can have.
	MaxTriggers = 10

	// MaxBatchTTL represents the maximum amount of duration the batch key will last
	MaxBatchTTL = 10 * time.Minute

	// DefaultConcurrencyLimit is the default concurrency limit applied when not specified
	DefaultConcurrencyLimit = 1_000

	// FunctionIdempotencyPeriod determines how long a specific function remains idempotent
	// when using idempotency keys.
	FunctionIdempotencyPeriod = 24 * time.Hour

	DefaultBatchSizeLimit = 100
	DefaultBatchTimeout   = 60 * time.Second

	// MaxEvents is the maximum number of events we can parse in a single batch.
	MaxEvents = 5_000

	InngestEventDataPrefix = "_inngest"
	// InvokeSlugKey is the data key used to store the fn name when invoking a function
	// via an RPC-like call, abstracting event-driven fanout.
	InvokeFnID          = "fn_id"
	InvokeCorrelationId = "correlation_id"

	// CancelTimeout is the maximum time a cancellation can exist
	CancelTimeout = time.Hour * 24 * 365

	// SourceEdgeRetries represents the number of times we'll retry running a source edge.
	// Each edge gets their own set of retries in our execution engine, embedded directly
	// in the job.  The retry count is taken from function config for every step _but_
	// initialization.
	SourceEdgeRetries = 20

	RequestVersionUnknown = -1

	// PriorityFactorMin is the minimum priority factor for any function run, in seconds.
	PriorityFactorMin = int64(-1 * 60 * 60 * 12)
	// PriorityFactorMax is the maximum priority factor for any function run, in seconds.
	// This is set to 12 hours.
	PriorityFactorMax = int64(60 * 60 * 12)
	// FutureQueeueFudgeLimit is the inclusive time range between [now, now() + FutureAtLimit]
	// in which priority factors are taken into account.
	FutureAtLimit = 2 * time.Second

	DefaultQueueContinueLimit = uint(5)

	DevServerTempDir     = ".inngest"
	DevServerDbFile      = "dev_db.db"
	DevServerRdbFile     = "dev_state.rdb"
	DevServerHistoryFile = "dev_history.json"

	PauseExpiredDeletionGracePeriod = time.Second * 10
)
