package lite

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/inngest/inngest/pkg/enums"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/coocood/freecache"
	"github.com/eko/gocache/lib/v4/cache"
	freecachestore "github.com/eko/gocache/store/freecache/v4"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"github.com/inngest/inngest/pkg/api"
	"github.com/inngest/inngest/pkg/api/apiv1"
	"github.com/inngest/inngest/pkg/config"
	_ "github.com/inngest/inngest/pkg/config/defaults"
	"github.com/inngest/inngest/pkg/config/registration"
	"github.com/inngest/inngest/pkg/consts"
	"github.com/inngest/inngest/pkg/coreapi"
	"github.com/inngest/inngest/pkg/cqrs/sqlitecqrs"
	"github.com/inngest/inngest/pkg/deploy"
	"github.com/inngest/inngest/pkg/devserver"
	"github.com/inngest/inngest/pkg/event"
	"github.com/inngest/inngest/pkg/execution"
	"github.com/inngest/inngest/pkg/execution/batch"
	"github.com/inngest/inngest/pkg/execution/debounce"
	"github.com/inngest/inngest/pkg/execution/driver"
	"github.com/inngest/inngest/pkg/execution/driver/httpdriver"
	"github.com/inngest/inngest/pkg/execution/executor"
	"github.com/inngest/inngest/pkg/execution/history"
	"github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/ratelimit"
	"github.com/inngest/inngest/pkg/execution/runner"
	"github.com/inngest/inngest/pkg/execution/state"
	"github.com/inngest/inngest/pkg/execution/state/redis_state"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
	"github.com/inngest/inngest/pkg/expressions"
	"github.com/inngest/inngest/pkg/headers"
	"github.com/inngest/inngest/pkg/keys"
	"github.com/inngest/inngest/pkg/logger"
	"github.com/inngest/inngest/pkg/pubsub"
	"github.com/inngest/inngest/pkg/run"
	"github.com/inngest/inngest/pkg/service"
	itrace "github.com/inngest/inngest/pkg/telemetry/trace"
	"github.com/inngest/inngest/pkg/util/awsgateway"
	"github.com/redis/rueidis"
	"go.opentelemetry.io/otel/propagation"
	"golang.org/x/sync/errgroup"
)

var redisSingleton *miniredis.Miniredis

// StartOpts configures the dev server
type StartOpts struct {
	Config        config.Config `json:"-"`
	RootDir       string        `json:"dir"`
	RedisURI      string        `json:"redis-uri"`
	PollInterval  int           `json:"poll-interval"`
	URLs          []string      `json:"urls"`
	Tick          time.Duration `json:"tick"`
	RetryInterval int           `json:"retry_interval"`

	// SigningKey is used to decide that the server should sign requests and
	// validate responses where applicable, modelling cloud behaviour.
	SigningKey *keys.SigningKey `json:"signing_key"`
	SQLiteDir  string           `json:"sqlite-dir"`

	// EventKey is used to authorize incoming events, ensuring they match the
	// given key.
	EventKey []string `json:"event_key"`
}

// Create and start a new dev server.  The dev server is used during (surprise surprise)
// development.
//
// It runs all available services from `inngest serve`, plus:
// - Adds development-specific APIs for communicating with the SDK.
func New(ctx context.Context, opts StartOpts) error {
	// The dev server _always_ logs output for development.
	if !opts.Config.Execution.LogOutput {
		opts.Config.Execution.LogOutput = true
	}

	// Always set Cloud.
	opts.Config.ServerKind = headers.ServerKindCloud

	// NOTE: looks deprecated?
	// Before running the development service, ensure that we change the http
	// driver in development to use our AWS Gateway http client, attempting to
	// automatically transform dev requests to lambda invocations.
	httpdriver.DefaultExecutor.Client.Transport = awsgateway.NewTransformTripper(httpdriver.DefaultExecutor.Client.Transport)
	deploy.Client.Transport = awsgateway.NewTransformTripper(deploy.Client.Transport)

	return start(ctx, opts)
}

func start(ctx context.Context, opts StartOpts) error {
	db, err := sqlitecqrs.New(sqlitecqrs.SqliteCQRSOptions{
		InMemory:  false,
		Directory: opts.SQLiteDir,
	})
	if err != nil {
		return err
	}

	tick := opts.Tick
	if tick < 1 {
		tick = devserver.DefaultTickDuration
	}

	// Initialize the devserver
	dbcqrs := sqlitecqrs.NewCQRS(db)
	hd := sqlitecqrs.NewHistoryDriver(db)
	hr := sqlitecqrs.NewHistoryReader(db)
	loader := dbcqrs.(state.FunctionLoader)

	stepLimitOverrides := make(map[string]int)
	stateSizeLimitOverrides := make(map[string]int)

	shardedRc, err := connectToOrCreateRedis(opts.RedisURI)
	if err != nil {
		return err
	}

	unshardedRc, err := connectToOrCreateRedis(opts.RedisURI)
	if err != nil {
		return err
	}

	unshardedClient := redis_state.NewUnshardedClient(unshardedRc, redis_state.StateDefaultKey, redis_state.QueueDefaultKey)
	shardedClient := redis_state.NewShardedClient(redis_state.ShardedClientOpts{
		UnshardedClient:        unshardedClient,
		FunctionRunStateClient: shardedRc,
		StateDefaultKey:        redis_state.StateDefaultKey,
		FnRunIsSharded:         redis_state.AlwaysShardOnRun,
		BatchClient:            shardedRc,
		QueueDefaultKey:        redis_state.QueueDefaultKey,
	})

	var sm state.Manager
	t := runner.NewTracker()
	sm, err = redis_state.New(
		ctx,
		redis_state.WithShardedClient(shardedClient),
		redis_state.WithUnshardedClient(unshardedClient),
	)
	if err != nil {
		return err
	}
	smv2 := redis_state.MustRunServiceV2(sm)

	queueShard := redis_state.QueueShard{Name: consts.DefaultQueueShardName, RedisClient: unshardedClient.Queue(), Kind: string(enums.QueueShardKindRedis)}

	shardSelector := func(ctx context.Context, _ uuid.UUID, _ *string) (redis_state.QueueShard, error) {
		return queueShard, nil
	}

	queueShards := map[string]redis_state.QueueShard{
		consts.DefaultQueueShardName: queueShard,
	}

	queueOpts := []redis_state.QueueOpt{
		redis_state.WithIdempotencyTTL(time.Hour),
		redis_state.WithNumWorkers(100),
		redis_state.WithPollTick(tick),
		redis_state.WithCustomConcurrencyKeyLimitRefresher(func(ctx context.Context, i queue.QueueItem) []state.CustomConcurrency {
			keys := i.Data.GetConcurrencyKeys()

			fn, err := dbcqrs.GetFunctionByInternalUUID(ctx, i.Data.Identifier.WorkspaceID, i.Data.Identifier.WorkflowID)
			if err != nil {
				// Use what's stored in the state store.
				return keys
			}
			f, err := fn.InngestFunction()
			if err != nil {
				return keys
			}

			if f.Concurrency != nil {
				for _, c := range f.Concurrency.Limits {
					if !c.IsCustomLimit() {
						continue
					}
					// If there's a concurrency key with the same hash, use the new function's
					// concurrency limits.
					//
					// NOTE:  This is accidentally quadratic but is okay as we bound concurrency
					// keys to a low value (2-3).
					for n, actual := range keys {
						if actual.Hash != "" && actual.Hash == c.Hash {
							actual.Limit = c.Limit
							keys[n] = actual
						}
					}
				}
			}

			return keys
		}),
		redis_state.WithConcurrencyLimitGetter(
			func(ctx context.Context, p redis_state.QueuePartition) redis_state.PartitionConcurrencyLimits {
				limits := redis_state.PartitionConcurrencyLimits{
					AccountLimit:   redis_state.NoConcurrencyLimit,
					FunctionLimit:  consts.DefaultConcurrencyLimit,
					CustomKeyLimit: consts.DefaultConcurrencyLimit,
				}

				// Ensure that we return the correct concurrency values per partition.
				funcs, err := dbcqrs.GetFunctions(ctx)
				if err != nil {
					return limits
				}
				for _, fn := range funcs {
					f, _ := fn.InngestFunction()
					if f.ID == uuid.Nil {
						f.ID = f.DeterministicUUID()
					}
					if p.FunctionID != nil && f.ID == *p.FunctionID && f.Concurrency != nil && f.Concurrency.PartitionConcurrency() > 0 {
						limits.FunctionLimit = f.Concurrency.PartitionConcurrency()
						return limits
					}
				}

				return limits
			}),
		redis_state.WithShardSelector(shardSelector),
		redis_state.WithQueueShardClients(queueShards),
	}

	rq := redis_state.NewQueue(queueShard, queueOpts...)

	rl := ratelimit.New(ctx, unshardedRc, "{ratelimit}:")

	batcher := batch.NewRedisBatchManager(shardedClient.Batch(), rq)
	debouncer := debounce.NewRedisDebouncer(unshardedClient.Debounce(), queueShard, rq)

	// Create a new expression aggregator, using Redis to load evaluables.
	agg := expressions.NewAggregator(ctx, 100, 100, sm.(expressions.EvaluableLoader), nil)

	var drivers = []driver.Driver{}
	for _, driverConfig := range opts.Config.Execution.Drivers {
		d, err := driverConfig.NewDriver(registration.NewDriverOpts{
			RequireLocalSigningKey: true,
			LocalSigningKey:        opts.SigningKey,
		})
		if err != nil {
			return err
		}
		drivers = append(drivers, d)
	}
	pb, err := pubsub.NewPublisher(ctx, opts.Config.EventStream.Service)
	if err != nil {
		return fmt.Errorf("failed to create publisher: %w", err)
	}

	exec, err := executor.NewExecutor(
		executor.WithStateManager(smv2),
		executor.WithPauseManager(sm),
		executor.WithRuntimeDrivers(
			drivers...,
		),
		executor.WithExpressionAggregator(agg),
		executor.WithQueue(rq),
		executor.WithLogger(logger.From(ctx)),
		executor.WithFunctionLoader(loader),
		executor.WithLifecycleListeners(
			history.NewLifecycleListener(
				nil,
				hd,
			),
			devserver.Lifecycle{
				Cqrs:       dbcqrs,
				Pb:         pb,
				EventTopic: opts.Config.EventStream.Service.Concrete.TopicName(),
			},
			run.NewTraceLifecycleListener(nil),
		),
		executor.WithStepLimits(func(id sv2.ID) int {
			if override, hasOverride := stepLimitOverrides[id.FunctionID.String()]; hasOverride {
				logger.From(ctx).Warn().Msgf("Using step limit override of %d for %q\n", override, id.FunctionID)
				return override
			}

			return consts.DefaultMaxStepLimit
		}),
		executor.WithStateSizeLimits(func(id sv2.ID) int {
			if override, hasOverride := stateSizeLimitOverrides[id.FunctionID.String()]; hasOverride {
				logger.From(ctx).Warn().Msgf("Using state size limit override of %d for %q\n", override, id.FunctionID)
				return override
			}

			return consts.DefaultMaxStateSizeLimit
		}),
		executor.WithInvokeFailHandler(getInvokeFailHandler(ctx, pb, opts.Config.EventStream.Service.Concrete.TopicName())),
		executor.WithSendingEventHandler(getSendingEventHandler(pb, opts.Config.EventStream.Service.Concrete.TopicName())),
		executor.WithDebouncer(debouncer),
		executor.WithBatcher(batcher),
		executor.WithAssignedQueueShard(queueShard),
		executor.WithShardSelector(shardSelector),
	)
	if err != nil {
		return err
	}

	// Create an executor.
	executorSvc := executor.NewService(
		opts.Config,
		executor.WithExecutionManager(dbcqrs),
		executor.WithState(sm),
		executor.WithServiceQueue(rq),
		executor.WithServiceExecutor(exec),
		executor.WithServiceBatcher(batcher),
		executor.WithServiceDebouncer(debouncer),
	)

	runner := runner.NewService(
		opts.Config,
		runner.WithCQRS(dbcqrs),
		runner.WithExecutor(exec),
		runner.WithExecutionManager(dbcqrs),
		runner.WithEventManager(event.NewManager()),
		runner.WithStateManager(sm),
		runner.WithRunnerQueue(rq),
		runner.WithTracker(t),
		runner.WithRateLimiter(rl),
		runner.WithBatchManager(batcher),
		runner.WithPublisher(pb),
	)

	// The devserver embeds the event API.
	pi := consts.StartDefaultPersistenceInterval
	persistenceInterval := &pi
	if opts.RedisURI != "" {
		// If we're using an external Redis, we rely on that to persist and
		// manage snapshotting
		persistenceInterval = nil

		logger.From(ctx).Info().Msgf("using external Redis %s; disabling in-memory persistence and snapshotting", opts.RedisURI)
	}

	dsOpts := devserver.StartOpts{
		Config:      opts.Config,
		RootDir:     opts.RootDir,
		URLs:        opts.URLs,
		Tick:        tick,
		SigningKey:  opts.SigningKey,
		EventKeys:   opts.EventKey,
		RequireKeys: true,
	}

	if opts.PollInterval > 0 {
		dsOpts.Poll = true
		dsOpts.PollInterval = opts.PollInterval
	}

	// The devserver embeds the event API.
	ds := devserver.NewService(dsOpts, runner, dbcqrs, pb, stepLimitOverrides, stateSizeLimitOverrides, unshardedRc, hd, &devserver.SingleNodeServiceOpts{
		PersistenceInterval: persistenceInterval,
	})
	// embed the tracker
	ds.Tracker = t
	ds.State = sm
	ds.Queue = rq
	ds.Executor = exec
	// start the API
	// Create a new API endpoint which hosts SDK-related functionality for
	// registering functions.
	devAPI := devserver.NewDevAPI(ds)

	devAPI.Route("/v1", func(r chi.Router) {
		// Add the V1 API to our dev server API.
		cache := cache.New[[]byte](freecachestore.NewFreecache(freecache.NewCache(1024 * 1024)))
		caching := apiv1.NewCacheMiddleware(cache)

		apiv1.AddRoutes(r, apiv1.Opts{
			CachingMiddleware: caching,
			EventReader:       ds.Data,
			FunctionReader:    ds.Data,
			FunctionRunReader: ds.Data,
			JobQueueReader:    ds.Queue.(queue.JobQueueReader),
			Executor:          ds.Executor,
		})
	})

	core, err := coreapi.NewCoreApi(coreapi.Options{
		Data:            ds.Data,
		Config:          ds.Opts.Config,
		Logger:          logger.From(ctx),
		Runner:          ds.Runner,
		Tracker:         ds.Tracker,
		State:           ds.State,
		Queue:           ds.Queue,
		EventHandler:    ds.HandleEvent,
		Executor:        ds.Executor,
		HistoryReader:   hr,
		LocalSigningKey: opts.SigningKey,
		RequireKeys:     true,
	})
	if err != nil {
		return err
	}

	// Create a new data API directly in the devserver.  This allows us to inject
	// the data API into the dev server port, providing a single router for the dev
	// server UI, events, and API for loading data.
	//
	// Merge the dev server API (for handling files & registration) with the data
	// API into the event API router.
	ds.Apiservice = api.NewService(api.APIServiceOptions{
		Config: ds.Opts.Config,
		Mounts: []api.Mount{
			{At: "/", Router: devAPI},
			{At: "/v0", Router: core.Router},
			{At: "/debug", Handler: middleware.Profiler()},
		},
		LocalEventKeys: opts.EventKey,
		RequireKeys:    true,
	})

	return service.StartAll(ctx, ds, runner, executorSvc, ds.Apiservice)
}

func connectToOrCreateRedis(redisURI string) (rueidis.Client, error) {
	if redisURI == "" {
		return createInmemoryRedisConnection()
	}

	url := redisURI
	// strip the redis:// prefix if we have one; connection fails with it
	if len(url) > 8 && url[:8] == "redis://" {
		url = url[8:]
	}

	rc, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:       []string{url},
		DisableCache:      true,
		BlockingPoolSize:  1,
		ForceSingleClient: true,
	})
	if err != nil {
		return nil, fmt.Errorf("error creating redis client: %w", err)
	}

	return rc, nil
}

// createInMemoryRedisConnection creates a new connection to the in-memory Redis
// server. If the server is not yet running, it will start one.
func createInmemoryRedisConnection() (rueidis.Client, error) {
	if redisSingleton == nil {
		redisSingleton = miniredis.NewMiniRedis()
		err := redisSingleton.Start()
		if err != nil {
			return nil, fmt.Errorf("error starting in-memory redis: %w", err)
		}

		poll := time.Second
		go func() {
			for range time.Tick(poll) {
				redisSingleton.FastForward(poll)
			}
		}()
	}

	rc, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress:       []string{redisSingleton.Addr()},
		DisableCache:      true,
		BlockingPoolSize:  1,
		ForceSingleClient: true,
	})
	if err != nil {
		return nil, fmt.Errorf("error creating in-memory redis client: %w", err)
	}

	return rc, nil
}

func getSendingEventHandler(pb pubsub.Publisher, topic string) execution.HandleSendingEvent {
	return func(ctx context.Context, evt event.Event, item queue.Item) error {
		trackedEvent := event.NewOSSTrackedEvent(evt)
		byt, err := json.Marshal(trackedEvent)
		if err != nil {
			return fmt.Errorf("error marshalling invocation event: %w", err)
		}

		carrier := itrace.NewTraceCarrier()
		itrace.UserTracer().Propagator().Inject(ctx, propagation.MapCarrier(carrier.Context))

		err = pb.Publish(
			ctx,
			topic,
			pubsub.Message{
				Name:      event.EventReceivedName,
				Data:      string(byt),
				Timestamp: time.Now(),
				Metadata: map[string]any{
					consts.OtelPropagationKey: carrier,
				},
			},
		)
		if err != nil {
			return fmt.Errorf("error publishing invocation event: %w", err)
		}

		return nil
	}
}

func getInvokeFailHandler(ctx context.Context, pb pubsub.Publisher, topic string) execution.InvokeFailHandler {
	return func(ctx context.Context, opts execution.InvokeFailHandlerOpts, evts []event.Event) error {
		eg := errgroup.Group{}

		for _, e := range evts {
			evt := e
			eg.Go(func() error {
				trackedEvent := event.NewOSSTrackedEvent(evt)
				byt, err := json.Marshal(trackedEvent)
				if err != nil {
					return fmt.Errorf("error marshalling function finished event: %w", err)
				}

				err = pb.Publish(
					ctx,
					topic,
					pubsub.Message{
						Name:      event.EventReceivedName,
						Data:      string(byt),
						Timestamp: trackedEvent.GetEvent().Time(),
					},
				)
				if err != nil {
					return fmt.Errorf("error publishing function finished event: %w", err)
				}

				return nil
			})
		}

		return eg.Wait()
	}
}
