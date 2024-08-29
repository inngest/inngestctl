package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/inngest/inngest/pkg/config"
	"github.com/inngest/inngest/pkg/consts"
	"github.com/inngest/inngest/pkg/cqrs"
	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/event"
	"github.com/inngest/inngest/pkg/execution"
	"github.com/inngest/inngest/pkg/execution/batch"
	"github.com/inngest/inngest/pkg/execution/debounce"
	"github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/state"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
	"github.com/inngest/inngest/pkg/inngest"
	"github.com/inngest/inngest/pkg/logger"
	"github.com/inngest/inngest/pkg/pubsub"
	"github.com/inngest/inngest/pkg/run"
	"github.com/inngest/inngest/pkg/service"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"
)

type Opt func(s *svc)

func WithExecutionManager(l cqrs.Manager) func(s *svc) {
	return func(s *svc) {
		s.data = l
	}
}

func WithState(sm state.Manager) func(s *svc) {
	return func(s *svc) {
		s.state = sm
	}
}

func WithServiceExecutor(exec execution.Executor) func(s *svc) {
	return func(s *svc) {
		s.exec = exec
	}
}

func WithExecutorOpts(opts ...ExecutorOpt) func(s *svc) {
	return func(s *svc) {
		s.opts = opts
	}
}

func WithServiceQueue(q queue.Queue) func(s *svc) {
	return func(s *svc) {
		s.queue = q
	}
}

func WithServiceDebouncer(d debounce.Debouncer) func(s *svc) {
	return func(s *svc) {
		s.debouncer = d
	}
}

func WithServiceBatcher(b batch.BatchManager) func(s *svc) {
	return func(s *svc) {
		s.batcher = b
	}
}

func NewService(c config.Config, opts ...Opt) service.Service {
	svc := &svc{config: c}
	for _, o := range opts {
		o(svc)
	}
	return svc
}

type svc struct {
	config config.Config
	// data provides an interface for data access
	data cqrs.Manager
	// state allows us to record step results
	state state.Manager
	// queue allows us to enqueue next steps.
	queue queue.Queue
	// exec runs the specific actions.
	exec      execution.Executor
	debouncer debounce.Debouncer
	batcher   batch.BatchManager

	wg sync.WaitGroup

	opts []ExecutorOpt
}

func (s *svc) Name() string {
	return "executor"
}

func (s *svc) Pre(ctx context.Context) error {
	var err error

	if s.state == nil {
		return fmt.Errorf("no state provided")
	}

	if s.queue == nil {
		return fmt.Errorf("no queue provided")
	}

	finishHandler, err := s.getFinishHandler(ctx)
	if err != nil {
		return fmt.Errorf("failed to create finish handler: %w", err)
	}
	s.exec.SetFinalizer(finishHandler)

	return nil
}

func (s *svc) Executor() execution.Executor {
	return s.exec
}

func (s *svc) getFinishHandler(ctx context.Context) (func(context.Context, sv2.ID, []event.Event) error, error) {
	pb, err := pubsub.NewPublisher(ctx, s.config.EventStream.Service)
	if err != nil {
		return nil, fmt.Errorf("failed to create publisher: %w", err)
	}

	topicName := s.config.EventStream.Service.Concrete.TopicName()

	return func(ctx context.Context, id sv2.ID, events []event.Event) error {
		eg := errgroup.Group{}

		for _, e := range events {
			evt := e
			eg.Go(func() error {
				trackedEvent := event.NewOSSTrackedEvent(evt)
				byt, err := json.Marshal(trackedEvent)
				if err != nil {
					return fmt.Errorf("error marshalling event: %w", err)
				}

				err = pb.Publish(
					ctx,
					topicName,
					pubsub.Message{
						Name:      event.EventReceivedName,
						Data:      string(byt),
						Timestamp: trackedEvent.GetEvent().Time(),
					},
				)
				if err != nil {
					return fmt.Errorf("error publishing event: %w", err)
				}
				return nil
			})
		}

		return eg.Wait()
	}, nil
}

func (s *svc) Run(ctx context.Context) error {
	logger.From(ctx).Info().Msg("subscribing to function queue")
	return s.queue.Run(ctx, func(ctx context.Context, info queue.RunInfo, item queue.Item) (bool, error) {
		// Don't stop the service on errors.
		s.wg.Add(1)
		defer s.wg.Done()

		item.RunInfo = &info

		var (
			err          error
			continuation bool
		)

		switch item.Kind {
		case queue.KindStart, queue.KindEdge, queue.KindSleep, queue.KindEdgeError:
			continuation, err = s.handleQueueItem(ctx, item)
		case queue.KindPause:
			err = s.handlePauseTimeout(ctx, item)
		case queue.KindDebounce:
			err = s.handleDebounce(ctx, item)
		case queue.KindScheduleBatch:
			err = s.handleScheduledBatch(ctx, item)
		default:
			err = fmt.Errorf("unknown payload type: %T", item.Payload)
		}

		if err != nil {
			logger.StdlibLogger(ctx).Error("error handling queue item", "error", err)
		}

		return continuation, err
	})
}

func (s *svc) Stop(ctx context.Context) error {
	s.exec.CloseLifecycleListeners(ctx)

	// Wait for all in-flight queue runs to finish
	s.wg.Wait()
	return nil
}

func (s *svc) handleQueueItem(ctx context.Context, item queue.Item) (bool, error) {
	payload, err := queue.GetEdge(item)
	if err != nil {
		return false, fmt.Errorf("unable to get edge from queue item: %w", err)
	}
	edge := payload.Edge

	resp, err := s.exec.Execute(ctx, item.Identifier, item, edge)
	// Check if the execution is cancelled, and if so finalize and terminate early.
	// This prevents steps from scheduling children.
	if err == state.ErrFunctionCancelled {
		return false, nil
	}

	if errors.Is(err, ErrHandledStepError) {
		// Retry any next steps.
		return false, err
	}

	if err != nil || (resp != nil && resp.Err != nil) {
		// Accordingly, we check if the driver's response is retryable here;
		// this will let us know whether we can re-enqueue.
		if resp != nil && !resp.Retryable() {
			return false, nil
		}

		// If the error is not of type response error, we assume the step is
		// always retryable.
		if resp == nil || err != nil {
			return false, err
		}

		// Always retry; non-retryable is covered above.
		return false, fmt.Errorf("%s", resp.Error())
	}

	if resp != nil && len(resp.Generator) > 0 {
		return true, nil
	}

	return false, nil
}

func (s *svc) handlePauseTimeout(ctx context.Context, item queue.Item) error {
	l := logger.From(ctx).With().Str("run_id", item.Identifier.RunID.String()).Logger()

	pauseTimeout, ok := item.Payload.(queue.PayloadPauseTimeout)
	if !ok {
		return fmt.Errorf("unable to get pause timeout from queue item: %T", item.Payload)
	}

	pause, err := s.state.PauseByID(ctx, pauseTimeout.PauseID)
	if err == state.ErrPauseNotFound {
		// This pause has been consumed.
		l.Debug().Interface("pause", pauseTimeout).Msg("consumed pause timeout ignored")
		return nil
	}
	if err != nil {
		return err
	}
	if pause == nil {
		return nil
	}

	r := execution.ResumeRequest{}

	// If the pause timeout is for an invocation, store an error to cause the
	// step to fail.
	if pause.Opcode != nil && *pause.Opcode == enums.OpcodeInvokeFunction.String() {
		r.SetInvokeTimeoutError()
	}

	return s.exec.Resume(ctx, *pause, r)
}

// handleScheduledBatch checks for
func (s *svc) handleScheduledBatch(ctx context.Context, item queue.Item) error {
	opts := batch.ScheduleBatchOpts{}
	if err := json.Unmarshal(item.Payload.(json.RawMessage), &opts); err != nil {
		return err
	}

	batchID := opts.BatchID

	status, err := s.batcher.StartExecution(ctx, opts.FunctionID, batchID, opts.BatchPointer)
	if err != nil {
		return err
	}
	if status == enums.BatchStatusStarted.String() {
		// batch already started, abort
		return nil
	}
	if status == enums.BatchStatusAbsent.String() {
		// just attempt clean up, don't care about the result
		_ = s.batcher.ExpireKeys(ctx, opts.FunctionID, batchID)
		return nil
	}

	fn, err := s.findFunctionByID(ctx, opts.FunctionID)
	if err != nil {
		return err
	}

	if err := s.exec.RetrieveAndScheduleBatch(ctx, *fn, batch.ScheduleBatchPayload{
		BatchID:         batchID,
		BatchPointer:    opts.BatchPointer,
		AccountID:       item.Identifier.AccountID,
		WorkspaceID:     item.Identifier.WorkspaceID,
		AppID:           item.Identifier.AppID,
		FunctionID:      item.Identifier.WorkflowID,
		FunctionVersion: fn.FunctionVersion,
	}, nil); err != nil {
		return fmt.Errorf("could not retrieve and schedule batch items: %w", err)
	}

	return nil
}

func (s *svc) handleDebounce(ctx context.Context, item queue.Item) error {
	d := debounce.DebouncePayload{}
	if err := json.Unmarshal(item.Payload.(json.RawMessage), &d); err != nil {
		return fmt.Errorf("error unmarshalling debounce payload: %w", err)
	}

	all, err := s.data.Functions(ctx)
	if err != nil {
		return err
	}

	for _, f := range all {
		if f.ID == d.FunctionID {
			di, err := s.debouncer.GetDebounceItem(ctx, d.DebounceID)
			if err != nil {
				return err
			}

			ctx, span := run.NewSpan(ctx,
				run.WithScope(consts.OtelScopeDebounce),
				run.WithName(consts.OtelSpanDebounce),
				run.WithSpanAttributes(
					attribute.String(consts.OtelSysAccountID, item.Identifier.AccountID.String()),
					attribute.String(consts.OtelSysWorkspaceID, item.Identifier.WorkspaceID.String()),
					attribute.String(consts.OtelSysAppID, item.Identifier.AppID.String()),
					attribute.String(consts.OtelSysFunctionID, item.Identifier.WorkflowID.String()),
					attribute.Bool(consts.OtelSysDebounceTimeout, true),
				),
			)
			defer span.End()

			_, err = s.exec.Schedule(ctx, execution.ScheduleRequest{
				Function:         f,
				AccountID:        di.AccountID,
				WorkspaceID:      di.WorkspaceID,
				AppID:            di.AppID,
				Events:           []event.TrackedEvent{di},
				PreventDebounce:  true,
				FunctionPausedAt: di.FunctionPausedAt,
			})
			if err != nil {
				return err
			}
			_ = s.debouncer.DeleteDebounceItem(ctx, d.DebounceID)
		}
	}

	return nil
}

func (s *svc) findFunctionByID(ctx context.Context, fnID uuid.UUID) (*inngest.Function, error) {
	fns, err := s.data.Functions(ctx)
	if err != nil {
		return nil, err
	}
	for _, f := range fns {
		if f.ID == fnID {
			return &f, nil
		}
	}
	return nil, fmt.Errorf("no function found with ID: %s", fnID)
}
