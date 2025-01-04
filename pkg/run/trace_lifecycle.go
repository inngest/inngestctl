package run

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/inngest/inngest/pkg/consts"
	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/event"
	"github.com/inngest/inngest/pkg/execution"
	"github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/state"
	statev1 "github.com/inngest/inngest/pkg/execution/state"
	"github.com/inngest/inngest/pkg/execution/state/redis_state"
	statev2 "github.com/inngest/inngest/pkg/execution/state/v2"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
	"github.com/inngest/inngest/pkg/inngest"
	itrace "github.com/inngest/inngest/pkg/telemetry/trace"
	"github.com/inngest/inngest/pkg/util/aigateway"
	"github.com/oklog/ulid/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func NewTraceLifecycleListener(l *slog.Logger) execution.LifecycleListener {
	if l == nil {
		l = slog.Default()
	}

	return traceLifecycle{
		log: l,
	}
}

type traceLifecycle struct {
	execution.NoopLifecyceListener

	log *slog.Logger
}

func (l traceLifecycle) OnFunctionScheduled(ctx context.Context, md statev2.Metadata, item queue.Item, evts []event.TrackedEvent) {
	runID := md.ID.RunID
	evtIDs := []string{}
	for _, e := range evts {
		id := e.GetInternalID()
		evtIDs = append(evtIDs, id.String())
	}

	// span that tells when the function was queued
	_, span := NewSpan(ctx,
		WithScope(consts.OtelScopeTrigger),
		WithName(consts.OtelSpanTrigger),
		WithTimestamp(ulid.Time(runID.Time())),
		WithSpanAttributes(
			attribute.String(consts.OtelSysAccountID, md.ID.Tenant.AccountID.String()),
			attribute.String(consts.OtelSysWorkspaceID, md.ID.Tenant.EnvID.String()),
			attribute.String(consts.OtelSysAppID, md.ID.Tenant.AppID.String()),
			attribute.String(consts.OtelSysFunctionID, md.ID.FunctionID.String()),
			attribute.String(consts.OtelSysFunctionSlug, md.Config.FunctionSlug()),
			attribute.Int(consts.OtelSysFunctionVersion, md.Config.FunctionVersion),
			attribute.String(consts.OtelAttrSDKRunID, runID.String()),
			attribute.Int64(consts.OtelSysFunctionStatusCode, enums.RunStatusScheduled.ToCode()),
			attribute.String(consts.OtelSysEventIDs, strings.Join(evtIDs, ",")),
		),
	)
	defer span.End()

	schedule := md.Config.CronSchedule()
	if schedule != nil {
		span.SetAttributes(attribute.String(consts.OtelSysCronExpr, *schedule))
	}

	batchID := md.Config.BatchID
	if batchID != nil {
		span.SetAttributes(
			attribute.String(consts.OtelSysBatchID, batchID.String()),
			attribute.Int64(consts.OtelSysBatchTS, int64(batchID.Time())),
		)
	}
	if md.Config.DebounceFlag() {
		span.SetAttributes(attribute.Bool(consts.OtelSysDebounceTimeout, true))
	}
	if md.Config.Context != nil {
		if val, ok := md.Config.Context[consts.OtelPropagationLinkKey]; ok {
			if link, ok := val.(string); ok {
				span.SetAttributes(attribute.String(consts.OtelPropagationLinkKey, link))
			}
		}
	}

	for _, e := range evts {
		evt := e.GetEvent()
		// serialize event data to the span
		if byt, err := json.Marshal(evt); err == nil {
			span.AddEvent(string(byt), trace.WithAttributes(
				attribute.Bool(consts.OtelSysEventData, true),
				attribute.String(consts.OtelSysEventInternalID, e.GetInternalID().String()),
			))
		}
	}

	// annotate the invoke span with target function run ID for reference purposes
	for _, e := range evts {
		go func(ctx context.Context, evt event.Event) {
			if v, ok := evt.Data[consts.InngestEventDataPrefix]; ok {
				meta := event.InngestMetadata{}
				if err := meta.Decode(v); err == nil {
					if meta.InvokeTraceCarrier != nil && meta.InvokeTraceCarrier.CanResumePause() {
						ictx := itrace.UserTracer().Propagator().Extract(ctx, propagation.MapCarrier(meta.InvokeTraceCarrier.Context))

						sid := meta.InvokeTraceCarrier.SpanID()

						cIDs := strings.Split(meta.InvokeCorrelationId, ".")
						if len(cIDs) != 2 {
							// format is invalid
							l.log.Error("invalid invoke correlation ID", "metadata", meta)
							return
						}

						var mrunID ulid.ULID
						if meta.RunID() != nil {
							mrunID = *meta.RunID()
						}

						_, ispan := NewSpan(ictx,
							WithScope(consts.OtelScopeStep),
							WithName(consts.OtelSpanInvoke),
							WithTimestamp(meta.InvokeTraceCarrier.Timestamp),
							WithSpanID(sid),
							WithSpanAttributes(
								attribute.String(consts.OtelSysAccountID, md.ID.Tenant.AccountID.String()),
								attribute.String(consts.OtelSysWorkspaceID, md.ID.Tenant.EnvID.String()),
								attribute.String(consts.OtelSysAppID, meta.SourceAppID),
								attribute.String(consts.OtelSysFunctionID, meta.SourceFnID),
								attribute.Int(consts.OtelSysFunctionVersion, meta.SourceFnVersion),
								attribute.String(consts.OtelAttrSDKRunID, mrunID.String()),
								attribute.Int(consts.OtelSysStepAttempt, 0),    // ?
								attribute.Int(consts.OtelSysStepMaxAttempt, 1), // ?
								attribute.String(consts.OtelSysStepGroupID, meta.InvokeGroupID),
								attribute.String(consts.OtelSysStepOpcode, enums.OpcodeInvokeFunction.String()),
								attribute.String(consts.OtelSysStepDisplayName, meta.InvokeDisplayName),

								attribute.String(consts.OtelSysStepInvokeTargetFnID, md.ID.FunctionID.String()),
								attribute.Int64(consts.OtelSysStepInvokeExpires, meta.InvokeExpiresAt),
								attribute.String(consts.OtelSysStepInvokeTriggeringEventID, evt.ID),
								attribute.String(consts.OtelSysStepInvokeRunID, runID.String()),
								attribute.Bool(consts.OtelSysStepInvokeExpired, false),
							),
						)
						defer ispan.End()
					}
				}
			}
		}(ctx, e.GetEvent())
	}
}

func (l traceLifecycle) OnFunctionStarted(
	ctx context.Context,
	md sv2.Metadata,
	item queue.Item,
	evts []json.RawMessage,
) {
	// reassign here to make sure we have the right traceID and such
	ctx = l.extractTraceCtx(ctx, md, true)

	start := time.Now()
	if !md.Config.StartedAt.IsZero() {
		start = md.Config.StartedAt
	}

	// spanID should always exists
	spanID, err := md.Config.GetSpanID()
	if err != nil {
		// generate a new one here to be used for subsequent runs.
		// this could happen for runs that started before this feature was introduced.
		sid := NewSpanID(ctx)
		spanID = &sid
	}

	runID := md.ID.RunID
	slug := md.Config.FunctionSlug()

	evtIDs := make([]string, len(md.Config.EventIDs))
	for i, e := range md.Config.EventIDs {
		evtIDs[i] = e.String()
	}

	// (re)Construct function span to force update the end time
	_, span := NewSpan(ctx,
		WithScope(consts.OtelScopeFunction),
		WithName(slug),
		WithTimestamp(start),
		WithSpanID(*spanID),
		WithSpanAttributes(
			attribute.String(consts.OtelSysAccountID, md.ID.Tenant.AccountID.String()),
			attribute.String(consts.OtelSysWorkspaceID, md.ID.Tenant.EnvID.String()),
			attribute.String(consts.OtelSysAppID, md.ID.Tenant.AppID.String()),
			attribute.String(consts.OtelSysFunctionID, md.ID.FunctionID.String()),
			attribute.String(consts.OtelSysFunctionSlug, slug),
			attribute.Int(consts.OtelSysFunctionVersion, md.Config.FunctionVersion),
			attribute.String(consts.OtelAttrSDKRunID, runID.String()),
			attribute.String(consts.OtelSysEventIDs, strings.Join(evtIDs, ",")),
			attribute.String(consts.OtelSysIdempotencyKey, md.Config.Idempotency),
			attribute.Int64(consts.OtelSysFunctionStatusCode, enums.RunStatusRunning.ToCode()),
			attribute.Bool(consts.OtelSysStepFirst, true),
		),
	)
	defer span.End()

	schedule := md.Config.CronSchedule()
	if schedule != nil {
		span.SetAttributes(attribute.String(consts.OtelSysCronExpr, *schedule))
	}
	batchID := md.Config.BatchID
	if batchID != nil {
		// fmt.Println("Start RunID:", runID.String(), ", BatchID:", batchID.String())
		span.SetAttributes(
			attribute.String(consts.OtelSysBatchID, batchID.String()),
			attribute.Int64(consts.OtelSysBatchTS, int64(batchID.Time())),
		)
	}
	if md.Config.TraceLink() != nil {
		span.SetAttributes(attribute.String(consts.OtelSysFunctionLink, *md.Config.TraceLink()))
	}

	if err := span.SetEvents(ctx, evts, md.Config.EventIDMapping()); err != nil {
		l.log.Warn("error setting events",
			"lifecycle", "OnFunctionStarted",
			"errors", err,
			"meta", md,
			"item", item,
			"evts", evts,
		)
	}
}

func (l traceLifecycle) OnFunctionFinished(
	ctx context.Context,
	md statev2.Metadata,
	item queue.Item,
	evts []json.RawMessage,
	resp statev1.DriverResponse,
) {
	// reassign here to make sure we have the right traceID and such
	ctx = l.extractTraceCtx(ctx, md, true)

	start := time.Now()
	if !md.Config.StartedAt.IsZero() {
		start = md.Config.StartedAt
	}

	// spanID should always exists
	spanID, err := md.Config.GetSpanID()
	if err != nil {
		// generate a new one here to be used for subsequent runs.
		// this could happen for runs that started before this feature was introduced.
		sid := NewSpanID(ctx)
		spanID = &sid
	}

	runID := md.ID.RunID
	slug := md.Config.FunctionSlug()

	evtIDs := make([]string, len(md.Config.EventIDs))
	for i, e := range md.Config.EventIDs {
		evtIDs[i] = e.String()
	}

	// (re)Construct function span to force update the end time
	_, span := NewSpan(ctx,
		WithScope(consts.OtelScopeFunction),
		WithName(slug),
		WithTimestamp(start),
		WithSpanID(*spanID),
		WithSpanAttributes(
			attribute.String(consts.OtelSysAccountID, md.ID.Tenant.AccountID.String()),
			attribute.String(consts.OtelSysWorkspaceID, md.ID.Tenant.EnvID.String()),
			attribute.String(consts.OtelSysAppID, md.ID.Tenant.AppID.String()),
			attribute.String(consts.OtelSysFunctionID, md.ID.FunctionID.String()),
			attribute.String(consts.OtelSysFunctionSlug, slug),
			attribute.Int(consts.OtelSysFunctionVersion, md.Config.FunctionVersion),
			attribute.String(consts.OtelAttrSDKRunID, runID.String()),
			attribute.String(consts.OtelSysEventIDs, strings.Join(evtIDs, ",")),
			attribute.String(consts.OtelSysIdempotencyKey, md.Config.Idempotency),
			attribute.Bool(consts.OtelSysStepFirst, true),
			attribute.Bool(consts.OtelSysFunctionHasAI, md.Config.HasAI),
		),
	)
	defer span.End()

	schedule := md.Config.CronSchedule()
	if schedule != nil {
		span.SetAttributes(attribute.String(consts.OtelSysCronExpr, *schedule))
	}
	batchID := md.Config.BatchID
	if batchID != nil {
		// fmt.Println("End RunID: ", runID.String(), ", BatchID:", batchID.String())
		span.SetAttributes(
			attribute.String(consts.OtelSysBatchID, batchID.String()),
			attribute.Int64(consts.OtelSysBatchTS, int64(batchID.Time())),
		)
	}
	if md.Config.TraceLink() != nil {
		span.SetAttributes(attribute.String(consts.OtelSysFunctionLink, *md.Config.TraceLink()))
	}

	if err := span.SetEvents(ctx, evts, md.Config.EventIDMapping()); err != nil {
		l.log.Warn("error setting events",
			"lifecycle", "OnFunctionFinished",
			"errors", err,
			"meta", md,
			"item", item,
			"evts", evts,
		)
	}

	switch resp.StatusCode {
	case 200:
		span.SetStatus(codes.Ok, "success")
		span.SetAttributes(attribute.Int64(consts.OtelSysFunctionStatusCode, enums.RunStatusCompleted.ToCode()))
	default: // everything else are errors
		span.SetStatus(codes.Error, resp.Error())
		span.SetAttributes(attribute.Int64(consts.OtelSysFunctionStatusCode, enums.RunStatusFailed.ToCode()))
	}

	var output any = resp.Err
	if resp.Output != nil {
		output = resp.Output
	}
	span.SetFnOutput(output)
}

func (l traceLifecycle) OnFunctionCancelled(ctx context.Context, md sv2.Metadata, req execution.CancelRequest, evts []json.RawMessage) {
	ctx = l.extractTraceCtx(ctx, md, true)

	start := time.Now()
	if !md.Config.StartedAt.IsZero() {
		start = md.Config.StartedAt
	}

	fnSpanID, err := md.Config.GetSpanID()
	if err != nil {
		l.log.Error("error retrieving spanID for cancelled function run",
			"err", err,
			"identifier", md.ID,
		)
		return
	}

	evtIDs := make([]string, len(md.Config.EventIDs))
	for i, eid := range md.Config.EventIDs {
		evtIDs[i] = eid.String()
	}

	_, span := NewSpan(ctx,
		WithScope(consts.OtelScopeFunction),
		WithName(md.Config.FunctionSlug()),
		WithTimestamp(start),
		WithSpanID(*fnSpanID),
		WithSpanAttributes(
			attribute.String(consts.OtelSysAccountID, md.ID.Tenant.AccountID.String()),
			attribute.String(consts.OtelSysWorkspaceID, md.ID.Tenant.EnvID.String()),
			attribute.String(consts.OtelSysAppID, md.ID.Tenant.AppID.String()),
			attribute.String(consts.OtelSysFunctionID, md.ID.FunctionID.String()),
			attribute.String(consts.OtelSysFunctionSlug, md.Config.FunctionSlug()),
			attribute.Int(consts.OtelSysFunctionVersion, md.Config.FunctionVersion),
			attribute.String(consts.OtelAttrSDKRunID, md.ID.RunID.String()),
			attribute.String(consts.OtelSysEventIDs, strings.Join(evtIDs, ",")),
			attribute.String(consts.OtelSysIdempotencyKey, md.IdempotencyKey()),
			attribute.Int64(consts.OtelSysFunctionStatusCode, enums.RunStatusCancelled.ToCode()),
		),
	)
	defer span.End()

	if md.Config.CronSchedule() != nil {
		span.SetAttributes(attribute.String(consts.OtelSysCronExpr, *md.Config.CronSchedule()))
	}
	if md.Config.BatchID != nil {
		span.SetAttributes(
			attribute.String(consts.OtelSysBatchID, md.Config.BatchID.String()),
			attribute.Int64(consts.OtelSysBatchTS, int64(md.Config.BatchID.Time())),
		)
	}

	if err := span.SetEvents(ctx, evts, md.Config.EventIDMapping()); err != nil {
		l.log.Warn("error setting events",
			"lifecycle", "OnFunctionCancelled",
			"errors", err,
			"meta", md,
			"req", req,
			"evts", evts,
		)
	}
}

func (l traceLifecycle) OnFunctionSkipped(
	ctx context.Context,
	md sv2.Metadata,
	s execution.SkipState,
) {
	ctx = l.extractTraceCtx(ctx, md, true)

	start := time.Now()
	if !md.Config.StartedAt.IsZero() {
		start = md.Config.StartedAt
	}

	// spanID should always exists
	spanID, err := md.Config.GetSpanID()
	if err != nil {
		// generate a new one here to be used for subsequent runs.
		// this could happen for runs that started before this feature was introduced.
		sid := NewSpanID(ctx)
		spanID = &sid
	}

	runID := md.ID.RunID
	slug := md.Config.FunctionSlug()

	evtIDs := make([]string, len(md.Config.EventIDs))
	for i, e := range md.Config.EventIDs {
		evtIDs[i] = e.String()
	}

	// NOTE: generate the trigger span since it's skipped
	{
		_, trigger := NewSpan(ctx,
			WithScope(consts.OtelScopeTrigger),
			WithName(consts.OtelSpanTrigger),
			WithTimestamp(ulid.Time(runID.Time())),
			WithSpanAttributes(
				attribute.String(consts.OtelSysAccountID, md.ID.Tenant.AccountID.String()),
				attribute.String(consts.OtelSysWorkspaceID, md.ID.Tenant.EnvID.String()),
				attribute.String(consts.OtelSysAppID, md.ID.Tenant.AppID.String()),
				attribute.String(consts.OtelSysFunctionID, md.ID.FunctionID.String()),
				attribute.String(consts.OtelSysFunctionSlug, md.Config.FunctionSlug()),
				attribute.Int(consts.OtelSysFunctionVersion, md.Config.FunctionVersion),
				attribute.String(consts.OtelAttrSDKRunID, runID.String()),
				attribute.Int64(consts.OtelSysFunctionStatusCode, enums.RunStatusScheduled.ToCode()),
				attribute.String(consts.OtelSysEventIDs, strings.Join(evtIDs, ",")),
			),
		)
		defer trigger.End()

		schedule := md.Config.CronSchedule()
		if schedule != nil {
			trigger.SetAttributes(attribute.String(consts.OtelSysCronExpr, *schedule))
		}

		batchID := md.Config.BatchID
		if batchID != nil {
			trigger.SetAttributes(
				attribute.String(consts.OtelSysBatchID, batchID.String()),
				attribute.Int64(consts.OtelSysBatchTS, int64(batchID.Time())),
			)
		}
		if md.Config.DebounceFlag() {
			trigger.SetAttributes(attribute.Bool(consts.OtelSysDebounceTimeout, true))
		}
		if md.Config.Context != nil {
			if val, ok := md.Config.Context[consts.OtelPropagationLinkKey]; ok {
				if link, ok := val.(string); ok {
					trigger.SetAttributes(attribute.String(consts.OtelPropagationLinkKey, link))
				}
			}
		}

		if err := trigger.SetEvents(ctx, s.Events, md.Config.EventIDMapping()); err != nil {
			l.log.Warn("error settings events for trigger",
				"lifecycle", "OnFunctionSkipped",
				"errors", err,
				"meta", md,
				"evts", s.Events,
			)
		}
	}

	// Generate the function span
	_, span := NewSpan(ctx,
		WithScope(consts.OtelScopeFunction),
		WithName(slug),
		WithTimestamp(start),
		WithSpanID(*spanID),
		WithSpanAttributes(
			attribute.String(consts.OtelSysAccountID, md.ID.Tenant.AccountID.String()),
			attribute.String(consts.OtelSysWorkspaceID, md.ID.Tenant.EnvID.String()),
			attribute.String(consts.OtelSysAppID, md.ID.Tenant.AppID.String()),
			attribute.String(consts.OtelSysFunctionID, md.ID.FunctionID.String()),
			attribute.String(consts.OtelSysFunctionSlug, slug),
			attribute.Int(consts.OtelSysFunctionVersion, md.Config.FunctionVersion),
			attribute.String(consts.OtelAttrSDKRunID, runID.String()),
			attribute.String(consts.OtelSysEventIDs, strings.Join(evtIDs, ",")),
			attribute.String(consts.OtelSysIdempotencyKey, md.IdempotencyKey()),
			attribute.Int64(consts.OtelSysFunctionStatusCode, enums.RunStatusSkipped.ToCode()),
		),
	)
	defer span.End()

	if md.Config.CronSchedule() != nil {
		span.SetAttributes(attribute.String(consts.OtelSysCronExpr, *md.Config.CronSchedule()))
	}
	if md.Config.BatchID != nil {
		span.SetAttributes(
			attribute.String(consts.OtelSysBatchID, md.Config.BatchID.String()),
			attribute.Int64(consts.OtelSysBatchTS, int64(md.Config.BatchID.Time())),
		)
	}

	if err := span.SetEvents(ctx, s.Events, md.Config.EventIDMapping()); err != nil {
		l.log.Warn("error setting events",
			"lifecycle", "OnFunctionSkipped",
			"errors", err,
			"meta", md,
			"evts", s.Events,
		)
	}
}

func (l traceLifecycle) OnStepStarted(
	ctx context.Context,
	md statev2.Metadata,
	item queue.Item,
	edge inngest.Edge,
	url string,
) {
	// reassign here to make sure we have the right traceID and such
	ctx = l.extractTraceCtx(ctx, md, false)

	spanID, err := item.SpanID()
	if err != nil {
		l.log.Error("error retrieving spanID", "error", err, "meta", md, "lifecycle", "OnStepStarted")
		return
	}
	start, ok := redis_state.GetItemStart(ctx)
	if !ok {
		l.log.Warn("start time not available for item", "lifecycle", "OnStepStarted")
		start = time.Now()
	}
	runID := md.ID.RunID

	name := consts.OtelExecPlaceholder
	// Check if this is a step planned from parallelism
	if edge, _ := queue.GetEdge(item); edge != nil {
		if edge.Edge.IncomingGeneratorStepName != "" {
			name = edge.Edge.IncomingGeneratorStepName
		}
	}

	_, span := NewSpan(ctx,
		WithScope(consts.OtelScopeExecution),
		WithName(name),
		WithTimestamp(start),
		WithSpanID(*spanID),
		WithSpanAttributes(
			attribute.String(consts.OtelSysAccountID, md.ID.Tenant.AccountID.String()),
			attribute.String(consts.OtelSysWorkspaceID, md.ID.Tenant.EnvID.String()),
			attribute.String(consts.OtelSysAppID, md.ID.Tenant.AppID.String()),
			attribute.String(consts.OtelSysFunctionID, md.ID.FunctionID.String()),
			attribute.String(consts.OtelSysFunctionSlug, md.Config.FunctionSlug()),
			attribute.Int(consts.OtelSysFunctionVersion, md.Config.FunctionVersion),
			attribute.String(consts.OtelAttrSDKRunID, runID.String()),
			attribute.Int(consts.OtelSysStepAttempt, item.Attempt),
			attribute.Int(consts.OtelSysStepMaxAttempt, item.GetMaxAttempts()),
			attribute.String(consts.OtelSysStepGroupID, item.GroupID),
			attribute.String(consts.OtelSysStepOpcode, enums.OpcodeStepPlanned.String()),
		),
	)
	defer span.End()

	if item.RunInfo != nil {
		span.SetAttributes(
			attribute.Int64(consts.OtelSysDelaySystem, item.RunInfo.Latency.Milliseconds()),
			attribute.Int64(consts.OtelSysDelaySojourn, item.RunInfo.SojournDelay.Milliseconds()),
		)
	}
	if item.Attempt > 0 {
		span.SetAttributes(attribute.Bool(consts.OtelSysStepRetry, true))
	}

	// first step
	if edge.Incoming == inngest.TriggerName {
		// NOTE:
		// annotate the step as the first step of the function
		// this way the delay associated with this run is directly correlated to the delay of the
		// function run itself.
		if item.Attempt == 0 {
			span.SetAttributes(attribute.Bool(consts.OtelSysStepFirst, true))
		}
	}
}

func (l traceLifecycle) OnStepGatewayRequestFinished(
	ctx context.Context,
	md sv2.Metadata,
	item queue.Item,
	edge inngest.Edge,
	// Opcode is the opcode for the offloaded request.  The Data field must be
	// set with the length of the output.
	op statev1.GeneratorOpcode,
	// Resp is the HTTP response
	resp *http.Response,
	// runErr is non-nil on a non-2xx status code.
	runErr error,
) {
	// reassign here to make sure we have the right traceID and such
	ctx = l.extractTraceCtx(ctx, md, false)

	spanID, err := item.SpanID()
	if err != nil {
		l.log.Error("error retrieving spanID", "meta", md, "error", err, "lifecycle", "OnStepFinished")
		return
	}
	start, ok := redis_state.GetItemStart(ctx)
	if !ok {
		l.log.Warn("start time not available for item", "lifecycle", "OnStepFinished")
		start = time.Now()
	}
	runID := md.ID.RunID

	_, span := NewSpan(ctx,
		WithScope(consts.OtelScopeExecution),
		WithName(consts.OtelExecPlaceholder),
		WithTimestamp(start),
		WithSpanID(*spanID),
		WithSpanAttributes(
			attribute.String(consts.OtelSysAccountID, md.ID.Tenant.AccountID.String()),
			attribute.String(consts.OtelSysWorkspaceID, md.ID.Tenant.EnvID.String()),
			attribute.String(consts.OtelSysAppID, md.ID.Tenant.AppID.String()),
			attribute.String(consts.OtelSysFunctionID, md.ID.FunctionID.String()),
			attribute.String(consts.OtelSysFunctionSlug, md.Config.FunctionSlug()),
			attribute.Int(consts.OtelSysFunctionVersion, md.Config.FunctionVersion),
			attribute.String(consts.OtelAttrSDKRunID, runID.String()),
			attribute.Int(consts.OtelSysStepAttempt, item.Attempt),
			attribute.Int(consts.OtelSysStepMaxAttempt, item.GetMaxAttempts()),
			attribute.String(consts.OtelSysStepGroupID, item.GroupID),
			attribute.String(consts.OtelSysStepOpcode, enums.OpcodeStepPlanned.String()),
		),
	)
	// Common attrs.
	span.SetName(op.UserDefinedName())
	span.SetAttributes(
		attribute.Int(consts.OtelSysStepStatusCode, resp.StatusCode),
		attribute.Int(consts.OtelSysStepOutputSizeBytes, int(resp.ContentLength)),
		attribute.String(consts.OtelSysStepID, op.ID),
		attribute.String(consts.OtelSysStepDisplayName, op.UserDefinedName()),
		attribute.String(consts.OtelSysStepOpcode, op.Op.String()),
	)

	defer span.End()

	if item.RunInfo != nil {
		span.SetAttributes(
			attribute.Int64(consts.OtelSysDelaySystem, item.RunInfo.Latency.Milliseconds()),
			attribute.Int64(consts.OtelSysDelaySojourn, item.RunInfo.SojournDelay.Milliseconds()),
		)
	}
	if item.Attempt > 0 {
		span.SetAttributes(attribute.Bool(consts.OtelSysStepRetry, true))
	}
	// first step
	if edge.Incoming == inngest.TriggerName {
		// NOTE:
		// annotate the step as the first step of the function
		// this way the delay associated with this run is directly correlated to the delay of the
		// function run itself.
		if item.Attempt == 0 {
			span.SetAttributes(attribute.Bool(consts.OtelSysStepFirst, true))
		}
	}
	if runErr == nil {
		span.SetStepOutput(op.Data)
		span.SetStatus(codes.Ok, string(op.Data))
	} else {
		span.SetStatus(codes.Error, runErr.Error())
		span.SetStepOutput(runErr.Error())
	}

	if input, _ := op.Input(); input != "" {
		span.SetStepInput(input)
	}

	// If we have AI calls, parse the input and output metadata directly.
	switch op.Op {
	case enums.OpcodeAIGateway:
		req, _ := op.AIGatewayOpts()
		// Parse the request
		if parsed, err := aigateway.ParseInput(ctx, req); err == nil {
			span.SetAIRequestMetadata(parsed)
		}
		// And parse the response.
		if parsed, err := aigateway.ParseOutput(ctx, req.Format, op.Data); err == nil {
			span.SetAIResponseMetadata(parsed)
		}
	}
}

func (l traceLifecycle) OnStepFinished(
	ctx context.Context,
	md statev2.Metadata,
	item queue.Item,
	edge inngest.Edge,
	resp *statev1.DriverResponse,
	runErr error,
) {
	spew.Dump("HIT OnStepFinished", string(debug.Stack()))

	// reassign here to make sure we have the right traceID and such
	ctx = l.extractTraceCtx(ctx, md, false)

	spanID, err := item.SpanID()
	if err != nil {
		l.log.Error("error retrieving spanID", "meta", md, "error", err, "lifecycle", "OnStepFinished")
		return
	}
	start, ok := redis_state.GetItemStart(ctx)
	if !ok {
		l.log.Warn("start time not available for item", "lifecycle", "OnStepFinished")
		start = time.Now()
	}
	runID := md.ID.RunID

	_, span := NewSpan(ctx,
		WithScope(consts.OtelScopeExecution),
		WithName(consts.OtelExecPlaceholder),
		WithTimestamp(start),
		WithSpanID(*spanID),
		WithSpanAttributes(
			attribute.String(consts.OtelSysAccountID, md.ID.Tenant.AccountID.String()),
			attribute.String(consts.OtelSysWorkspaceID, md.ID.Tenant.EnvID.String()),
			attribute.String(consts.OtelSysAppID, md.ID.Tenant.AppID.String()),
			attribute.String(consts.OtelSysFunctionID, md.ID.FunctionID.String()),
			attribute.String(consts.OtelSysFunctionSlug, md.Config.FunctionSlug()),
			attribute.Int(consts.OtelSysFunctionVersion, md.Config.FunctionVersion),
			attribute.String(consts.OtelAttrSDKRunID, runID.String()),
			attribute.Int(consts.OtelSysStepAttempt, item.Attempt),
			attribute.Int(consts.OtelSysStepMaxAttempt, item.GetMaxAttempts()),
			attribute.String(consts.OtelSysStepGroupID, item.GroupID),
			attribute.String(consts.OtelSysStepOpcode, enums.OpcodeStepPlanned.String()),
			attribute.Bool(consts.OtelSysFunctionHasAI, md.Config.HasAI),
		),
	)
	defer span.End()

	if item.RunInfo != nil {
		span.SetAttributes(
			attribute.Int64(consts.OtelSysDelaySystem, item.RunInfo.Latency.Milliseconds()),
			attribute.Int64(consts.OtelSysDelaySojourn, item.RunInfo.SojournDelay.Milliseconds()),
		)
	}
	if item.Attempt > 0 {
		span.SetAttributes(attribute.Bool(consts.OtelSysStepRetry, true))
	}

	// first step
	if edge.Incoming == inngest.TriggerName {
		// NOTE:
		// annotate the step as the first step of the function
		// this way the delay associated with this run is directly correlated to the delay of the
		// function run itself.
		if item.Attempt == 0 {
			span.SetAttributes(attribute.Bool(consts.OtelSysStepFirst, true))
		}
	}

	if runErr != nil {
		span.SetStatus(codes.Error, runErr.Error())
		span.SetStepOutput(runErr)
		return
	}

	// check response
	if resp != nil {
		if op := resp.TraceVisibleStepExecution(); op != nil {
			spanName := op.UserDefinedName()
			span.SetName(spanName)

			// fnSpan.SetAttributes(attribute.Int64(consts.OtelSysFunctionStatusCode, enums.RunStatusRunning.ToCode()))

			foundOp := op.Op
			// The op changes based on the current state of the step, so we
			// are required to normalize here.
			switch foundOp {
			case enums.OpcodeStep, enums.OpcodeStepRun, enums.OpcodeStepError:
				foundOp = enums.OpcodeStepRun
			}

			span.SetAttributes(
				attribute.Int(consts.OtelSysStepStatusCode, resp.StatusCode),
				attribute.Int(consts.OtelSysStepOutputSizeBytes, resp.OutputSize),
				attribute.String(consts.OtelSysStepID, op.ID),
				attribute.String(consts.OtelSysStepDisplayName, op.UserDefinedName()),
				attribute.String(consts.OtelSysStepOpcode, foundOp.String()),
			)

			if typ := op.RunType(); typ != "" {
				span.SetStepRunType(typ)
			}

			if op.IsError() {
				span.SetStepOutput(op.Error)
				span.SetStatus(codes.Error, op.Error.Message)
			} else {
				span.SetStepOutput(op.Data)
				span.SetStatus(codes.Ok, string(op.Data))
			}

			if input, _ := op.Input(); input != "" {
				span.SetStepInput(input)
			}

			// If we have AI calls, parse the input and output metadata directly.
			switch op.Op {
			case enums.OpcodeAIGateway:
				req, _ := op.AIGatewayOpts()
				if parsed, err := aigateway.ParseInput(ctx, req); err == nil {
					span.SetAIRequestMetadata(parsed)
				}
			case enums.OpcodeStep, enums.OpcodeStepRun:
				// Handle input and attempt to best-effort parse.
				input, _ := op.Input()
				if parsed, err := aigateway.ParseUnknownInput(ctx, json.RawMessage(input)); err == nil {
					span.SetAIRequestMetadata(parsed)

					// Now that we know the step run was a wrapped AI call, we can also parse the output
					// to see if we can store the response metadata correctly.
				}
			}

		} else if resp.Retryable() { // these are function retries
			span.SetStatus(codes.Error, *resp.Err)
			span.SetAttributes(
				attribute.String(consts.OtelSysStepOpcode, enums.OpcodeNone.String()),
				attribute.Int(consts.OtelSysStepStatusCode, resp.StatusCode),
				attribute.Int(consts.OtelSysStepOutputSizeBytes, resp.OutputSize),
			)

			var output any = resp.Err
			if resp.Output != nil {
				output = resp.Output
			}
			span.SetStepOutput(output)
		} else if resp.IsTraceVisibleFunctionExecution() {
			spanName := consts.OtelExecFnOk
			span.SetStatus(codes.Ok, "success")

			if resp.StatusCode != 200 {
				spanName = consts.OtelExecFnErr
				span.SetStatus(codes.Error, resp.Error())
			}

			span.SetAttributes(attribute.String(consts.OtelSysStepOpcode, enums.OpcodeNone.String()))
			span.SetName(spanName)

			var output any = resp.Err
			if resp.Output != nil {
				output = resp.Output
			}
			span.SetStepOutput(output)
			span.SetStepStack(md.Stack)
		} else {
			// if it's not a step or function response that represents either a failed or a successful execution.

			// annotate it as a planning step currently only used for parallelism, so we know
			// we can ignore it when displaying on UI
			span.SetAttributes(
				attribute.Bool(consts.OtelSysStepPlan, true),
			)
		}
	}
}

func (l traceLifecycle) OnSleep(
	ctx context.Context,
	md statev2.Metadata,
	item queue.Item,
	gen statev1.GeneratorOpcode,
	until time.Time,
) {
	// reassign here to make sure we have the right traceID and such
	ctx = l.extractTraceCtx(ctx, md, false)

	dur, err := gen.SleepDuration()
	if err != nil {
		l.log.Error("error retrieving sleep duration", "error", err, "meta", md, "lifecycle", "OnSleep")
		return
	}

	startedAt := until.Add(-1 * dur)
	runID := md.ID.RunID

	_, span := NewSpan(ctx,
		WithScope(consts.OtelScopeStep),
		WithName(consts.OtelSpanSleep),
		WithTimestamp(startedAt),
		WithSpanAttributes(
			attribute.String(consts.OtelSysAccountID, md.ID.Tenant.AccountID.String()),
			attribute.String(consts.OtelSysWorkspaceID, md.ID.Tenant.EnvID.String()),
			attribute.String(consts.OtelSysAppID, md.ID.Tenant.AppID.String()),
			attribute.String(consts.OtelSysFunctionID, md.ID.FunctionID.String()),
			attribute.String(consts.OtelSysFunctionSlug, md.Config.FunctionSlug()),
			attribute.Int(consts.OtelSysFunctionVersion, md.Config.FunctionVersion),
			attribute.String(consts.OtelAttrSDKRunID, runID.String()),
			attribute.Int(consts.OtelSysStepAttempt, 0),    // ?
			attribute.Int(consts.OtelSysStepMaxAttempt, 1), // ?
			attribute.String(consts.OtelSysStepGroupID, item.GroupID),
			attribute.String(consts.OtelSysStepOpcode, enums.OpcodeSleep.String()),
			attribute.String(consts.OtelSysStepDisplayName, gen.UserDefinedName()),
			attribute.Int64(consts.OtelSysStepSleepEndAt, until.UnixMilli()),
		),
	)
	defer span.End(trace.WithTimestamp(until))
}

func (l traceLifecycle) OnInvokeFunction(
	ctx context.Context,
	md statev2.Metadata,
	item queue.Item,
	gen statev1.GeneratorOpcode,
	invocationEvt event.Event,
) {
	ctx = l.extractTraceCtx(ctx, md, false)

	meta, err := invocationEvt.InngestMetadata()
	if err != nil {
		l.log.Error("invocation event metadata not available",
			"lifecycle", "OnInvokeFunction",
			"meta", md,
			"evt", invocationEvt,
			"error", err,
		)
		return
	}

	runID := md.ID.RunID
	carrier := meta.InvokeTraceCarrier
	if carrier == nil {
		l.log.Error("no trace carrier available",
			"meta", md,
			"lifecycle", "OnInvokeFunction",
		)
		return
	}
	spanID := carrier.SpanID()

	_, span := NewSpan(ctx,
		WithScope(consts.OtelScopeStep),
		WithName(consts.OtelSpanInvoke),
		WithTimestamp(carrier.Timestamp),
		WithSpanID(spanID),
		WithSpanAttributes(
			attribute.String(consts.OtelSysAccountID, md.ID.Tenant.AccountID.String()),
			attribute.String(consts.OtelSysWorkspaceID, md.ID.Tenant.EnvID.String()),
			attribute.String(consts.OtelSysAppID, md.ID.Tenant.AppID.String()),
			attribute.String(consts.OtelSysFunctionID, md.ID.FunctionID.String()),
			attribute.String(consts.OtelSysFunctionSlug, md.Config.FunctionSlug()),
			attribute.Int(consts.OtelSysFunctionVersion, md.Config.FunctionVersion),
			attribute.String(consts.OtelAttrSDKRunID, runID.String()),
			attribute.Int(consts.OtelSysStepAttempt, 0),    // ?
			attribute.Int(consts.OtelSysStepMaxAttempt, 1), // ?
			attribute.String(consts.OtelSysStepGroupID, item.GroupID),
			attribute.String(consts.OtelSysStepOpcode, enums.OpcodeInvokeFunction.String()),
			attribute.String(consts.OtelSysStepDisplayName, gen.UserDefinedName()),
			attribute.String(consts.OtelSysStepInvokeTargetFnID, meta.InvokeFnID),
			attribute.Int64(consts.OtelSysStepInvokeExpires, meta.InvokeExpiresAt),
			attribute.String(consts.OtelSysStepInvokeTriggeringEventID, invocationEvt.ID),
		),
	)
	defer span.End()
}

func (l traceLifecycle) OnInvokeFunctionResumed(
	ctx context.Context,
	md statev2.Metadata,
	pause state.Pause,
	r execution.ResumeRequest,
) {
	if pause.Metadata == nil {
		l.log.Error("no pause metadata", "meta", md, "lifecycle", "OnInvokeFunctionResumed")
		return
	}

	meta, ok := pause.Metadata[consts.OtelPropagationKey]
	if !ok {
		l.log.Error("no trace propagation in pause", "meta", md, "lifecycle", "OnInvokeFunctionResumed")
		return
	}

	returnedEventID := ""
	if r.EventID != nil {
		returnedEventID = r.EventID.String()
	}

	carrier := itrace.NewTraceCarrier()
	if err := carrier.Unmarshal(meta); err == nil {
		ctx = itrace.UserTracer().Propagator().Extract(ctx, propagation.MapCarrier(carrier.Context))
		if carrier.CanResumePause() {
			// Used for spans
			triggeringEventID := ""
			if pause.TriggeringEventID != nil {
				triggeringEventID = *pause.TriggeringEventID
			}

			targetFnID := ""
			if pause.InvokeTargetFnID != nil {
				targetFnID = *pause.InvokeTargetFnID
			}

			runID := ""
			if r.RunID != nil {
				runID = r.RunID.String()
			}

			_, span := NewSpan(ctx,
				WithScope(consts.OtelScopeStep),
				WithName(consts.OtelSpanInvoke),
				WithTimestamp(carrier.Timestamp),
				WithSpanID(carrier.SpanID()),
				WithSpanAttributes(
					attribute.String(consts.OtelSysAccountID, pause.Identifier.AccountID.String()),
					attribute.String(consts.OtelSysWorkspaceID, pause.Identifier.WorkspaceID.String()),
					attribute.String(consts.OtelSysAppID, pause.Identifier.AppID.String()),
					attribute.String(consts.OtelSysFunctionID, pause.Identifier.WorkflowID.String()),
					attribute.Int(consts.OtelSysFunctionVersion, pause.Identifier.WorkflowVersion),
					attribute.String(consts.OtelAttrSDKRunID, pause.Identifier.RunID.String()),
					attribute.Int(consts.OtelSysStepAttempt, 0),    // ?
					attribute.Int(consts.OtelSysStepMaxAttempt, 1), // ?
					attribute.String(consts.OtelSysStepGroupID, pause.GroupID),
					attribute.String(consts.OtelSysStepDisplayName, pause.StepName),
					attribute.String(consts.OtelSysStepOpcode, enums.OpcodeInvokeFunction.String()),
					attribute.String(consts.OtelSysStepInvokeTargetFnID, targetFnID),
					attribute.Int64(consts.OtelSysStepInvokeExpires, pause.Expires.Time().UnixMilli()),
					attribute.String(consts.OtelSysStepInvokeTriggeringEventID, triggeringEventID),
					attribute.String(consts.OtelSysStepInvokeReturnedEventID, returnedEventID),
					attribute.String(consts.OtelSysStepInvokeRunID, runID),
					attribute.Bool(consts.OtelSysStepInvokeExpired, r.EventID == nil),
				),
			)
			defer span.End()

			var output any
			if r.With != nil {
				output = r.With
			}
			if r.HasError() {
				output = r.Error()
				span.SetStatus(codes.Error, r.Error())
			}

			if output != nil {
				span.SetStepOutput(output)
			}
		}
	}
}

func (l traceLifecycle) OnWaitForEvent(
	ctx context.Context,
	md statev2.Metadata,
	item queue.Item,
	gen statev1.GeneratorOpcode,
	pause state.Pause,
) {
	ctx = l.extractTraceCtx(ctx, md, false)

	runID := md.ID.RunID
	opts, err := gen.WaitForEventOpts()
	if err != nil {
		l.log.Error("error retrieving wait opts", "error", err, "meta", md, "lifecycle", "OnWaitForEvent")
		return
	}
	expires, err := opts.Expires()
	if err != nil {
		l.log.Error("error retrieving expiration for wait", "error", err, "meta", md, "lifecycle", "OnWaitForEvent")
		return
	}

	v, ok := pause.Metadata[consts.OtelPropagationKey]
	if !ok {
		l.log.Error("no trace propagation", "meta", md, "lifecycle", "OnWaitForEvent")
		return
	}
	carrier, ok := v.(*itrace.TraceCarrier)
	if !ok {
		l.log.Error("no trace carrier", "meta", md, "lifecycle", "OnWaitForEvent")
		return
	}

	_, span := NewSpan(ctx,
		WithScope(consts.OtelScopeStep),
		WithName(consts.OtelSpanWaitForEvent),
		WithTimestamp(carrier.Timestamp),
		WithSpanID(carrier.SpanID()),
		WithSpanAttributes(
			attribute.String(consts.OtelSysStepOpcode, enums.OpcodeWaitForEvent.String()),
			attribute.String(consts.OtelSysAccountID, md.ID.Tenant.AccountID.String()),
			attribute.String(consts.OtelSysWorkspaceID, md.ID.Tenant.EnvID.String()),
			attribute.String(consts.OtelSysAppID, md.ID.Tenant.AppID.String()),
			attribute.String(consts.OtelSysFunctionID, md.ID.FunctionID.String()),
			attribute.Int(consts.OtelSysFunctionVersion, md.Config.FunctionVersion),
			attribute.String(consts.OtelAttrSDKRunID, runID.String()),
			attribute.Int(consts.OtelSysStepAttempt, 0),
			attribute.Int(consts.OtelSysStepMaxAttempt, 1),
			attribute.String(consts.OtelSysStepGroupID, item.GroupID),
			attribute.String(consts.OtelSysStepWaitEventName, opts.Event),
			attribute.Int64(consts.OtelSysStepWaitExpires, expires.UnixMilli()),
			attribute.String(consts.OtelSysStepDisplayName, gen.UserDefinedName()),
		),
	)
	defer span.End()

	if opts.If != nil {
		span.SetAttributes(attribute.String(consts.OtelSysStepWaitExpression, *opts.If))
	}
}

func (l traceLifecycle) OnWaitForEventResumed(
	ctx context.Context,
	md statev2.Metadata,
	pause state.Pause,
	r execution.ResumeRequest,
) {
	if pause.Metadata == nil {
		l.log.Error("no pause metadata", "meta", md, "lifecycle", "OnWaitForEventResumed")
		return
	}

	meta, ok := pause.Metadata[consts.OtelPropagationKey]
	if !ok {
		l.log.Error("no trace", "meta", md, "lifecycle", "OnWaitForEventResumed")
		return
	}

	returnedEventID := ""
	if r.EventID != nil {
		returnedEventID = r.EventID.String()
	}

	carrier := itrace.NewTraceCarrier()
	if err := carrier.Unmarshal(meta); err == nil {
		ctx = itrace.UserTracer().Propagator().Extract(ctx, propagation.MapCarrier(carrier.Context))
		if carrier.CanResumePause() {
			_, span := NewSpan(ctx,
				WithScope(consts.OtelScopeStep),
				WithName(consts.OtelSpanWaitForEvent),
				WithTimestamp(carrier.Timestamp),
				WithSpanID(carrier.SpanID()),
				WithSpanAttributes(
					attribute.String(consts.OtelSysAccountID, pause.Identifier.AccountID.String()),
					attribute.String(consts.OtelSysWorkspaceID, pause.Identifier.WorkspaceID.String()),
					attribute.String(consts.OtelSysAppID, pause.Identifier.AppID.String()),
					attribute.String(consts.OtelSysFunctionID, pause.Identifier.WorkflowID.String()),
					attribute.Int(consts.OtelSysFunctionVersion, pause.Identifier.WorkflowVersion),
					attribute.String(consts.OtelAttrSDKRunID, pause.Identifier.RunID.String()),
					attribute.Int(consts.OtelSysStepAttempt, 0),    // ?
					attribute.Int(consts.OtelSysStepMaxAttempt, 1), // ?
					attribute.String(consts.OtelSysStepGroupID, pause.GroupID),
					attribute.String(consts.OtelSysStepDisplayName, pause.StepName),
					attribute.String(consts.OtelSysStepOpcode, enums.OpcodeWaitForEvent.String()),
					attribute.Int64(consts.OtelSysStepWaitExpires, pause.Expires.Time().UnixMilli()),
					attribute.Bool(consts.OtelSysStepWaitExpired, r.EventID == nil),
					attribute.String(consts.OtelSysStepWaitMatchedEventID, returnedEventID),
				),
			)
			defer span.End()

			if pause.Event != nil {
				span.SetAttributes(attribute.String(consts.OtelSysStepWaitEventName, *pause.Event))
			}
			if pause.Expression != nil {
				span.SetAttributes(attribute.String(consts.OtelSysStepWaitExpression, *pause.Expression))
			}
			if r.With != nil {
				span.SetStepOutput(r.With)
			}
			if r.HasError() {
				span.SetStatus(codes.Error, r.Error())
			}
		}
	}
}

// NOTE: this is copied from the same function inside executor.
// should probably delete it some time when it's no longer needed.
//
// extractTraceCtx extracts the trace context from the given item, if it exists.
// If it doesn't it falls back to extracting the trace for the run overall.
// If neither exist or they are invalid, it returns the original context.
func (l *traceLifecycle) extractTraceCtx(ctx context.Context, md sv2.Metadata, isFnSpan bool) context.Context {
	fntrace := md.Config.FunctionTrace()
	if fntrace != nil {
		// NOTE:
		// this gymastics happens because the carrier stores the spanID separately.
		// it probably can be simplified
		tmp := itrace.UserTracer().Propagator().Extract(ctx, propagation.MapCarrier(fntrace.Context))
		// NOTE: this is getting complex
		// need the original with the parent span
		if isFnSpan {
			return tmp
		}

		spanID, err := md.Config.GetSpanID()
		if err != nil {
			l.log.Error("error retreiving spanID for trace",
				"error", err,
				"meta", md,
				"isFnSpan", isFnSpan,
			)
			return ctx
		}
		sctx := trace.SpanContextFromContext(tmp).WithSpanID(*spanID)
		return trace.ContextWithSpanContext(ctx, sctx)
	}

	return ctx
}
