package history

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/event"
	"github.com/inngest/inngest/pkg/execution"
	"github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/state"
	"github.com/inngest/inngest/pkg/execution/state/redis_state"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
	"github.com/inngest/inngest/pkg/inngest"
	"github.com/inngest/inngest/pkg/logger"
	"github.com/oklog/ulid/v2"
)

func NewLifecycleListener(l *slog.Logger, d ...Driver) execution.LifecycleListener {
	if l == nil {
		l = slog.Default()
	}
	return lifecycle{
		log:     l,
		drivers: d,
	}
}

type lifecycle struct {
	log     *slog.Logger
	drivers []Driver
}

func (l lifecycle) Close(ctx context.Context) error {
	var err error
	for _, d := range l.drivers {
		err = errors.Join(err, d.Close(ctx))
	}
	return err
}

// OnFunctionScheduled is called when a new function is initialized from
// an event or trigger.
//
// Note that this does not mean the function immediately starts.  A function
// may start if and when there's capacity due to concurrency.
func (l lifecycle) OnFunctionScheduled(
	ctx context.Context,
	md sv2.Metadata,
	item queue.Item,
	_ []event.TrackedEvent,
) {
	groupID, err := toUUID(item.GroupID)
	if err != nil {
		l.log.Error(
			"error parsing group ID",
			"error", err,
			"group_id", item.GroupID,
			"run_id", md.ID.RunID.String(),
		)
	}

	h := History{
		Cron:            md.Config.CronSchedule(),
		ID:              ulid.MustNew(ulid.Now(), rand.Reader),
		AccountID:       md.ID.Tenant.AccountID,
		WorkspaceID:     md.ID.Tenant.EnvID,
		CreatedAt:       time.Now(),
		FunctionID:      md.ID.FunctionID,
		FunctionVersion: int64(md.Config.FunctionVersion),
		GroupID:         groupID,
		RunID:           md.ID.RunID,
		Type:            enums.HistoryTypeFunctionScheduled.String(),
		Attempt:         int64(item.Attempt),
		IdempotencyKey:  md.IdempotencyKey(),
		EventID:         md.Config.EventID(),
		BatchID:         md.Config.BatchID,
	}
	for _, d := range l.drivers {
		if err := d.Write(context.WithoutCancel(ctx), h); err != nil {
			l.log.Error("execution lifecycle error", "lifecycle", "onFunctionScheduled", "error", err)
		}
	}
}

// OnFunctionStarted is called when the function starts.  This may be
// immediately after the function is scheduled, or in the case of increased
// latency (eg. due to debouncing or concurrency limits) some time after the
// function is scheduled.
func (l lifecycle) OnFunctionStarted(
	ctx context.Context,
	md sv2.Metadata,
	item queue.Item,
	_ []json.RawMessage,
) {
	groupID, err := toUUID(item.GroupID)
	if err != nil {
		l.log.Error(
			"error parsing group ID",
			"error", err,
			"group_id", item.GroupID,
			"run_id", md.ID.RunID.String(),
		)
	}

	latency, _ := redis_state.GetItemSystemLatency(ctx)
	latencyMS := latency.Milliseconds()

	h := History{
		ID:              ulid.MustNew(ulid.Now(), rand.Reader),
		Cron:            md.Config.CronSchedule(),
		AccountID:       md.ID.Tenant.AccountID,
		WorkspaceID:     md.ID.Tenant.EnvID,
		CreatedAt:       time.Now(),
		FunctionID:      md.ID.FunctionID,
		FunctionVersion: int64(md.Config.FunctionVersion),
		GroupID:         groupID,
		RunID:           md.ID.RunID,
		Type:            enums.HistoryTypeFunctionStarted.String(),
		Attempt:         int64(item.Attempt),
		IdempotencyKey:  md.IdempotencyKey(),
		EventID:         md.Config.EventID(),
		LatencyMS:       &latencyMS,
		BatchID:         md.Config.BatchID,
	}
	for _, d := range l.drivers {
		if err := d.Write(context.WithoutCancel(ctx), h); err != nil {
			l.log.Error("execution lifecycle error", "lifecycle", "onFunctionStarted", "error", err)
		}
	}
}

// OnFunctionSkipped is called when a function run is skipped.
func (l lifecycle) OnFunctionSkipped(
	ctx context.Context,
	md sv2.Metadata,
	s execution.SkipState,
) {
	h := History{
		AccountID:   md.ID.Tenant.AccountID,
		BatchID:     md.Config.BatchID,
		Cron:        s.CronSchedule,
		EventID:     md.Config.EventID(),
		RunID:       md.ID.RunID,
		CreatedAt:   time.Now(),
		SkipReason:  &s.Reason,
		FunctionID:  md.ID.FunctionID,
		WorkspaceID: md.ID.Tenant.EnvID,
		Type:        enums.HistoryTypeFunctionSkipped.String(),
	}

	for _, d := range l.drivers {
		if err := d.Write(context.WithoutCancel(ctx), h); err != nil {
			l.log.Error("execution lifecycle error", "lifecycle", "onFunctionSkipped", "error", err)
		}
	}
}

// OnFunctionFinished is called when a function finishes.  This will
// be called when a function completes successfully or permanently failed,
// with the final driver response indicating the type of success.
//
// If failed, DriverResponse will contain a non nil Err string.
func (l lifecycle) OnFunctionFinished(
	ctx context.Context,
	md sv2.Metadata,
	item queue.Item,
	_ []json.RawMessage,
	resp state.DriverResponse,
) {
	groupID, err := toUUID(item.GroupID)
	if err != nil {
		l.log.Error(
			"error parsing group ID",
			"error", err,
			"group_id", item.GroupID,
			"run_id", md.ID.RunID.String(),
		)
	}

	completedStepCount := int64(md.Metrics.StepCount)

	h := History{
		Cron:               md.Config.CronSchedule(),
		ID:                 ulid.MustNew(ulid.Now(), rand.Reader),
		AccountID:          md.ID.Tenant.AccountID,
		WorkspaceID:        md.ID.Tenant.EnvID,
		CompletedStepCount: &completedStepCount,
		CreatedAt:          time.Now(),
		FunctionID:         md.ID.FunctionID,
		FunctionVersion:    int64(md.Config.FunctionVersion),
		RunID:              md.ID.RunID,
		GroupID:            groupID,
		Type:               enums.HistoryTypeFunctionCompleted.String(),
		Attempt:            int64(item.Attempt),
		IdempotencyKey:     md.IdempotencyKey(),
		EventID:            md.Config.EventID(),
		BatchID:            md.Config.BatchID,
	}

	err = applyResponse(&h, &resp)
	if err != nil {
		// Swallow error and log, since we don't want a response parsing error
		// to fail history writing.
		l.log.Error(
			"error applying response to history",
			"error", err,
			"run_id", md.ID.RunID.String(),
		)
	}

	if resp.Err != nil {
		h.Type = enums.HistoryTypeFunctionFailed.String()
	}
	for _, d := range l.drivers {
		if err := d.Write(context.WithoutCancel(ctx), h); err != nil {
			l.log.Error("execution lifecycle error", "lifecycle", "onFunctionFinished", "error", err)
		}
	}
}

// OnFunctionCancelled is called when a function is cancelled.  This includes
// the cancellation request, detailing either the event that cancelled the
// function or the API request information.
func (l lifecycle) OnFunctionCancelled(
	ctx context.Context,
	md sv2.Metadata,
	req execution.CancelRequest,
	evts []json.RawMessage,
) {
	completedStepCount := int64(md.Metrics.StepCount)
	groupID := uuid.New()

	h := History{
		Cron:               md.Config.CronSchedule(),
		ID:                 ulid.MustNew(ulid.Now(), rand.Reader),
		AccountID:          md.ID.Tenant.AccountID,
		WorkspaceID:        md.ID.Tenant.EnvID,
		CompletedStepCount: &completedStepCount,
		CreatedAt:          time.Now(),
		FunctionID:         md.ID.FunctionID,
		FunctionVersion:    int64(md.Config.FunctionVersion),
		GroupID:            &groupID,
		RunID:              md.ID.RunID,
		Type:               enums.HistoryTypeFunctionCancelled.String(),
		IdempotencyKey:     md.IdempotencyKey(),
		EventID:            md.Config.EventID(),
		Cancel:             &req,
		BatchID:            md.Config.BatchID,
	}
	for _, d := range l.drivers {
		if err := d.Write(context.WithoutCancel(ctx), h); err != nil {
			l.log.Error("execution lifecycle error", "lifecycle", "onFunctionCancelled", "error", err)
		}
	}
}

// OnStepScheduled is called when a new step is scheduled.  It contains the
// queue item which embeds the next step information.
func (l lifecycle) OnStepScheduled(
	ctx context.Context,
	md sv2.Metadata,
	item queue.Item,
	stepName *string,
) {
	edge, _ := queue.GetEdge(item)
	if edge == nil {
		return
	}

	groupID, err := toUUID(item.GroupID)
	if err != nil {
		l.log.Error(
			"error parsing group ID",
			"error", err,
			"group_id", item.GroupID,
			"run_id", md.ID.RunID.String(),
		)
	}

	h := History{
		ID:              ulid.MustNew(ulid.Now(), rand.Reader),
		AccountID:       md.ID.Tenant.AccountID,
		WorkspaceID:     md.ID.Tenant.EnvID,
		CreatedAt:       time.Now(),
		FunctionID:      md.ID.FunctionID,
		FunctionVersion: int64(md.Config.FunctionVersion),
		GroupID:         groupID,
		RunID:           md.ID.RunID,
		Type:            enums.HistoryTypeStepScheduled.String(),
		Attempt:         int64(item.Attempt),
		IdempotencyKey:  md.IdempotencyKey(),
		EventID:         md.Config.EventID(),
		StepName:        stepName,
		StepID:          &edge.Edge.Incoming, // TODO: Add step name to edge.
		BatchID:         md.Config.BatchID,
	}
	for _, d := range l.drivers {
		if err := d.Write(context.WithoutCancel(ctx), h); err != nil {
			l.log.Error("execution lifecycle error", "lifecycle", "onStepScheduled", "error", err)
		}
	}
}

func (l lifecycle) OnStepStarted(
	ctx context.Context,
	md sv2.Metadata,
	item queue.Item,
	edge inngest.Edge,
	url string,
) {
	groupID, err := toUUID(item.GroupID)
	if err != nil {
		l.log.Error(
			"error parsing group ID",
			"error", err,
			"group_id", item.GroupID,
			"run_id", md.ID.RunID.String(),
		)
	}

	latency, _ := redis_state.GetItemSystemLatency(ctx)
	latencyMS := latency.Milliseconds()

	h := History{
		ID:              ulid.MustNew(ulid.Now(), rand.Reader),
		AccountID:       md.ID.Tenant.AccountID,
		WorkspaceID:     md.ID.Tenant.EnvID,
		CreatedAt:       time.Now(),
		FunctionID:      md.ID.FunctionID,
		FunctionVersion: int64(md.Config.FunctionVersion),
		GroupID:         groupID,
		RunID:           md.ID.RunID,
		Type:            enums.HistoryTypeStepStarted.String(),
		Attempt:         int64(item.Attempt),
		IdempotencyKey:  md.IdempotencyKey(),
		EventID:         md.Config.EventID(),
		StepName:        &edge.Incoming,
		StepID:          &edge.Incoming, // TODO: Add step name to edge.
		URL:             &url,
		LatencyMS:       &latencyMS,
	}

	for _, d := range l.drivers {
		if err := d.Write(context.WithoutCancel(ctx), h); err != nil {
			l.log.Error("execution lifecycle error", "lifecycle", "onStepStarted", "error", err)
		}
	}
}

func (l lifecycle) OnStepFinished(
	ctx context.Context,
	md sv2.Metadata,
	item queue.Item,
	edge inngest.Edge,
	resp *state.DriverResponse,
	runErr error,
) {
	// TODO: should this be handled differently?
	if runErr != nil {
		return
	}

	groupID, err := toUUID(item.GroupID)
	if err != nil {
		l.log.Error(
			"error parsing group ID",
			"error", err,
			"group_id", item.GroupID,
			"run_id", md.ID.RunID.String(),
		)
	}

	h := History{
		ID:              ulid.MustNew(ulid.Now(), rand.Reader),
		AccountID:       md.ID.Tenant.AccountID,
		WorkspaceID:     md.ID.Tenant.EnvID,
		CreatedAt:       time.Now(),
		FunctionID:      md.ID.FunctionID,
		FunctionVersion: int64(md.Config.FunctionVersion),
		RunID:           md.ID.RunID,
		GroupID:         groupID,
		Type:            enums.HistoryTypeStepCompleted.String(),
		Attempt:         int64(item.Attempt),
		IdempotencyKey:  md.IdempotencyKey(),
		EventID:         md.Config.EventID(),
		StepName:        &resp.Step.Name,
		StepID:          &edge.Incoming,
		URL:             &resp.Step.URI,
		BatchID:         md.Config.BatchID,
	}

	err = applyResponse(&h, resp)
	if err != nil {
		// Swallow error and log, since we don't want a response parsing error
		// to fail history writing.
		l.log.Error(
			"error applying response to history",
			"error", err,
			"run_id", md.ID.RunID.String(),
		)
	}

	if h.Result != nil {
		h.Result.Headers = resp.Header

		if resp.SDK != "" {
			parts := strings.Split(resp.SDK, ":")
			if len(parts) == 2 {
				// Trim prefix because the TS SDK sends "inngest-js:vX.X.X"
				h.Result.SDKLanguage = strings.TrimPrefix(parts[0], "inngest-")

				h.Result.SDKVersion = parts[1]
			} else {
				l.log.Warn(
					"invalid SDK version",
					"sdk", resp.SDK,
				)
			}
		}
	}

	if resp.Err != nil && resp.Retryable() {
		h.Type = enums.HistoryTypeStepErrored.String()
	}
	if resp.Err != nil && !resp.Retryable() {
		h.Type = enums.HistoryTypeStepFailed.String()
	}

	if len(resp.Generator) == 1 && resp.Generator[0].Op == enums.OpcodeStepError {
		h.Type = enums.HistoryTypeStepErrored.String()
		if resp.NoRetry {
			h.Type = enums.HistoryTypeStepFailed.String()
		}
	}

	for _, d := range l.drivers {
		if err := d.Write(context.WithoutCancel(ctx), h); err != nil {
			l.log.Error("execution lifecycle error", "lifecycle", "onStepFinished", "error", err)
		}
	}
}

func (l lifecycle) OnStepGatewayRequestFinished(
	context.Context,
	sv2.Metadata,
	queue.Item,
	inngest.Edge,
	state.GeneratorOpcode,
	// Resp is the HTTP response
	*http.Response,
	*state.UserError,
) {
	// XXX: The tracing lifecycle has taken place over this, and this is not used.
}

func (l lifecycle) OnWaitForEvent(
	ctx context.Context,
	md sv2.Metadata,
	item queue.Item,
	op state.GeneratorOpcode,
	_ state.Pause,
) {
	groupID, err := toUUID(item.GroupID)
	if err != nil {
		l.log.Error(
			"error parsing group ID",
			"error", err,
			"group_id", item.GroupID,
			"run_id", md.ID.RunID.String(),
		)
	}

	opts, _ := op.WaitForEventOpts()
	expires, _ := opts.Expires()
	stepName := op.UserDefinedName()
	// nothing right now.
	h := History{
		ID:              ulid.MustNew(ulid.Now(), rand.Reader),
		AccountID:       md.ID.Tenant.AccountID,
		WorkspaceID:     md.ID.Tenant.EnvID,
		CreatedAt:       time.Now(),
		FunctionID:      md.ID.FunctionID,
		FunctionVersion: int64(md.Config.FunctionVersion),
		GroupID:         groupID,
		RunID:           md.ID.RunID,
		Type:            enums.HistoryTypeStepWaiting.String(),
		Attempt:         int64(item.Attempt),
		IdempotencyKey:  md.IdempotencyKey(),
		EventID:         md.Config.EventID(),
		StepName:        &stepName,
		StepID:          &op.ID,
		WaitForEvent: &WaitForEvent{
			EventName:  opts.Event,
			Expression: opts.If,
			Timeout:    expires,
		},
		BatchID: md.Config.BatchID,
	}
	for _, d := range l.drivers {
		if err := d.Write(context.WithoutCancel(ctx), h); err != nil {
			l.log.Error("execution lifecycle error", "lifecycle", "onWaitForEvent", "error", err)
		}
	}
}

// OnWaitForEventResumed is called when a function is resumed from waiting for
// an event.
func (l lifecycle) OnWaitForEventResumed(
	ctx context.Context,
	md sv2.Metadata,
	pause state.Pause,
	req execution.ResumeRequest,
) {
	var groupIDUUID *uuid.UUID
	groupID := pause.GroupID
	if groupID != "" {
		val, err := toUUID(groupID)
		if err != nil {
			l.log.Error(
				"error parsing group ID",
				"error", err,
				"group_id", groupID,
				"run_id", md.ID.RunID.String(),
			)
		}
		groupIDUUID = val
	}

	var stepName *string
	if req.StepName != "" {
		stepName = &req.StepName
	}

	h := History{
		AccountID:       md.ID.Tenant.AccountID,
		WorkspaceID:     md.ID.Tenant.EnvID,
		CreatedAt:       time.Now(),
		FunctionID:      md.ID.FunctionID,
		FunctionVersion: int64(md.Config.FunctionVersion),
		GroupID:         groupIDUUID,
		ID:              ulid.MustNew(ulid.Now(), rand.Reader),
		RunID:           md.ID.RunID,
		Type:            enums.HistoryTypeStepCompleted.String(),
		IdempotencyKey:  md.IdempotencyKey(),
		EventID:         md.Config.EventID(),
		WaitResult: &WaitResult{
			EventID: req.EventID,
			Timeout: req.EventID == nil,
		},
		BatchID:  md.Config.BatchID,
		StepName: stepName,
	}
	for _, d := range l.drivers {
		if err := d.Write(context.WithoutCancel(ctx), h); err != nil {
			l.log.Error("execution lifecycle error", "lifecycle", "onWaitForEventResumed", "error", err)
		}
	}
}

// OnInvokeFunction is called when a function is invoked from a step.
func (l lifecycle) OnInvokeFunction(
	ctx context.Context,
	md sv2.Metadata,
	item queue.Item,
	op state.GeneratorOpcode,
	invocationEvt event.Event,
) {
	eventID := ulid.MustParse(invocationEvt.ID)
	meta, err := invocationEvt.InngestMetadata()
	if err != nil {
		logger.StdlibLogger(ctx).Error("error retrieving inngest metadata", "error", err)
		return
	}
	corrID := meta.InvokeCorrelationId

	groupID, err := toUUID(item.GroupID)
	if err != nil {
		l.log.Error(
			"error parsing group ID",
			"error", err,
			"group_id", item.GroupID,
			"run_id", md.ID.RunID.String(),
		)
	}

	fnID := ""
	expiry := time.Time{}

	opts, err := op.InvokeFunctionOpts()
	if err != nil {
		l.log.Error("error parsing invoke function options", "error", err)
	}

	if opts != nil {
		fnID = opts.FunctionID
		optsExp, err := opts.Expires()
		if err != nil {
			l.log.Error("error parsing invoke function options expiry", "error", err)
		} else {
			expiry = optsExp
		}
	} else {
		l.log.Error("invoke function options are nil")
	}

	var invokeFunction *InvokeFunction
	// Not having all of the required data here indicates that something is
	// wrong; let's not add a partial history item for this. Either everything
	// or nothing, to ensure the reader doesn't have to do too much work.
	if corrID != "" && eventID.String() != "" && fnID != "" {
		invokeFunction = &InvokeFunction{
			CorrelationID: corrID,
			EventID:       eventID,
			FunctionID:    fnID,
			Timeout:       expiry,
		}
	}

	stepName := op.UserDefinedName()
	h := History{
		AccountID:       md.ID.Tenant.AccountID,
		Attempt:         int64(item.Attempt),
		CreatedAt:       time.Now(),
		EventID:         md.Config.EventID(),
		FunctionID:      md.ID.FunctionID,
		FunctionVersion: int64(md.Config.FunctionVersion),
		GroupID:         groupID,
		ID:              ulid.MustNew(ulid.Now(), rand.Reader),
		IdempotencyKey:  md.IdempotencyKey(),
		InvokeFunction:  invokeFunction,
		RunID:           md.ID.RunID,
		StepID:          &op.ID,
		StepName:        &stepName,
		Type:            enums.HistoryTypeStepInvoking.String(),
		WorkspaceID:     md.ID.Tenant.EnvID,
		BatchID:         md.Config.BatchID,
	}
	for _, d := range l.drivers {
		if err := d.Write(context.WithoutCancel(ctx), h); err != nil {
			l.log.Error("execution lifecycle error", "lifecycle", "onInvokeFunction", "error", err)
		}
	}
}

// OnInvokeFunctionResumed is called when a function is resumed from an
// invoke function step. This happens when the invoked function has
// completed or the step timed out whilst waiting.
func (l lifecycle) OnInvokeFunctionResumed(
	ctx context.Context,
	md sv2.Metadata,
	pause state.Pause,
	req execution.ResumeRequest,
) {
	var groupIDUUID *uuid.UUID
	groupID := pause.GroupID
	if groupID != "" {
		val, err := toUUID(groupID)
		if err != nil {
			l.log.Error(
				"error parsing group ID",
				"error", err,
				"group_id", groupID,
				"run_id", md.ID.RunID.String(),
			)
		}
		groupIDUUID = val
	}

	var stepName *string
	if req.StepName != "" {
		stepName = &req.StepName
	}

	h := History{
		AccountID:       md.ID.Tenant.AccountID,
		WorkspaceID:     md.ID.Tenant.EnvID,
		CreatedAt:       time.Now(),
		EventID:         md.Config.EventID(),
		FunctionID:      md.ID.FunctionID,
		FunctionVersion: int64(md.Config.FunctionVersion),
		GroupID:         groupIDUUID,
		ID:              ulid.MustNew(ulid.Now(), rand.Reader),
		IdempotencyKey:  md.IdempotencyKey(),
		InvokeFunctionResult: &InvokeFunctionResult{
			EventID: req.EventID,
			RunID:   req.RunID,
			Timeout: req.EventID == nil,
		},
		RunID:    md.ID.RunID,
		Type:     enums.HistoryTypeStepCompleted.String(),
		StepName: stepName,
		BatchID:  md.Config.BatchID,
	}

	if withErr := req.Error(); withErr != "" {
		h.Type = enums.HistoryTypeStepFailed.String()
		h.Result = &Result{
			Output: withErr,
		}
	} else if withData := req.Data(); withData != "" {
		h.Result = &Result{
			Output: withData,
		}
	}

	for _, d := range l.drivers {
		if err := d.Write(context.WithoutCancel(ctx), h); err != nil {
			l.log.Error("execution lifecycle error", "lifecycle", "onInvokeFunctionResumed", "error", err)
		}
	}
}

// OnSleep is called when a sleep step is scheduled.  The
// state.GeneratorOpcode contains the sleep details.
func (l lifecycle) OnSleep(
	ctx context.Context,
	md sv2.Metadata,
	item queue.Item,
	op state.GeneratorOpcode,
	until time.Time,
) {
	groupID, err := toUUID(item.GroupID)
	if err != nil {
		l.log.Error(
			"error parsing group ID",
			"error", err,
			"group_id", item.GroupID,
			"run_id", md.ID.RunID.String(),
		)
	}

	stepName := op.UserDefinedName()
	h := History{
		ID:              ulid.MustNew(ulid.Now(), rand.Reader),
		AccountID:       md.ID.Tenant.AccountID,
		WorkspaceID:     md.ID.Tenant.EnvID,
		FunctionID:      md.ID.FunctionID,
		FunctionVersion: int64(md.Config.FunctionVersion),
		RunID:           md.ID.RunID,
		CreatedAt:       time.Now(),
		GroupID:         groupID,
		Type:            enums.HistoryTypeStepSleeping.String(),
		Attempt:         int64(item.Attempt),
		IdempotencyKey:  md.IdempotencyKey(),
		EventID:         md.Config.EventID(),
		StepName:        &stepName,
		StepID:          &op.ID,
		Sleep: &Sleep{
			Until: until,
		},
		BatchID: md.Config.BatchID,
	}
	for _, d := range l.drivers {
		if err := d.Write(context.WithoutCancel(ctx), h); err != nil {
			l.log.Error("execution lifecycle error", "lifecycle", "onSleep", "error", err)
		}
	}
}

func applyResponse(
	h *History,
	resp *state.DriverResponse,
) error {
	h.Result = &Result{
		DurationMS: int(resp.Duration.Milliseconds()),
		RawOutput:  resp.Output,
		SizeBytes:  resp.OutputSize,
		// XXX: Add more fields here
	}

	// If it's a completed generator step then some data is stored in the
	// output. We'll try to extract it.
	if op := resp.HistoryVisibleStep(); op != nil {
		h.StepID = &op.ID
		h.StepType = getStepType(*op)
		h.Result.Output, _ = op.Output()
		stepName := op.UserDefinedName()
		h.StepName = &stepName
	}

	// Only set the output to the response error string if there isn't already output. This prevents overriding errors in the user's function
	if resp.Output == nil && resp.Error() != "" {
		h.Result.Output = resp.Error()
		h.Result.SizeBytes = len(h.Result.Output)
		return nil
	}

	if len(resp.GeneratorWithoutNone()) > 0 {
		// If we're a generator, exit now to prevent attempting to parse
		// generator response as an output; the generator response may be in
		// relation to many parallel steps, not just the one we're currently
		// writing history for.
		return nil
	}

	if outputStr, ok := resp.Output.(string); ok {
		// If it's a string and doesn't have extractable data, then
		// assume it's already the stringified JSON for the data
		// returned by the user's step. Some scenarios when that can
		// happen:
		// - FunctionCompleted. It isn't enveloped like generator steps.
		// - StepFailed. It has error-related fields.
		// - StepCompleted preceding parallel steps. Its output schema
		//     conforms to the normal generator StepCompleted schema,
		//     but doesn't contain any of the user's step output data.
		h.Result.Output = outputStr
		return nil
	}

	byt, err := json.Marshal(resp.Output)
	if err != nil {
		return fmt.Errorf("error marshalling step output: %w", err)
	}
	h.Result.Output = string(byt)
	return nil
}

func toUUID(id string) (*uuid.UUID, error) {
	if id == "" {
		return nil, nil
	}

	parsed, err := uuid.Parse(id)
	if err != nil {
		return nil, err
	}

	return &parsed, nil

}

// Returns the user-facing step type. In other words, the returned step type
// should match the code the user wrote (e.g. `step.sleep()` becomes
// enums.HistoryStepTypeSleep).
func getStepType(opcode state.GeneratorOpcode) *enums.HistoryStepType {
	var out enums.HistoryStepType
	switch opcode.Op {
	case enums.OpcodeSleep:
		out = enums.HistoryStepTypeSleep

	case enums.OpcodeStep, enums.OpcodeStepRun, enums.OpcodeStepError:
		// NOTE: enums.OpcodeStepError follows the same logic for determining
		// step types.
		if opcode.Data == nil && opcode.Error == nil {
			// Not a user-facing step.
			return nil
		}
		// This is a hacky way to detect `step.sendEvent()`, but it's all we
		// have until we add an opcode for it.
		if opcode.Name == "sendEvent" {
			out = enums.HistoryStepTypeSend
		} else {
			out = enums.HistoryStepTypeRun
		}
	case enums.OpcodeWaitForEvent:
		out = enums.HistoryStepTypeWait
	default:
		// Not a user-facing step.
		return nil
	}

	return &out
}
