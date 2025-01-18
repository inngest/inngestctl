package resolvers

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/99designs/gqlgen/graphql"
	"github.com/inngest/inngest/pkg/coreapi/graph/models"
	"github.com/inngest/inngest/pkg/cqrs"
	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/util"
	"github.com/oklog/ulid/v2"
)

const (
	defaultRunItems = 40
	maxRunItems     = 400
)

func (r *queryResolver) Runs(ctx context.Context, num int, cur *string, order []*models.RunsV2OrderBy, filter models.RunsFilterV2) (*models.RunsV2Connection, error) {
	opts := toRunsQueryOpt(num, cur, order, filter)
	runs, err := r.Data.GetTraceRuns(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("error retrieving runs: %w", err)
	}

	var (
		scursor *string
		ecursor *string
	)
	// eventID to run map
	evtRunMap := map[ulid.ULID][]*models.FunctionRunV2{}
	// used for retrieving eventIDs
	evtIDs := []ulid.ULID{}

	edges := []*models.FunctionRunV2Edge{}
	total := len(runs)
	for i, r := range runs {
		var (
			started   *time.Time
			ended     *time.Time
			sourceID  *string
			output    *string
			batchTime *time.Time
		)

		c := r.Cursor
		if i == 0 {
			scursor = &c // start cursor
		}
		if i == total-1 {
			ecursor = &c // end cursor
		}

		if r.StartedAt.UnixMilli() > 0 {
			started = &r.StartedAt
		}
		if r.EndedAt.UnixMilli() > 0 {
			ended = &r.EndedAt
		}
		if len(r.SourceID) > 0 {
			sourceID = &r.SourceID
		}
		if len(r.Output) > 0 {
			s := string(r.Output)
			output = &s
		}

		runID := ulid.MustParse(r.RunID)
		status, err := models.ToFunctionRunStatus(r.Status)
		if err != nil {
			continue
		}

		if r.BatchID != nil {
			ts := ulid.Time(r.BatchID.Time())
			batchTime = &ts
		}

		node := &models.FunctionRunV2{
			ID:             runID,
			AppID:          r.AppID,
			FunctionID:     r.FunctionID,
			TraceID:        r.TraceID,
			QueuedAt:       r.QueuedAt,
			StartedAt:      started,
			EndedAt:        ended,
			SourceID:       sourceID,
			Status:         status,
			Output:         output,
			IsBatch:        r.IsBatch,
			BatchCreatedAt: batchTime,
			CronSchedule:   r.CronSchedule,
			HasAi:          r.HasAI,
		}

		triggerIDS := []ulid.ULID{}
		for _, tid := range r.TriggerIDs {
			if id, err := ulid.Parse(tid); err == nil {
				triggerIDS = append(triggerIDS, id)

				// track evtID only if it's not batch nor cron
				if !r.IsBatch && r.CronSchedule == nil {
					if _, ok := evtRunMap[id]; !ok {
						evtRunMap[id] = []*models.FunctionRunV2{}
					}
					evtRunMap[id] = append(evtRunMap[id], node)
					evtIDs = append(evtIDs, id)
				}
			}
		}

		node.TriggerIDs = triggerIDS

		edges = append(edges, &models.FunctionRunV2Edge{
			Node:   node,
			Cursor: r.Cursor,
		})
	}

	evts, err := r.Data.GetEventsByInternalIDs(ctx, evtIDs)
	if err != nil {
		return nil, fmt.Errorf("error retrieving events associated with runs: %w", err)
	}
	var wg sync.WaitGroup
	for _, e := range evts {
		wg.Add(1)
		go func(evt *cqrs.Event) {
			defer wg.Done()

			runs, ok := evtRunMap[evt.GetInternalID()]
			if !ok {
				return
			}
			for _, run := range runs {
				run.EventName = &evt.EventName
			}
		}(e)
	}
	wg.Wait()

	pageInfo := &models.PageInfo{
		HasNextPage: total == int(opts.Items),
		StartCursor: scursor,
		EndCursor:   ecursor,
	}

	return &models.RunsV2Connection{
		Edges:    edges,
		PageInfo: pageInfo,
	}, nil
}

func (r *queryResolver) Run(ctx context.Context, runID string) (*models.FunctionRunV2, error) {
	runid, err := ulid.Parse(runID)
	if err != nil {
		return nil, fmt.Errorf("error parsing runID: %w", err)
	}

	run, err := r.Data.GetTraceRun(ctx, cqrs.TraceRunIdentifier{RunID: runid})
	if err != nil {
		return nil, fmt.Errorf("error retrieving run: %w", err)
	}

	var (
		startedAt *time.Time
		endedAt   *time.Time
		sourceID  *string
		output    *string
		batchTS   *time.Time
	)

	triggerIDs := []ulid.ULID{}
	for _, evtID := range run.TriggerIDs {
		if id, err := ulid.Parse(evtID); err == nil {
			triggerIDs = append(triggerIDs, id)
		}
	}

	if len(run.Output) > 0 {
		o := string(run.Output)
		output = &o
	}

	if run.BatchID != nil {
		ts := ulid.Time(run.BatchID.Time())
		batchTS = &ts
	}

	status, err := models.ToFunctionRunStatus(run.Status)
	if err != nil {
		return nil, fmt.Errorf("error parsing status: %w", err)
	}

	if run.StartedAt.UnixMilli() > 0 {
		startedAt = &run.StartedAt
	}
	if run.SourceID != "" {
		sourceID = &run.SourceID
	}

	switch status {
	case models.FunctionRunStatusCompleted, models.FunctionRunStatusFailed, models.FunctionRunStatusCancelled:
		if run.EndedAt.UnixMilli() > 0 {
			endedAt = &run.EndedAt
		}
	}

	res := models.FunctionRunV2{
		ID:             runid,
		AppID:          run.AppID,
		FunctionID:     run.FunctionID,
		TraceID:        run.TraceID,
		QueuedAt:       run.QueuedAt,
		StartedAt:      startedAt,
		EndedAt:        endedAt,
		Status:         status,
		SourceID:       sourceID,
		TriggerIDs:     triggerIDs,
		IsBatch:        run.IsBatch,
		BatchCreatedAt: batchTS,
		CronSchedule:   run.CronSchedule,
		Output:         output,
	}

	return &res, nil
}

func (r *queryResolver) RunTraceSpanOutputByID(ctx context.Context, outputID string) (*models.RunTraceSpanOutput, error) {
	id := &cqrs.SpanIdentifier{}
	if err := id.Decode(outputID); err != nil {
		return nil, fmt.Errorf("error parsing span identifier: %w", err)
	}

	spanData, err := r.Data.GetSpanOutput(ctx, *id)
	if err != nil {
		return nil, err
	}

	resp := models.RunTraceSpanOutput{}
	if spanData.IsError {
		stepErr, err := models.UnmarshalStepError(spanData.Data)
		if err != nil {
			stepErr = &models.StepError{
				Name:  util.ToPtr("Error"),
				Stack: util.ToPtr(string(spanData.Data)),
			}
		}

		resp.Error = stepErr
	} else {
		d := string(spanData.Data)
		resp.Data = &d
	}

	if len(spanData.Input) > 0 {
		input := string(spanData.Input)
		resp.Input = &input
	}

	return &resp, nil
}

func (r *queryResolver) RunTrigger(ctx context.Context, runID string) (*models.RunTraceTrigger, error) {
	runid, err := ulid.Parse(runID)
	if err != nil {
		return nil, fmt.Errorf("error parsing runID: %w", err)
	}

	run, err := r.Data.GetTraceRun(ctx, cqrs.TraceRunIdentifier{RunID: runid})
	if err != nil {
		return nil, fmt.Errorf("error retrieving run: %w", err)
	}

	var (
		evtName *string
		ts      time.Time
	)

	evtIDs := []ulid.ULID{}
	for _, id := range run.TriggerIDs {
		if evtID, err := ulid.Parse(id); err == nil {
			evtIDs = append(evtIDs, evtID)

			// use the earliest
			evtTime := ulid.Time(evtID.Time())
			if ts.IsZero() {
				ts = evtTime
			}
			if evtTime.Before(ts) {
				ts = evtTime
			}
		}
	}

	events, err := r.Data.GetEventsByInternalIDs(ctx, evtIDs)
	if err != nil {
		return nil, fmt.Errorf("error retrieving events: %w", err)
	}

	payloads := []string{}
	for _, evt := range events {
		byt, err := json.Marshal(evt.GetEvent())
		if err != nil {
			return nil, fmt.Errorf("error parsing event payload: %w", err)
		}
		payloads = append(payloads, string(byt))
	}

	// only parse event name if it's not cron
	if run.CronSchedule == nil {
		// just need the first one
		var name string
		for _, evt := range events {
			if name == "" {
				name = evt.EventName
			}
			// finish early if  it's not a batch and there's already a value
			if run.BatchID == nil && name != "" {
				break
			}

			// if there are multiple events and they are not identical,
			// set event name to nil
			if evt.EventName != name {
				name = ""
				break
			}
		}

		if name != "" {
			evtName = &name
		}
	}

	resp := models.RunTraceTrigger{
		EventName: evtName,
		Timestamp: ts,
		IDs:       evtIDs,
		Payloads:  payloads,
		BatchID:   run.BatchID,
		IsBatch:   run.BatchID != nil,
		Cron:      run.CronSchedule,
	}

	return &resp, nil
}

func (r *runsV2ConnResolver) TotalCount(ctx context.Context, obj *models.RunsV2Connection) (int, error) {
	cursor, ok := graphql.GetFieldContext(ctx).Parent.Args["after"].(*string)
	if !ok {
		return 0, fmt.Errorf("failed to access cursor")
	}

	orderBy, ok := graphql.GetFieldContext(ctx).Parent.Args["orderBy"].([]*models.RunsV2OrderBy)
	if !ok {
		return 0, fmt.Errorf("failed to retrieve order")
	}

	filter, ok := graphql.GetFieldContext(ctx).Parent.Args["filter"].(models.RunsFilterV2)
	if !ok {
		return 0, fmt.Errorf("failed to access query filter")
	}

	opts := toRunsQueryOpt(0, cursor, orderBy, filter)
	count, err := r.Data.GetTraceRunsCount(ctx, opts)
	if err != nil {
		return 0, fmt.Errorf("error retrieving count for runs: %w", err)
	}

	return count, nil
}

func toRunsQueryOpt(
	num int,
	cur *string,
	order []*models.RunsV2OrderBy,
	filter models.RunsFilterV2,
) cqrs.GetTraceRunOpt {
	tsfield := enums.TraceRunTimeQueuedAt
	switch *filter.TimeField {
	case models.RunsV2OrderByFieldStartedAt:
		tsfield = enums.TraceRunTimeStartedAt
	case models.RunsV2OrderByFieldEndedAt:
		tsfield = enums.TraceRunTimeEndedAt
	}

	statuses := []enums.RunStatus{}
	if len(filter.Status) > 0 {
		for _, s := range filter.Status {
			var status enums.RunStatus
			switch s {
			case models.FunctionRunStatusQueued:
				status = enums.RunStatusScheduled
			case models.FunctionRunStatusRunning:
				status = enums.RunStatusRunning
			case models.FunctionRunStatusCompleted:
				status = enums.RunStatusCompleted
			case models.FunctionRunStatusCancelled:
				status = enums.RunStatusCancelled
			case models.FunctionRunStatusFailed:
				status = enums.RunStatusFailed
			default:
				// unknown status
				continue
			}
			statuses = append(statuses, status)
		}
	}

	orderBy := []cqrs.GetTraceRunOrder{}
	for _, o := range order {
		var (
			field enums.TraceRunTime
			dir   enums.TraceRunOrder
		)

		switch o.Field {
		case models.RunsV2OrderByFieldQueuedAt:
			field = enums.TraceRunTimeQueuedAt
		case models.RunsV2OrderByFieldStartedAt:
			field = enums.TraceRunTimeStartedAt
		case models.RunsV2OrderByFieldEndedAt:
			field = enums.TraceRunTimeEndedAt
		default: // unknown, skip
			continue
		}

		switch o.Direction {
		case models.RunsOrderByDirectionAsc:
			dir = enums.TraceRunOrderAsc
		case models.RunsOrderByDirectionDesc:
			dir = enums.TraceRunOrderDesc
		default: // unknown, skip
			continue
		}

		orderBy = append(orderBy, cqrs.GetTraceRunOrder{Field: field, Direction: dir})
	}

	var cursor string
	if cur != nil {
		cursor = *cur
	}

	var cel string
	if filter.Query != nil {
		cel = *filter.Query
	}

	until := time.Now()
	if filter.Until != nil {
		until = *filter.Until
	}

	items := defaultRunItems
	if num > 0 && num < maxRunItems {
		items = num
	}

	return cqrs.GetTraceRunOpt{
		Filter: cqrs.GetTraceRunFilter{
			AppID:      filter.AppIDs,
			FunctionID: filter.FunctionIDs,
			TimeField:  tsfield,
			From:       filter.From,
			Until:      until,
			Status:     statuses,
			CEL:        cel,
		},
		Order:  orderBy,
		Cursor: cursor,
		Items:  uint(items),
	}
}
