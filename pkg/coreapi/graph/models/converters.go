package models

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/inngest/inngest/pkg/cqrs"
	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/logger"
)

func MakeFunction(f *cqrs.Function) (*Function, error) {
	fn, err := f.InngestFunction()
	if err != nil {
		return nil, err
	}

	triggers := make([]*FunctionTrigger, len(fn.Triggers))
	for n, t := range fn.Triggers {
		var (
			val string
			typ FunctionTriggerTypes
		)
		if t.CronTrigger != nil {
			typ = FunctionTriggerTypesCron
			val = t.Cron
		}
		if t.EventTrigger != nil {
			typ = FunctionTriggerTypesEvent
			val = t.Event
		}
		triggers[n] = &FunctionTrigger{
			Type:  typ,
			Value: val,
		}
	}

	concurrency := 0
	if fn.Concurrency != nil {
		concurrency = fn.Concurrency.PartitionConcurrency()
	}

	return &Function{
		AppID:       f.AppID.String(),
		ID:          f.ID.String(),
		Name:        f.Name,
		Slug:        f.Slug,
		Config:      string(f.Config),
		Concurrency: concurrency,
		Triggers:    triggers,
		URL:         fn.Steps[0].URI,
	}, nil
}

func MakeFunctionRun(f *cqrs.FunctionRun) *FunctionRun {
	status, err := ToFunctionRunStatus(f.Status)
	if err != nil {
		logger.StdlibLogger(context.Background()).
			Error(
				"unknown run status",
				"error", err,
				"status", f.Status.String(),
			)
	}

	// TODO: Map GQL types to CQRS types and remove this.
	r := &FunctionRun{
		ID:         f.RunID.String(),
		FunctionID: f.FunctionID.String(),
		FinishedAt: f.EndedAt,
		StartedAt:  &f.RunStartedAt,
		EventID:    f.EventID.String(),
		BatchID:    f.BatchID,
		Status:     &status,
		Cron:       f.Cron,
	}
	if len(f.Output) > 0 {
		str := string(f.Output)
		r.Output = &str
	}
	return r
}

func ToFunctionRunStatus(s enums.RunStatus) (FunctionRunStatus, error) {
	switch s {
	case enums.RunStatusScheduled:
		return FunctionRunStatusQueued, nil
	case enums.RunStatusRunning:
		return FunctionRunStatusRunning, nil
	case enums.RunStatusCompleted:
		return FunctionRunStatusCompleted, nil
	case enums.RunStatusFailed:
		return FunctionRunStatusFailed, nil
	case enums.RunStatusCancelled:
		return FunctionRunStatusCancelled, nil
	default:
		return FunctionRunStatusRunning, fmt.Errorf("unknown run status: %d", s)
	}
}

func UnmarshalStepError(data []byte) (*StepError, error) {
	var rawData map[string]any
	err := json.Unmarshal(data, &rawData)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	// Use reflection to find the struct fields.
	allowedFields := make(map[string]bool)
	t := reflect.TypeOf(StepError{})
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("json")
		if tag == "" {
			continue
		}

		// Handle cases like `json:"name,omitempty"`.
		name := strings.Split(tag, ",")[0]
		allowedFields[name] = true
	}

	// Error if there are any extra fields.
	for key := range rawData {
		if !allowedFields[key] {
			return nil, fmt.Errorf("unexpected field in JSON: %s", key)
		}
	}

	// Unmarshal into StepError struct
	var stepError StepError
	err = json.Unmarshal(data, &stepError)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal into StepError: %w", err)
	}

	return &stepError, nil
}
