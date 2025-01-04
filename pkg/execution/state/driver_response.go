package state

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/inngest"
)

const DefaultErrorName = "Error"
const DefaultErrorMessage = "Function execution error"
const DefaultStepErrorMessage = "Step execution error"

type Retryable interface {
	Retryable() bool
}

type UserError struct {
	Name    string `json:"name"`
	Message string `json:"message"`
	Stack   string `json:"stack,omitempty"`

	// Data allows for multiple return values in eg. Golang.  If provided,
	// the SDK MAY choose to store additional data for its own purposes here.
	Data json.RawMessage `json:"data,omitempty"`

	// NoRetry is set when parsing the opcode via the retry header.
	// It is NOT set via the SDK.
	NoRetry bool `json:"noRetry,omitempty"`

	// Cause allows nested errors to be passed back to the SDK.
	Cause *UserError `json:"cause,omitempty"`
}

// DriverResponse is returned after a driver executes an action.  This represents any
// output from running the step, including the output (as a JSON map), the error, and
// whether the driver's response is "scheduled", eg. the driver is running the job
// asynchronously.
//
// In asynchronous cases, we expect that the driver informs us of the response via
// an event in the future.
type DriverResponse struct {
	// Step represents the step that this response is for.
	Step inngest.Step `json:"step"`
	// Duration is how long the step took to run, from the driver itsef.
	Duration time.Duration `json:"dur"`
	// RequestVersion represents the hashing version used within the current SDK request.
	//
	// This allows us to store the hash version for each function run to check backcompat.
	RequestVersion int `json:"request_version"`
	// Generator indicates that this response is a partial repsonse from a
	// SDK-based step (generator) function.  These functions are invoked
	// multiple times with function state, and return a 206 Partial Content
	// with an opcode indicating the next action (eg. wait for event, run step,
	// sleep, etc.)
	//
	// The flow for an SDK-based step/generator function is:
	//
	//    1. Function runs.
	//    2. It hits a step.  The step immediately runs, and we return an
	//       opcode [consts.RanStep, "step name/data", { output }]
	//    3. We store this in the state, then continue to invoke the function
	//       with mutated state.  Each tool inside the function (step/wait)
	//       returns a new opcode which we store in step state.
	Generator []*GeneratorOpcode `json:"generator,omitempty"`
	// Output is the output from an action, as a JSON-marshalled value.
	Output any `json:"output"`
	// OutputSize is the size of the response payload, verbatim, in bytes.
	OutputSize int `json:"size"`

	// UserError indicates that the SDK ran and the step or function errored.
	//
	// This will be the value returned from OpcodeStepError or,
	// for older versions of the SDK or Function errors, a parsed
	// error from the response output.
	UserError *UserError `json:"userError,omitempty"`

	// Err represents a failing function: that the SDK wasn't hit, the SDK
	// catastrophically died (timeouts, OOM), or failed to execute top-level code.
	//
	// Step errors handled graceully always return OpcodeStepError and fill UserError.
	Err *string `json:"err"`
	// RetryAt is an optional retry at field, specifying when we should retry
	// the step if the step errored.
	RetryAt *time.Time `json:"retryAt,omitempty"`
	// Noretry, if true, indicates that we should never retry this step.
	NoRetry bool `json:"noRetry,omitempty"`
	// StatusCode represents the status code for the response.
	StatusCode int `json:"statusCode,omitempty"`
	// SDK represents the SDK language and version used for these
	// functions, in the format: "js:v0.1.0"
	SDK string `json:"sdk,omitempty"`

	// final indicates whether the error has been marked as final.  This occurs
	// when the response errors and the executor detects that this is the final
	// retry of the step.
	//
	// When final is true, Retryable() always returns false.
	final bool

	Header http.Header `json:"header,omitempty"`
}

// SetFinal indicates that this error is final, regardless of the status code
// returned.  This is used to prevent retries when the max limit is reached.
func (r *DriverResponse) SetFinal() {
	r.NoRetry = true
	r.final = true
}

// SetError sets the Err field to the string of the error specified.
func (r *DriverResponse) SetError(err error) {
	if err == nil {
		return
	}
	str := err.Error()
	r.Err = &str
}

// NextRetryAt fulfils the queue.RetryAtSpecifier interface
func (r DriverResponse) NextRetryAt() *time.Time {
	return r.RetryAt
}

// HasAI checks if any ops in the response are related to AI.
func (r DriverResponse) HasAI() bool {
	if r.Generator == nil {
		return false
	}

	for _, op := range r.Generator {
		if op.HasAI() {
			return true
		}
	}

	return false
}

func (r DriverResponse) Error() string {
	if r.Err == nil {
		return ""
	}
	return *r.Err
}

// Retryable returns whether the response indicates that the action is
// retryable.
//
// This is based of the action's output.  If the output contains a "status"
// field, we retry on any 5xx status; 4xx statuses are _not_ retried.  If the
// output contains no "status" field, we will always assume that we can retry
// the action.
//
// Note that responses where Err is nil are not retryable, and if Final() is
// set to true this response is also not retryable.
func (r DriverResponse) Retryable() bool {
	if r.Err == nil {
		// There's no error, so no need to retry
		return false
	}

	if r.NoRetry {
		// If there's a no retry flag set this is never retryable.
		return false
	}

	if r.final {
		// SetFinal has been called to ensure that this response is
		// never retried.
		return false
	}

	return true
}

// Final returns whether this response is final and the backing state store can
// record this step as finalized when recording the response.
//
// Only non-retryable errors should be marked as final;  successful responses will
// have their child edges evaluated and should be recorded as final once the next
// steps are enqueued.  This ensures that the number of scheduled and finalized steps
// in state only matches once the function ends.
//
// Final MUST exist as state stores need to push step IDs to the stack when the response
// is final.  We must do this prior to calling state.Finalized(), as the stack must be
// mutated prior to enqueuing steps.
func (r *DriverResponse) Final() bool {
	if r.final {
		return true
	}

	// If there's an error, return true if the error is not retryable.
	if r.Err != nil && !r.Retryable() {
		return true
	}

	return false
}

// HistoryVisibleStep returns a single generator op if this response is a
// generator containing only one op and should be visible in history, otherwise
// nil. This function should only be used in the context of StepCompleted, since
// other op codes should have visible StepScheduled, StepStarted, etc.
func (r *DriverResponse) HistoryVisibleStep() *GeneratorOpcode {
	if r.Generator == nil {
		return nil
	}

	// If multiple ops are being reported then we can't know which specific op
	// to return.
	if len(r.Generator) != 1 {
		return nil
	}

	op := r.Generator[0]

	// The other opcodes should not have visible StepCompleted history items.
	// For example OpcodeWaitForEvent should get a visible StepWaiting instead
	// of a visible StepCompleted.
	if op.Op != enums.OpcodeStep && op.Op != enums.OpcodeStepRun && op.Op != enums.OpcodeStepError {
		return nil
	}

	return op
}

// TraceVisibleStepExecution returns a single generator op if this response
// should be visible in a trace, otherwise nil. If returning nil, the response
// may still be considered to be a function response, in which case it likely
// also needs to be tracked.
func (r *DriverResponse) TraceVisibleStepExecution() *GeneratorOpcode {
	// If the response is not a generator, we received a response that was not
	// concerning a step.
	if r.Generator == nil {
		return nil
	}

	// If a response contains more than 1 operation, parallelism is enabled and
	// we are reporting multiple steps at once. We do not want to report this.
	if len(r.Generator) != 1 {
		return nil
	}

	op := r.Generator[0]

	// The planned step opcode is only used when we are in parallel; it's
	// possible for a single step to be planned during parallelism, so we
	// capture that here.
	if op.Op == enums.OpcodeStepPlanned {
		return nil
	}

	return op
}

// TraceVisibleFunctionExecution returns whether this response is a non-step
// response and should be visible in a trace.
func (r *DriverResponse) IsTraceVisibleFunctionExecution() bool {
	return r.StatusCode != 206
}

func (r *DriverResponse) UpdateOpcodeOutput(op *GeneratorOpcode, to json.RawMessage) {
	for n, o := range r.Generator {
		if o.ID != op.ID {
			continue
		}
		op.Data = to
		r.Generator[n].Data = to
	}
}

// UpdateOpcodeError updates an opcode's data and error to the given inputs.
func (r *DriverResponse) UpdateOpcodeError(op *GeneratorOpcode, err UserError) {
	for n, o := range r.Generator {
		if o.ID != op.ID {
			continue
		}
		op.Error = &err
		r.Generator[n].Error = &err
	}
}

// IsFunctionResult returns true if the response is a function result. It will
// return false if it's a step result.
func (r *DriverResponse) IsFunctionResult() bool {
	for _, op := range r.Generator {
		if op.Op != enums.OpcodeNone {
			return false
		}
	}
	return true
}

type WrappedStandardError struct {
	err error

	StandardError
}

func WrapInStandardError(err error, name string, message string, stack string) error {
	s := &WrappedStandardError{
		err: err,
		StandardError: StandardError{
			Name:    name,
			Message: message,
			Stack:   stack,
		},
	}
	s.StandardError.Error = s.Error()

	return s
}

func (s WrappedStandardError) Unwrap() error {
	return s.err
}

func (s WrappedStandardError) Error() string {
	return s.StandardError.String()
}

type StandardError struct {
	Error   string `json:"error"`
	Name    string `json:"name"`
	Message string `json:"message"`
	Stack   string `json:"stack,omitempty"`
}

func (s StandardError) String() string {
	return fmt.Sprintf("%s: %s", s.Name, s.Message)
}

func (s StandardError) Serialize(key string) string {
	// see ui/packages/components/src/utils/outputRenderer.ts:10

	data := map[string]any{
		key: s,
	}

	b, err := json.Marshal(data)
	if err != nil {
		// This should never happen.
		return s.String()
	}

	return string(b)
}

func (r *DriverResponse) StandardError() StandardError {
	ret := StandardError{
		Error:   DefaultErrorMessage,
		Name:    DefaultErrorName,
		Message: DefaultErrorMessage,
	}

	var raw map[string]any

	switch rawJson := r.Output.(type) {
	case json.RawMessage:
		// Try to unmarshal, but don't return on error, use raw map as fallback
		_ = json.Unmarshal(rawJson, &raw)
	case map[string]any:
		raw = rawJson
	default:
		// Handle other types by setting their value directly as a message
		switch v := r.Output.(type) {
		case []byte:
			if len(v) > 0 {
				raw = map[string]any{"message": string(v)}
			}
		case string:
			if len(v) > 0 {
				raw = map[string]any{"message": v}
			}
		case interface{}:
			if v != nil {
				raw = map[string]any{"message": v}
			}
		}
	}

	// Process the raw map if it's not empty
	if len(raw) > 0 {
		processed, _ := processErrorFields(raw)

		for _, key := range []string{"error", "name", "message", "stack"} {
			if val, ok := processed[key].(string); ok && val != "" {
				switch key {
				case "error":
					ret.Error = val
				case "name":
					ret.Name = val
				case "message":
					ret.Message = val
				case "stack":
					ret.Stack = val
				}
			}
		}
	}

	if r.Err != nil {
		if ret.Error == DefaultErrorMessage {
			ret.Error = *r.Err
		}
		if ret.Message == DefaultErrorMessage {
			ret.Message = *r.Err
		}
	}

	return ret
}

// processErrorFields looks for an error field then a body field to handle
// error messages from step responses.
func processErrorFields(input map[string]any) (map[string]any, error) {
	fields := []string{"error", "body"}
	for _, f := range fields {
		// Attempt to fetch the JS/SDK error from the body.
		switch v := input[f].(type) {
		case map[string]any:
			return v, nil
		case json.RawMessage:
			if mapped, err := processErrorString(string(v)); err == nil {
				return mapped, nil
			}
		case []byte:
			if mapped, err := processErrorString(string(v)); err == nil {
				return mapped, nil
			}
		case string:
			if mapped, err := processErrorString(v); err == nil {
				return mapped, nil
			}
		}
	}
	return input, nil
}

// processErrorString attempts to unquote and unmarshal a JSON-encoded string
func processErrorString(s string) (map[string]any, error) {
	// Bound inner error fields to 32kb
	if len(s) > 32*1024 {
		return nil, fmt.Errorf("error field too large")
	}

	if unquote, err := strconv.Unquote(s); err == nil {
		s = unquote
	}
	mapped := map[string]any{}
	err := json.Unmarshal([]byte(s), &mapped)
	return mapped, err
}
