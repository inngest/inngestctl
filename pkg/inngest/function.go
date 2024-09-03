package inngest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	petname "github.com/dustinkirkland/golang-petname"
	"github.com/fatih/structs"
	"github.com/google/uuid"
	"github.com/gosimple/slug"
	multierror "github.com/hashicorp/go-multierror"
	"github.com/inngest/inngest/pkg/consts"
	"github.com/inngest/inngest/pkg/expressions"
	"github.com/inngest/inngest/pkg/syscode"
	"github.com/xhit/go-str2duration/v2"
)

const (
	DefaultStepName = "step-1"
)

// Function represents a step function which is triggered whenever an event
// is received or on a schedule.  In essence, it contains:
//
// - Triggers, which represent when a function is invoked
//
// - Steps, which represent the individual steps of actions that the function calls.
//
// A function may be simple (ie. only having a single step) or complex (ie. many
// steps).  Simple functions are easy:  run the single step's action.  Complex functions
// represent steps as a DAG, with edges between the trigger and each step.
type Function struct {
	// ConfigVersion represents the configuration version.  This lets us add or change
	// JSON definitions within functions when unmarshalling.
	ConfigVersion int `json:"cv,omitempty"`

	// ID is an internal surrogate key representing this function.
	ID uuid.UUID `json:"id"`

	// FunctionVersion represents the version of this specific function.  The same
	// function ID may be updated many times over the lifetime of a function; this
	// represents the specific version for the functon ID.
	FunctionVersion int `json:"fv"`

	// Name is the descriptive name for the function
	Name string `json:"name"`

	// Slug is the human-friendly ID for the function
	Slug string `json:"slug"`

	Priority *Priority `json:"priority,omitempty"`

	// Timeouts represents timeouts for a function.
	Timeouts *Timeouts `json:"timeouts,omitempty"`

	// ConcurrencyLimits allows limiting the concurrency of running functions, optionally constrained
	// by individual concurrency keys.
	//
	// Users may specify up to 2 concurrency keys.
	Concurrency *ConcurrencyLimits `json:"concurrency,omitempty"`

	Debounce *Debounce `json:"debounce,omitempty"`

	// Trigger represnets the trigger for the function.
	Triggers MultipleTriggers `json:"triggers"`

	// EventBatch determines how the function will process a list of incoming events
	EventBatch *EventBatchConfig `json:"batchEvents,omitempty"`

	// RateLimit allows specifying custom rate limiting for the function.  A RateLimit is
	// hard rate limiting:  any function invocations over the rate limit will be ignored and
	// will never run.
	RateLimit *RateLimit `json:"rateLimit,omitempty"`

	// Throttle represents a soft rate limit for gating function starts.  Any function runs
	// over the throttle period will be enqueued in the backlog to run at the next available
	// time.
	Throttle *Throttle `json:"throttle,omitempty"`

	// Cancel specifies cancellation signals for the function
	Cancel []Cancel `json:"cancel,omitempty"`

	// Actions represents the actions to take for this function.  If empty, this assumes
	// that we have a single action specified in the current directory using
	Steps []Step `json:"steps,omitempty"`

	// Edges represent edges between steps in the dag.
	Edges []Edge `json:"edges,omitempty"`
}

// DeterministicUUID returns a deterministic V3 UUID based off of the SHA1
// hash of the function's name.
func (f *Function) DeterministicUUID() uuid.UUID {
	return DeterministicSha1UUID(f.Name + f.Steps[0].URI)
}

// Throttle represents concurrency over time.
type Throttle struct {
	// Limit is how often the function can be called within the specified period.  The
	// minimum limit is 1.
	Limit uint `json:"limit"`
	// Period represents the time period for throttling the function.  The minimum
	// granularity is a single second.
	Period time.Duration `json:"period"`
	// Burst...
	Burst uint `json:"burst"`
	// Key is an optional string to constrain throttling using event data.  For
	// example, if you want to throttle incoming notifications based off of a user's
	// ID in an event you can use the following key: "{{ event.user.id }}".  This ensures
	// that we throttle functions for each user independently.
	Key *string `json:"key,omitempty"`
}

func (t *Throttle) UnmarshalJSON(in []byte) error {
	if t == nil {
		t = &Throttle{}
	}

	var err error
	input := struct {
		Limit  uint    `json:"limit"`
		Period string  `json:"period"`
		Burst  uint    `json:"burst"`
		Key    *string `json:"key,omitempty"`
	}{}
	if err = json.Unmarshal(in, &input); err != nil {
		return err
	}

	t.Limit = input.Limit
	t.Burst = input.Burst
	t.Key = input.Key
	t.Period, err = str2duration.ParseDuration(input.Period)

	// Normalization
	if t.Limit == 0 {
		t.Limit = 1
	}
	if t.Burst == 0 {
		t.Burst = 1
	}
	return err
}

func (t Throttle) MarshalJSON() ([]byte, error) {
	s := structs.New(t)
	s.TagName = "json"
	val := s.Map()
	// convert period to a string.
	val["period"] = str2duration.String(t.Period)
	return json.Marshal(val)
}

// Timeouts represents timeouts for the function. If any of the timeouts are hit, the function
// will be marked as cancelled with a cancellation reason.
type Timeouts struct {
	// Start represents the timeout for starting a function.  If the time between scheduling
	// and starting a function exceeds this value, the function will be cancelled.  Note that
	// this is inclusive of time between retries.
	//
	// A function may exceed this duration because of concurrency limits, throttling, etc.
	Start time.Duration `json:"start"`

	// Finish represents the time between a function starting and the function finishing.
	// If a function takes longer than this time to finish, the function is marked as cancelled.
	// The start time is taken from the time that the first successful function request begins,
	// and does not include the time spent in the queue before the function starts.
	//
	// Note that if the final request to a function begins before this timeout, and completes
	// after this timeout, the function will succeed.
	Finish time.Duration `json:"finish"`
}

type Priority struct {
	Run *string `json:"run"`
}

type Debounce struct {
	Key     *string `json:"key,omitempty"`
	Period  string  `json:"period"`
	Timeout *string `json:"timeout,omitempty"`
}

func (d Debounce) TimeoutDuration() *time.Duration {
	if d.Timeout == nil || *d.Timeout == "" {
		return nil
	}
	if dur, err := str2duration.ParseDuration(*d.Timeout); err == nil {
		return &dur
	}
	return nil
}

// Cancel represents a cancellation signal for a function.  When specified, this
// will set up pauses which automatically cancel the function based off of matching
// events and expressions.
type Cancel struct {
	Event   string  `json:"event"`
	Timeout *string `json:"timeout,omitempty"`
	If      *string `json:"if,omitempty"`
}

// ConcurrencyLimit returns the limit for the function itself, ie. the concurrnecy limit
// set without keys and scoped to the function.
func (f Function) ConcurrencyLimit() int {
	if f.Concurrency != nil {
		return f.Concurrency.PartitionConcurrency()
	}
	return 0
}

// GetSlug returns the function slug, defaulting to creating a slug of the function name.
func (f Function) GetSlug() string {
	if f.Slug != "" {
		return f.Slug
	}
	return strings.ToLower(slug.Make(f.Name))
}

func (f Function) IsScheduled() bool {
	for _, t := range f.Triggers {
		if t.CronTrigger != nil {
			return true
		}
	}
	return false
}

func (f Function) IsBatchEnabled() bool {
	if f.EventBatch == nil {
		return false
	}
	return f.EventBatch.IsEnabled()
}

// Validate returns an error if the function definition is invalid.
func (f Function) Validate(ctx context.Context) error {
	var err error
	if f.Name == "" {
		err = multierror.Append(err, fmt.Errorf("A function name is required"))
	}
	if len(f.Triggers) == 0 {
		err = multierror.Append(err, fmt.Errorf("At least one trigger is required"))
	}

	if f.Concurrency != nil {
		if cerr := f.Concurrency.Validate(ctx); cerr != nil {
			err = multierror.Append(err, cerr)
		}
	}

	if terr := f.Triggers.Validate(ctx); terr != nil {
		err = multierror.Append(err, terr)
	}

	if f.EventBatch != nil {
		if berr := f.EventBatch.IsValid(ctx); berr != nil {
			err = multierror.Append(err, berr)
		}

		if len(f.Cancel) > 0 {
			err = multierror.Append(err, syscode.Error{
				Code:    syscode.CodeComboUnsupported,
				Message: "Batching and cancellation are mutually exclusive",
			})
		}

		if f.Debounce != nil {
			err = multierror.Append(err, syscode.Error{
				Code:    syscode.CodeComboUnsupported,
				Message: "Batching and debouncing are mutually exclusive",
			})
		}
	}

	for _, step := range f.Steps {
		if step.Name == "" {
			err = multierror.Append(err, fmt.Errorf("All steps must have a name"))
		}
		uri, serr := url.Parse(step.URI)
		if serr != nil {
			err = multierror.Append(err, fmt.Errorf("Steps must have a valid URI"))
		}
		switch uri.Scheme {
		case "http", "https":
			continue
		default:
			err = multierror.Append(err, fmt.Errorf("Non-HTTP steps are not yet supported"))
		}
	}

	edges, aerr := f.AllEdges(ctx)
	if aerr != nil {
		return multierror.Append(err, aerr)
	}

	if len(f.Steps) != 1 {
		err = multierror.Append(err, fmt.Errorf("Functions must contain one step"))
	}

	// Validate edges.
	for _, edge := range edges {
		// Ensure that any expressions are also valid.
		if edge.Metadata == nil {
			continue
		}
		if edge.Metadata.If != "" {
			if _, verr := expressions.NewExpressionEvaluator(ctx, edge.Metadata.If); verr != nil {
				err = multierror.Append(err, verr)
			}
		}
		if edge.Metadata.Wait != nil {
			// Ensure that this is a valid duration or expression.
			if _, err := str2duration.ParseDuration(*edge.Metadata.Wait); err == nil {
				continue
			}
			if _, err := expressions.NewExpressionEvaluator(ctx, *edge.Metadata.Wait); err == nil {
				continue
			}
			err = multierror.Append(err, fmt.Errorf("Unable to parse wait as a duration or expression: %s", *edge.Metadata.Wait))
		}
	}

	// Validate priority expression
	if f.Priority != nil && f.Priority.Run != nil {
		if _, exprErr := expressions.NewExpressionEvaluator(ctx, *f.Priority.Run); exprErr != nil {
			err = multierror.Append(err, fmt.Errorf("Priority.Run expression is invalid: %s", exprErr))
		}
		// NOTE: Priority.Run is not valid when batch is enabled.
		if f.EventBatch != nil {
			err = multierror.Append(err, fmt.Errorf("A function cannot specify Priority.Run and Batch together"))
		}
	}

	// Validate cancellation expressions
	for _, c := range f.Cancel {
		if c.If != nil {
			if exprErr := expressions.Validate(ctx, *c.If); exprErr != nil {
				err = multierror.Append(err, fmt.Errorf("Cancellation expression is invalid: %s", exprErr))
			}
		}
	}

	if len(f.Cancel) > consts.MaxCancellations {
		err = multierror.Append(err, fmt.Errorf("This function exceeds the max number of cancellation events: %d", consts.MaxCancellations))
	}

	if f.Debounce != nil {
		if f.Debounce.Key != nil && *f.Debounce.Key == "" {
			// Some clients may send an empty string.
			f.Debounce.Key = nil
		}
		if f.Debounce.Key != nil {
			// Ensure the expression is valid if present.
			if exprErr := expressions.Validate(ctx, *f.Debounce.Key); exprErr != nil {
				err = multierror.Append(err, fmt.Errorf("Debounce expression is invalid: %s", exprErr))
			}
		}

		period, perr := str2duration.ParseDuration(f.Debounce.Period)
		if perr != nil {
			err = multierror.Append(err, fmt.Errorf("The debounce period of '%s' is invalid: %w", f.Debounce.Period, perr))
		}
		if period < consts.MinDebouncePeriod {
			err = multierror.Append(err, fmt.Errorf("The debounce period of '%s' is less than the min of: %s", f.Debounce.Period, consts.MinDebouncePeriod))
		}
		if period > consts.MaxDebouncePeriod {
			err = multierror.Append(err, fmt.Errorf("The debounce period of '%s' is greater than the max of: %s", f.Debounce.Period, consts.MaxDebouncePeriod))
		}
	}

	// Validate rate limit expression
	if f.RateLimit != nil {
		if rateLimitErr := f.RateLimit.IsValid(ctx); rateLimitErr != nil {
			err = multierror.Append(err, rateLimitErr)
		}
	}

	if f.Throttle != nil {
		if f.Throttle.Period > consts.FunctionIdempotencyPeriod {
			return fmt.Errorf("Throttle period must be less than %s", consts.FunctionIdempotencyPeriod)
		}
	}

	return err
}

// RunPriorityFactor returns the run priority factor for this function, given an input event.
func (f Function) RunPriorityFactor(ctx context.Context, event map[string]any) (int64, error) {
	if f.Priority == nil || f.Priority.Run == nil {
		return 0, nil
	}

	// Validate the expression first.
	if err := expressions.Validate(ctx, *f.Priority.Run); err != nil {
		return 0, fmt.Errorf("Priority.Run expression is invalid: %s", err)
	}

	expr, err := expressions.NewExpressionEvaluator(ctx, *f.Priority.Run)
	if err != nil {
		// This should never happen.
		return 0, fmt.Errorf("Priority.Run expression is invalid: %s", err)
	}

	val, _, err := expr.Evaluate(ctx, expressions.NewData(map[string]any{"event": event}))
	if err != nil {
		return 0, fmt.Errorf("Priority.Run expression errored: %s", err)
	}

	var result int64

	switch v := val.(type) {
	case int:
		result = int64(v)
	case int64:
		result = v
	default:
		return 0, fmt.Errorf("Priority.Run expression returned non-int: %v", val)
	}

	// Apply bounds
	if result > consts.PriorityFactorMax {
		return consts.PriorityFactorMax, nil
	}
	if result < consts.PriorityFactorMin {
		return consts.PriorityFactorMin, nil
	}

	return result, nil
}

// URI returns the function's URI.  It is expected that the function has already been
// validated.
func (f Function) URI() (*url.URL, error) {
	if len(f.Steps) >= 1 {
		return url.Parse(f.Steps[0].URI)
	}
	return nil, fmt.Errorf("No steps configured")
}

// AllEdges produces edge configuration for steps defined within the function.
// If no edges for a step exists, an automatic step from the tirgger is added.
func (f Function) AllEdges(ctx context.Context) ([]Edge, error) {
	edges := []Edge{}

	// O1 lookup of steps.
	stepmap := map[string]Step{}
	// Track whether incoming edges exist for each step
	seen := map[string]bool{}
	for _, s := range f.Steps {
		stepmap[s.ID] = s
		seen[s.ID] = false
	}

	var err error

	// Map all edges for incoming steps.
	for _, edge := range f.Edges {
		if _, ok := seen[edge.Incoming]; !ok {
			err = multierror.Append(
				err,
				fmt.Errorf("Step '%s' doesn't exist within edge", edge.Incoming),
			)
			continue
		}
		seen[edge.Incoming] = true
		edges = append(edges, edge)
	}

	// For all unseen edges, add a trigger edge.
	for step, ok := range seen {
		if ok {
			continue
		}
		edges = append(edges, Edge{
			Outgoing: TriggerName,
			Incoming: step,
		})
	}

	// Ensure that the edges are sorted by name, giving us
	// deterministic output.
	sort.SliceStable(edges, func(i, j int) bool {
		return edges[i].Outgoing < edges[j].Outgoing
	})
	return edges, nil
}

// DeterminsiticAppUUID returns a deterministic V3 UUID based off of the SHA1
// hash of the app's URL.
func DeterministicAppUUID(url string) uuid.UUID {
	return DeterministicSha1UUID(url)
}

// DeterministicSha1UUID returns a deterministic V3 UUID based off of the SHA1
// hash of the input string.
func DeterministicSha1UUID(str string) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(str))
}

func RandomID() (string, error) {
	// Generate a 6 character long hex string.  This is the suffix to
	// our DSN prefix, which decreases the chance of collosion by 1/16,777,216.
	// This makes the total chance of collisions from an _empty_ keyspace
	// 1 in 3,435,034,312,704 (we'll ignore birthday problems).
	byt := make([]byte, 3)
	if _, err := rand.Read(byt); err != nil {
		return "", fmt.Errorf("error generating ID: %w", err)
	}
	petname.NonDeterministicMode()
	return fmt.Sprintf("%s-%s", petname.Generate(2, "-"), hex.EncodeToString(byt)), nil
}
