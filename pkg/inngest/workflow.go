package inngest

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/inngest/inngest/pkg/consts"
	"github.com/inngest/inngest/pkg/expressions"
	"github.com/xhit/go-str2duration/v2"
)

const (
	TriggerName = "$trigger"
)

var (
	SourceEdge = Edge{
		Incoming: TriggerName,
	}
)

// Workflow represents a workflow encoded wtihin the Cue configuration language.
//
// This represents the logic for a workflow, but does not represent any specific
// workflow in the database.
type Workflow struct {
	// UUID is a surrogate key.
	UUID uuid.UUID `json:"-"`
	// Version represents the workflow version, if this is loaded from a
	// persistent store.
	Version int `json:"-"`
	// Concurrency indicates the total concurrency for this function.
	Concurrency int `json:"concurrency"`
	// ID is the immutable human identifier for the workflow.  This acts
	// similarly to a git repository name;  a single workflow ID can contain
	// many workflow versions.
	//
	// When deploying a specific workflow version we read the cue configuration
	// and upsert a version to the given ID.
	ID        string         `json:"id"`
	Name      string         `json:"name"`
	RateLimit *RateLimit     `json:"throttle,omitempty"`
	Triggers  []Trigger      `json:"triggers"`
	Steps     []WorkflowStep `json:"actions"`
	Edges     []Edge         `json:"edges"`
	Cancel    []Cancel       `json:"cancel,omitempty"`
}

// WorkflowStep is a reference to an action within a workflow.
type WorkflowStep struct {
	// ID is a string-based identifier for the step, used to reference
	// the step's output in code.
	ID string `json:"id"`
	// ClientID represnets an incrementing ID for the step.
	//
	// Deprecated:  use a string-based ID instead.
	ClientID uint                   `json:"clientID"`
	Name     string                 `json:"name"`
	DSN      string                 `json:"dsn"`
	Version  *VersionConstraint     `json:"version,omitempty"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
	Retries  *RetryOptions          `json:"retries,omitempty"`
	// Cancel specifies cancellation signals for the function
	Cancel []Cancel `json:"cancel,omitempty"`
}

func (ws WorkflowStep) Step() Step {
	s := Step{
		ID:   ws.ID,
		Name: ws.Name,
	}
	// TODO: URI
	if ws.Retries != nil {
		s.Retries = ws.Retries.Attempts
	}
	return s
}

// RetryCount returns the number of retries for this step.
func (s WorkflowStep) RetryCount() int {
	if s.Retries != nil && s.Retries.Attempts != nil {
		return *s.Retries.Attempts
	}
	return consts.DefaultRetryCount
}

type RateLimit struct {
	// Limit is how often the function can be called within the specified period
	Limit uint `json:"limit"`
	// Period represents the time period for throttling the function
	Period string `json:"period"`
	// Key is an optional string to constrain throttling using event data.  For
	// example, if you want to throttle incoming notifications based off of a user's
	// ID in an event you can use the following key: "{{ event.user.id }}".  This ensures
	// that we throttle functions for each user independently.
	Key *string `json:"key,omitempty"`
}

func (r RateLimit) IsValid(ctx context.Context) error {
	if r.Limit <= 0 {
		return errors.New("limit must be greater than 0")
	}

	if r.Key != nil {
		if err := expressions.Validate(ctx, *r.Key); err != nil {
			return fmt.Errorf("key is invalid: %w", err)
		}
	}

	if r.Period == "" {
		return errors.New("period must be specified")
	}
	dur, err := str2duration.ParseDuration(r.Period)
	if err != nil {
		return fmt.Errorf("failed to parse time duration: %w", err)
	}
	if dur > consts.FunctionIdempotencyPeriod {
		return fmt.Errorf("Rate limit period must be less than %s", consts.FunctionIdempotencyPeriod)
	}

	return nil
}

type Edge struct {
	// Incoming is the name of the step to run.  This is always the name of the
	// concrete step, even if we're running a generator.
	Incoming string `json:"incoming"`
	// StepPlanned is the ID of the generator step planned via enums.OpcodeStepPlanned.
	//
	// We cannot use "Incoming" here as the incoming name still needs to tbe the generator.
	IncomingGeneratorStep string `json:"gen,omitempty"`
	// Outgoing is the name of the generator step or step that last ran.
	Outgoing string `json:"outgoing"`
	// Metadata specifies the type of edge to use.  This defaults
	// to EdgeTypeEdge - a basic link that can conditionally run.
	Metadata *EdgeMetadata `json:"metadata,omitempty"`
}

func (e Edge) IsSource() bool {
	return e.Outgoing == "" && e.Incoming == TriggerName || e.Outgoing == TriggerName
}

type EdgeMetadata struct {
	Name string `json:"name,omitempty"`
	If   string `json:"if,omitempty"`
	// Wait specifies that the edge should only be traversed after the specified
	// duration.  This, in effect, allows you to delay jobs for a given amount of
	// time.
	Wait               *string `json:"wait,omitempty"`
	*AsyncEdgeMetadata `json:"async,omitempty"`
}

type AsyncEdgeMetadata struct {
	TTL string `json:"ttl"`
	// Event specifies the event name to listen for, which can coninue this workflow.
	Event string `json:"event"`
	// Match represents the optional expression to use when matching the event.
	// If specified, the event name must match and this expression must evaluate
	// to true for the workflow to continue.  This allows you to filter events
	// to eg. the same user.
	Match     *string `json:"match"`
	OnTimeout bool    `json:"onTimeout"`
}

// VersionCoinstraint represents version constraints for an action.  We use semver without
// patches:
//   - Major versions are backwards-incompatible (eg. requesting different secrets,
//     incompatible APIs).
//   - Minor versions are backwards compatible improvements, fixes, or additions.  We
//     automatically use the latest minor version within every step function.
type VersionConstraint struct {
	Major *uint `json:"major,omitempty"`
	Minor *uint `json:"minor,omitempty"`
}

// RetryOptions represents configuration for how to retry.
type RetryOptions struct {
	// Attempts is the maximum number of times to retry.
	Attempts *int `json:"attempts,omitempty"`
}
