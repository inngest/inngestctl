package step

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/execution/state"
	str2duration "github.com/xhit/go-str2duration/v2"
)

var (
	ErrEventNotReceived = fmt.Errorf("event not received")
)

type WaitForEventOpts struct {
	// Name represents the optional step name.
	Name string
	// Event is the event name to wait for.
	Event string
	// Timeout is how long to wait.  We must always timebound event lsiteners.
	Timeout time.Duration
	// If allows you to write arbitrary expressions to match against.
	If *string `json:"if"`
}

func WaitForEvent[T any](ctx context.Context, id string, opts WaitForEventOpts) (T, error) {
	mgr := preflight(ctx)
	args := map[string]any{
		"timeout": str2duration.String(opts.Timeout),
	}
	if opts.If != nil {
		args["if"] = *opts.If
	}
	if opts.Name == "" {
		opts.Name = id
	}

	op := mgr.NewOp(enums.OpcodeWaitForEvent, id, args)
	if val, ok := mgr.Step(op); ok {
		var output T
		if val == nil || bytes.Equal(val, []byte{0x6e, 0x75, 0x6c, 0x6c}) {
			return output, ErrEventNotReceived
		}
		if err := json.Unmarshal(val, &output); err != nil {
			mgr.SetErr(fmt.Errorf("error unmarshalling wait for event value in '%s': %w", opts.Event, err))
			panic(ControlHijack{})
		}
		return output, nil
	}

	mgr.AppendOp(state.GeneratorOpcode{
		ID:   op.MustHash(),
		Op:   op.Op,
		Name: opts.Name,
		Opts: op.Opts,
	})
	panic(ControlHijack{})
}
