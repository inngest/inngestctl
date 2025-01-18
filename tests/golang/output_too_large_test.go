package golang

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/inngest/inngestgo/step"
	"github.com/stretchr/testify/require"

	"github.com/inngest/inngest/tests/client"
	"github.com/inngest/inngestgo"
)

func TestOutputTooLarge(t *testing.T) {
	t.Run("step output too large", func(t *testing.T) {
		r := require.New(t)
		ctx := context.Background()

		c := client.New(t)
		h, server, registerFuncs := NewSDKHandler(t, "my-app")
		defer server.Close()

		eventName := "my-event"
		var runID string
		fn := inngestgo.CreateFunction(
			inngestgo.FunctionOpts{
				Name:    "my-fn",
				Retries: inngestgo.IntPtr(0),
			},
			inngestgo.EventTrigger(eventName, nil),
			func(ctx context.Context, input inngestgo.Input[FnRunTestEvt]) (any, error) {
				runID = input.InputCtx.RunID
				_, _ = step.Run(ctx, "a", func(ctx context.Context) (string, error) {
					// Return something that exceeds the max output size
					return strings.Repeat("a", 5*1024*1024), nil
				})
				return nil, nil
			},
		)

		h.Register(fn)
		registerFuncs()

		_, err := inngestgo.Send(
			ctx,
			inngestgo.Event{Name: eventName, Data: map[string]any{"foo": 1}},
		)
		r.NoError(err)

		var run client.Run
		r.Eventually(func() bool {
			if runID == "" {
				return false
			}
			run = c.Run(ctx, runID)
			return run.Status == "FAILED"
		}, 5*time.Second, 100*time.Millisecond)

		r.Equal(`"output_too_large"`, run.Output)
	})

	t.Run("function output too large", func(t *testing.T) {
		r := require.New(t)
		ctx := context.Background()

		c := client.New(t)
		h, server, registerFuncs := NewSDKHandler(t, "my-app")
		defer server.Close()

		eventName := "my-event"
		var runID string
		fn := inngestgo.CreateFunction(
			inngestgo.FunctionOpts{
				Name:    "my-fn",
				Retries: inngestgo.IntPtr(0),
			},
			inngestgo.EventTrigger(eventName, nil),
			func(ctx context.Context, input inngestgo.Input[FnRunTestEvt]) (any, error) {
				runID = input.InputCtx.RunID
				// Return something that exceeds the max output size
				return strings.Repeat("a", 5*1024*1024), nil
			},
		)

		h.Register(fn)
		registerFuncs()

		_, err := inngestgo.Send(
			ctx,
			inngestgo.Event{Name: eventName, Data: map[string]any{"foo": 1}},
		)
		r.NoError(err)

		var run client.Run
		r.Eventually(func() bool {
			if runID == "" {
				return false
			}
			run = c.Run(ctx, runID)
			return run.Status == "FAILED"
		}, 5*time.Second, 100*time.Millisecond)

		r.Equal(`"output_too_large"`, run.Output)
	})
}
