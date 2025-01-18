package golang

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/inngest/inngest/pkg/consts"
	"github.com/inngest/inngest/pkg/coreapi/graph/models"
	"github.com/inngest/inngest/pkg/util"
	"github.com/inngest/inngest/tests/client"
	"github.com/inngest/inngestgo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFunctionFailure(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	c := client.New(t)
	h, server, registerFuncs := NewSDKHandler(t, "fnfail")
	defer server.Close()

	// Create our function.
	var (
		counter int32
		runID   string
	)
	fn := inngestgo.CreateFunction(
		inngestgo.FunctionOpts{
			Name:    "test sdk",
			Retries: inngestgo.IntPtr(0),
		},
		inngestgo.EventTrigger("failure/run", nil),
		func(ctx context.Context, input inngestgo.Input[FnRunTestEvt]) (any, error) {
			if runID == "" {
				runID = input.InputCtx.RunID
			}
			atomic.AddInt32(&counter, 1)
			return true, fmt.Errorf("nope!")
		},
	)
	h.Register(fn)

	// Register the fns via the test SDK harness above.
	registerFuncs()

	// Trigger the function.
	evt := inngestgo.Event{
		Name: "failure/run",
		Data: map[string]interface{}{
			"foo": "bar",
		},
	}
	_, err := inngestgo.Send(ctx, evt)
	require.NoError(t, err)

	<-time.After(5 * time.Second)

	require.EqualValues(t, counter, 1)

	t.Run("trace run should have appropriate data", func(t *testing.T) {
		run := c.WaitForRunTraces(ctx, t, &runID, client.WaitForRunTracesOptions{Status: models.FunctionStatusFailed, ChildSpanCount: 1})

		require.Equal(t, models.RunTraceSpanStatusFailed.String(), run.Trace.Status)
		// output test
		require.NotNil(t, run.Trace.OutputID)
		runOutput := c.RunSpanOutput(ctx, *run.Trace.OutputID)
		require.NotNil(t, runOutput)
		c.ExpectSpanErrorOutput(t, "", "nope!", runOutput)

		rootSpanID := run.Trace.SpanID

		t.Run("failed run", func(t *testing.T) {
			span := run.Trace.ChildSpans[0]
			assert.Equal(t, consts.OtelExecFnErr, span.Name)
			assert.False(t, span.IsRoot)
			assert.Equal(t, rootSpanID, span.ParentSpanID)
			assert.Equal(t, models.RunTraceSpanStatusFailed.String(), span.Status)
			assert.NotNil(t, span.OutputID)

			// output test
			output := c.RunSpanOutput(ctx, *span.OutputID)
			assert.NotNil(t, output)
			c.ExpectSpanErrorOutput(t, "", "nope!", output)
		})
	})
}

func TestFunctionFailureWithRetries(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	c := client.New(t)
	h, server, registerFuncs := NewSDKHandler(t, "fnfail-retry")
	defer server.Close()

	// Create our function.
	var (
		counter int32
		runID   string
	)
	fn := inngestgo.CreateFunction(
		inngestgo.FunctionOpts{
			Name:    "test sdk fail with retry",
			Retries: inngestgo.IntPtr(1),
		},
		inngestgo.EventTrigger("failure/run-retry", nil),
		func(ctx context.Context, input inngestgo.Input[FnRunTestEvt]) (any, error) {
			runID = input.InputCtx.RunID

			atomic.AddInt32(&counter, 1)
			return true, fmt.Errorf("nope!")
		},
	)
	h.Register(fn)

	// Register the fns via the test SDK harness above.
	registerFuncs()

	// Trigger the function.
	evt := inngestgo.Event{
		Name: "failure/run-retry",
		Data: map[string]interface{}{
			"foo": "bar",
		},
	}
	_, err := inngestgo.Send(ctx, evt)
	require.NoError(t, err)

	<-time.After(5 * time.Second)

	require.EqualValues(t, counter, 1)

	t.Run("in progress run", func(t *testing.T) {
		run := c.WaitForRunTraces(ctx, t, &runID, client.WaitForRunTracesOptions{Status: models.FunctionStatusRunning, ChildSpanCount: 1})

		require.Equal(t, models.RunTraceSpanStatusRunning.String(), run.Trace.Status)
		require.Nil(t, run.Trace.OutputID)

		rootSpanID := run.Trace.SpanID

		// test first attempt
		t.Run("attempt 1", func(t *testing.T) {
			span := run.Trace.ChildSpans[0]
			assert.Equal(t, "execute", span.Name)
			assert.False(t, span.IsRoot)
			assert.GreaterOrEqual(t, len(span.ChildSpans), 1)
			assert.Equal(t, rootSpanID, span.ParentSpanID)
			assert.Equal(t, models.RunTraceSpanStatusRunning.String(), span.Status)
			assert.Nil(t, span.OutputID)

			t.Run("failed", func(t *testing.T) {
				failed := span.ChildSpans[0]
				assert.Equal(t, "Attempt 0", failed.Name)
				assert.False(t, span.IsRoot)
				assert.Equal(t, models.RunTraceSpanStatusFailed.String(), failed.Status)

				// output test
				assert.NotNil(t, failed.OutputID)
				output := c.RunSpanOutput(ctx, *failed.OutputID)
				assert.NotNil(t, output)
				c.ExpectSpanErrorOutput(t, "", "nope!", output)
			})
		})
	})

	t.Run("trace run should have appropriate data", func(t *testing.T) {
		run := c.WaitForRunTraces(ctx, t, &runID, client.WaitForRunTracesOptions{Status: models.FunctionStatusFailed, Timeout: 1 * time.Minute, Interval: 5 * time.Second, ChildSpanCount: 1})

		require.Equal(t, models.RunTraceSpanStatusFailed.String(), run.Trace.Status)
		// output test
		require.NotNil(t, run.Trace.OutputID)
		runOutput := c.RunSpanOutput(ctx, *run.Trace.OutputID)
		c.ExpectSpanErrorOutput(t, "", "nope!", runOutput)

		rootSpanID := run.Trace.SpanID

		// first attempt
		t.Run("failed run", func(t *testing.T) {
			span := run.Trace.ChildSpans[0]
			assert.Equal(t, consts.OtelExecPlaceholder, span.Name)
			assert.False(t, span.IsRoot)
			assert.Equal(t, rootSpanID, span.ParentSpanID)
			assert.Equal(t, 2, len(span.ChildSpans))
			assert.Equal(t, 2, span.Attempts)
			assert.Equal(t, models.RunTraceSpanStatusFailed.String(), span.Status)
			assert.NotNil(t, span.OutputID)

			// output test
			output := c.RunSpanOutput(ctx, *span.OutputID)
			assert.NotNil(t, output)
			c.ExpectSpanErrorOutput(t, "", "nope!", output)

			t.Run("attempt 0", func(t *testing.T) {
				one := span.ChildSpans[0]
				assert.Equal(t, "Attempt 0", one.Name)
				assert.False(t, one.IsRoot)
				assert.Equal(t, rootSpanID, one.ParentSpanID)
				assert.Equal(t, 0, one.Attempts)
				assert.Equal(t, models.RunTraceSpanStatusFailed.String(), one.Status)
				assert.NotNil(t, one.OutputID)

				// output test
				oneOutput := c.RunSpanOutput(ctx, *one.OutputID)
				c.ExpectSpanErrorOutput(t, "", "nope!", oneOutput)
			})

			// second attempt
			t.Run("attempt 1", func(t *testing.T) {
				two := span.ChildSpans[1]
				assert.Equal(t, "Attempt 1", two.Name)
				assert.False(t, two.IsRoot)
				assert.Equal(t, rootSpanID, two.ParentSpanID)
				assert.Equal(t, 1, two.Attempts)
				assert.Equal(t, models.RunTraceSpanStatusFailed.String(), two.Status)
				assert.NotNil(t, two.OutputID)

				// output test
				twoOutput := c.RunSpanOutput(ctx, *two.OutputID)
				c.ExpectSpanErrorOutput(t, "", "nope!", twoOutput)
			})
		})
	})
}

func TestNonSDKJSON(t *testing.T) {
	startApp := func(
		proxyURL *url.URL,
		appName string,
		eventName string,
	) (*HTTPServer, func() error) {
		inngestgo.DefaultClient = inngestgo.NewClient(
			inngestgo.ClientOpts{EventKey: util.ToPtr("test")},
		)
		opts := inngestgo.HandlerOpts{
			Logger:      slog.Default(),
			RegisterURL: inngestgo.StrPtr(fmt.Sprintf("%s/fn/register", DEV_URL)),
			URL:         proxyURL,
		}
		h := inngestgo.NewHandler(appName, opts)
		fn := inngestgo.CreateFunction(
			inngestgo.FunctionOpts{
				Name:    "my-fn",
				Retries: inngestgo.IntPtr(0),
			},
			inngestgo.EventTrigger(eventName, nil),
			func(ctx context.Context, input inngestgo.Input[any]) (any, error) {
				return nil, nil
			},
		)
		h.Register(fn)
		server := NewHTTPServer(h)

		sync := func() error {
			req, err := http.NewRequest(http.MethodPut, server.LocalURL(), nil)
			if err != nil {
				return err
			}

			timeout := time.Now().Add(5 * time.Second)
			for {
				resp, err := http.DefaultClient.Do(req)
				if err == nil && resp.StatusCode == http.StatusOK {
					return nil
				}
				if time.Now().After(timeout) {
					return fmt.Errorf("timeout waiting for sync")
				}
				time.Sleep(100 * time.Millisecond)
			}
		}

		return server, sync
	}

	t.Run("non-sdk json", func(t *testing.T) {
		_ = os.Setenv("INNGEST_DEV", DEV_URL)
		r := require.New(t)
		ctx := context.Background()
		c := client.New(t)

		// Start proxy which returns a non-SDK JSON response.
		errResp := map[string]any{
			"Reason":  "ConcurrentInvocationLimitExceeded",
			"Type":    "User",
			"message": "Rate Exceeded.",
		}
		proxy := NewHTTPServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusTooManyRequests)
			byt, _ := json.Marshal(errResp)
			_, _ = w.Write(byt)
		}))
		defer proxy.Close()
		proxyURL, err := url.Parse(proxy.URL())
		r.NoError(err)

		// Random to avoid collisions with other tests.
		appName := uuid.New().String()
		eventName := uuid.New().String()

		// Start app.
		server, sync := startApp(proxyURL, appName, eventName)
		defer server.Close()
		r.NoError(sync())

		// Send event and get run ID.
		eventID, err := inngestgo.Send(
			ctx,
			inngestgo.Event{Data: map[string]any{"foo": 1}, Name: eventName},
		)
		r.NoError(err)
		runID, err := waitForRunIDFromEventID(ctx, c, eventID)
		r.NoError(err)

		require.EventuallyWithT(t, func(t *assert.CollectT) {
			a := assert.New(t)
			a.NotEmpty(runID)

			run, err := c.RunTraces(ctx, *runID)
			if !a.NoError(err) {
				return
			}
			a.Equal(run.Status, "FAILED")

			output := c.RunSpanOutput(ctx, *run.Trace.OutputID)
			a.Nil(output.Data)
			if !a.NotNil(output.Error) {
				return
			}
			a.Equal("Error", *output.Error.Name)
			if !a.NotNil(output.Error.Stack) {
				return
			}
			var stack map[string]any
			err = json.Unmarshal([]byte(*output.Error.Stack), &stack)
			a.NoError(err)
			a.Equal(stack, errResp)
		}, 10*time.Second, time.Second)
	})
}

func waitForRunIDFromEventID(
	ctx context.Context,
	c *client.Client,
	eventID string,
) (*string, error) {
	timeout := time.Now().Add(5 * time.Second)
	for {
		runs, err := c.RunsByEventID(ctx, eventID)
		if err == nil && len(runs) == 1 {
			return &runs[0].ID, nil
		}

		if time.Now().After(timeout) {
			return nil, fmt.Errorf("timeout waiting for run")
		}
		time.Sleep(100 * time.Millisecond)
	}
}
