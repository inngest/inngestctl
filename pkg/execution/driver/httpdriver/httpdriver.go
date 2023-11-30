package httpdriver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/inngest/inngest/pkg/consts"
	"github.com/inngest/inngest/pkg/execution/driver"
	"github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/state"
	headers_lib "github.com/inngest/inngest/pkg/headers"
	"github.com/inngest/inngest/pkg/inngest"
)

var (
	dialer = &net.Dialer{KeepAlive: 15 * time.Second}

	DefaultTransport = &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          0,
		IdleConnTimeout:       0,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		// New, ensuring that services can take their time before
		// responding with headers as they process long running
		// kjobs.
		ResponseHeaderTimeout: consts.MaxFunctionTimeout,
	}
	DefaultClient = &http.Client{
		Timeout:       consts.MaxFunctionTimeout,
		CheckRedirect: checkRedirect,
		Transport:     DefaultTransport,
	}

	DefaultExecutor = &executor{Client: DefaultClient}

	ErrEmptyResponse = fmt.Errorf("no response data")
	ErrNoRetryAfter  = fmt.Errorf("no retry after present")
	ErrNotSDK        = errors.New("response is not from an SDK")
)

type executor struct {
	Client     *http.Client
	signingKey []byte
}

// RuntimeType fulfiils the inngest.Runtime interface.
func (e executor) RuntimeType() string {
	return "http"
}

func (e executor) Execute(ctx context.Context, s state.State, item queue.Item, edge inngest.Edge, step inngest.Step, idx, attempt int) (*state.DriverResponse, error) {
	uri, err := url.Parse(step.URI)
	if err != nil {
		return nil, err
	}

	input, err := driver.MarshalV1(ctx, s, step, idx, "", attempt)
	if err != nil {
		return nil, err
	}

	return DoRequest(ctx, e.Client, Request{
		SigningKey: e.signingKey,
		URL:        *uri,
		Input:      input,
		Edge:       edge,
		Step:       step,
	})
}

type Request struct {
	SigningKey []byte
	URL        url.URL
	Input      []byte
	Edge       inngest.Edge
	Step       inngest.Step
}

// DoRequest executes the HTTP request with the given input.
func DoRequest(ctx context.Context, c *http.Client, r Request) (*state.DriverResponse, error) {
	if c == nil {
		c = DefaultClient
	}

	if r.URL.Scheme != "http" && r.URL.Scheme != "https" {
		return nil, fmt.Errorf("Unable to use HTTP executor for non-HTTP runtime")
	}

	// If we have a generator step name, ensure we add the step ID parameter
	values, _ := url.ParseQuery(r.URL.RawQuery)
	if r.Edge.IncomingGeneratorStep != "" {
		values.Set("stepId", r.Edge.IncomingGeneratorStep)
		r.URL.RawQuery = values.Encode()
	} else {
		values.Set("stepId", r.Edge.Incoming)
		r.URL.RawQuery = values.Encode()
	}

	resp, err := do(ctx, c, r)
	if err != nil {
		return nil, err
	}

	if resp.statusCode == 206 {
		// This is a generator-based function returning opcodes.
		dr := &state.DriverResponse{
			Step:           r.Step,
			Duration:       resp.duration,
			Output:         string(resp.body),
			OutputSize:     len(resp.body),
			NoRetry:        resp.noRetry,
			RetryAt:        resp.retryAt,
			RequestVersion: resp.requestVersion,
			StatusCode:     resp.statusCode,
			SDK:            resp.sdk,
		}
		dr.Generator, err = ParseGenerator(ctx, resp.body)
		if err != nil {
			return nil, err
		}
		if resp.noRetry {
			// Ensure we return a NonRetriableError to indicate that
			// we're not retrying when we store the error message.
			err = errors.New("NonRetriableError")
			dr.SetError(err)
		}

		// If this was a generator response with a single op, set some
		// relevant step data so that it's easier to identify this step in
		// history.
		if op := dr.SingleStep(); op != nil {
			dr.Step.ID = op.ID
			dr.Step.Name = op.UserDefinedName()

			if dr.IsSingleStepError() {
				defaultErrMsg := state.DefaultStepErrorMessage
				userErr := state.UserErrorFromRaw(&defaultErrMsg, op.Error)
				if mapped, ok := userErr["message"].(string); ok {
					dr.Err = &mapped
				} else {
					dr.Err = &defaultErrMsg
				}
			}
		}

		if !resp.isSDK {
			dr.SetError(ErrNotSDK)
		}
		return dr, nil
	}

	body := parseResponse(resp.body)
	dr := &state.DriverResponse{
		Step:           r.Step,
		Output:         body,
		Duration:       resp.duration,
		OutputSize:     len(resp.body),
		NoRetry:        resp.noRetry,
		RetryAt:        resp.retryAt,
		RequestVersion: resp.requestVersion,
		StatusCode:     resp.statusCode,
		SDK:            resp.sdk,
	}
	if resp.statusCode < 200 || resp.statusCode > 299 {
		// Add an error to driver.Response if the status code isn't 2XX.
		err = fmt.Errorf("invalid status code: %d", resp.statusCode)
		dr.SetError(err)
	}
	if resp.noRetry {
		// Ensure we return a NonRetriableError to indicate that
		// we're not retrying when we store the error message.
		err = errors.New("NonRetriableError")
		dr.SetError(err)
	}
	if !resp.isSDK {
		dr.SetError(ErrNotSDK)
	}
	return dr, err
}

func do(ctx context.Context, c *http.Client, r Request) (*response, error) {
	if c == nil {
		c = DefaultClient
	}

	ctx, cancel := context.WithTimeout(ctx, consts.MaxFunctionTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.URL.String(), bytes.NewBuffer(r.Input))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}
	req.Header.Add("Content-Type", "application/json")

	if len(r.SigningKey) > 0 {
		req.Header.Add("X-Inngest-Signature", Sign(ctx, r.SigningKey, r.Input))
	}

	pre := time.Now()
	resp, err := c.Do(req)
	dur := time.Since(pre)

	if err != nil {
		if urlErr, ok := err.(*url.Error); ok && urlErr.Err == context.DeadlineExceeded {
			// This timed out.
			return nil, context.DeadlineExceeded
		}
		if errors.Is(err, syscall.EPIPE) {
			return nil, fmt.Errorf("Your server closed the request before finishing.")
		}
		if errors.Is(err, syscall.ECONNRESET) {
			return nil, fmt.Errorf("Your server reset the request connection.")
		}
		return nil, fmt.Errorf("Error performing request to SDK URL: %w", err)
	}

	if err != nil {
		return nil, fmt.Errorf("error executing request: %w", err)
	}
	defer resp.Body.Close()
	byt, err := io.ReadAll(io.LimitReader(resp.Body, consts.MaxBodySize))
	if err != nil {
		return nil, fmt.Errorf("error reading response body: %w", err)
	}

	// These variables are extracted from streaming and non-streaming responses separately.
	//
	// They're defined here so that we can normalize code paths after testing for streaming
	// responses and handling them.
	var (
		statusCode int
		body       []byte
		noRetry    bool
		retryAtStr *string
		retryAt    *time.Time
		headers    = map[string]string{}
	)

	body = byt
	statusCode = resp.StatusCode
	for k, v := range resp.Header {
		headers[strings.ToLower(k)] = v[0]
	}

	// Check the retry status from the headers and versions.
	noRetry = !shouldRetry(statusCode, headers[headerNoRetry], headers[headerSDK])

	// Check if this was a streaming response.  If so, extract headers sent
	// from _after_ the response started within the payload.
	//
	// If the responding status code is 201 Created, the response has been
	// streamed back to us. In this case, the response body will be namespaced
	// under the "body" key, and the status code will be namespaced under the
	// "status" key.
	//
	// Only SDK versions that include the status in the body are expected to
	// send a 201 status code and namespace in this way, so failing to parse
	// here is an error.
	if resp.StatusCode == 201 {
		stream, err := ParseStream(byt)
		if err != nil {
			return nil, err
		}
		// These are all contained within a single wrapper.
		body = stream.Body
		statusCode = stream.StatusCode
		retryAtStr = stream.RetryAt
		noRetry = stream.NoRetry
		// Upsert headers from the stream.
		for k, v := range stream.Headers {
			headers[k] = v
		}
	}

	// Extract the retry at header if it hasn't been set explicitly in streaming.
	if after := headers["retry-after"]; retryAtStr == nil && after != "" {
		retryAtStr = &after
	}
	if retryAtStr != nil {
		if at, err := ParseRetry(*retryAtStr); err == nil {
			retryAt = &at
		}
	}

	isSDK := headers_lib.ValueFromMap(headers_lib.HeaderKeySDK, headers) != ""

	// Get the request version
	rv, _ := strconv.Atoi(headers[headerRequestVersion])
	return &response{
		body:           body,
		statusCode:     statusCode,
		duration:       dur,
		retryAt:        retryAt,
		noRetry:        noRetry,
		requestVersion: rv,
		sdk:            headers[headerSDK],
		isSDK:          isSDK,
	}, err

}

type response struct {
	body           []byte
	statusCode     int
	duration       time.Duration
	requestVersion int
	// retryAt is the time to retry this step at, on failure, if specified in the
	// Retry-After headers, or X-Retry-After.
	//
	// This adheres to the HTTP spec; we support both seconds and times in this header.
	retryAt *time.Time
	noRetry bool
	// sdk represents the SDK language and version used for these
	// functions, in the format: "js:v0.1.0"
	sdk string

	// If response came from the SDK
	isSDK bool
}
