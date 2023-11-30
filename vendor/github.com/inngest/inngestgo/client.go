package inngestgo

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
)

var (
	// DefaultClient represents the default, mutable, global client used
	// within the `Send` function provided by this package.
	//
	// You should initialize this within an init() function using `NewClient`
	// if you plan to use the `Send` function:
	//
	// 	func init() {
	// 		inngestgo.DefaultClient = inngestgo.NewClient(
	// 			"key",
	// 			inngestgo.WithHTTPClient(&http.Client{Timeout: 10 * time.Second}),
	// 		)
	// 	}
	//
	// If this client is not set, Send will return an error.
	DefaultClient Client
)

const (
	defaultEndpoint = "https://inn.gs"
)

// Send uses the DefaultClient to send the given event.
func Send(ctx context.Context, e any) (string, error) {
	if DefaultClient == nil {
		return "", fmt.Errorf("no default client initialized for inngest")
	}
	return DefaultClient.Send(ctx, e)
}

// SendMany uses the DefaultClient to send the given event batch.
func SendMany(ctx context.Context, e []any) ([]string, error) {
	if DefaultClient == nil {
		return nil, fmt.Errorf("no default client initialized for inngest")
	}
	return DefaultClient.SendMany(ctx, e)
}

// Client represents a client used to send events to Inngest.
type Client interface {
	// Send sends the specific event to the ingest API.
	Send(ctx context.Context, evt any) (string, error)
	// Send sends a batch of events to the ingest API.
	SendMany(ctx context.Context, evt []any) ([]string, error)
}

type ClientOpts struct {
	// HTTPClient is the HTTP client used to send events.
	HTTPClient *http.Client
	// EventKey is your Inngest event key for sending events.  This defaults to the
	// `INNGEST_EVENT_KEY` environment variable if nil.
	EventKey *string
	// EventURL is the URL of the event API to send events to.  This defaults to
	// https://inn.gs if nil.
	EventURL *string
	// Env is the branch environment to deploy to.  If nil, this uses
	// os.Getenv("INNGEST_ENV").  This only deploys to branches if the
	// signing key is a branch signing key.
	Env *string
}

// NewClient returns a concrete client initialized with the given ingest key,
// which can immediately send events to the ingest API.
func NewClient(opts ClientOpts) Client {
	c := &apiClient{
		ClientOpts: opts,
	}

	if c.ClientOpts.HTTPClient == nil {
		c.ClientOpts.HTTPClient = http.DefaultClient
	}

	return c
}

// apiClient is a concrete implementation of Client that uses the given HTTP client
// to send events to the ingest API
type apiClient struct {
	ClientOpts
}

func (a apiClient) GetEnv() string {
	if a.Env == nil {
		return os.Getenv("INNGEST_ENV")
	}
	return *a.Env
}

func (a apiClient) GetEventKey() string {
	if a.EventKey == nil {
		return os.Getenv("INNGEST_EVENT_KEY")
	}
	return *a.EventKey
}

type validatable interface {
	Validate() error
}

func (a apiClient) Send(ctx context.Context, e any) (string, error) {
	res, err := a.SendMany(ctx, []any{e})
	if err != nil || len(res) == 0 {
		return "", err
	}
	return res[0], nil
}

func (a apiClient) SendMany(ctx context.Context, e []any) ([]string, error) {
	for _, e := range e {
		if v, ok := e.(validatable); ok {
			if err := v.Validate(); err != nil {
				return nil, fmt.Errorf("error validating event: %w", err)
			}
		}
	}

	byt, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("error marshalling event to json: %w", err)
	}

	ep := defaultEndpoint
	if IsDev() {
		ep = DevServerURL()
	}
	if a.EventURL != nil {
		ep = *a.EventURL
	}

	url := fmt.Sprintf("%s/e/%s", ep, a.GetEventKey())
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(byt))
	if err != nil {
		return nil, fmt.Errorf("error creating event request: %w", err)
	}
	SetBasicRequestHeaders(req)

	if a.GetEnv() != "" {
		req.Header.Add(HeaderKeyEnv, a.GetEnv())
	}

	resp, err := a.HTTPClient.Post(url, "application/json", bytes.NewBuffer(byt))
	if err != nil {
		return nil, fmt.Errorf("error sending event request: %w", err)
	}

	// There is no body to read;  the ingest API responds with status codes representing
	// each error.  We don't necessarily care about the error behind this close.
	defer resp.Body.Close()

	switch resp.StatusCode {
	case 200, 201:
		ids := eventAPIResponse{}
		_ = json.NewDecoder(resp.Body).Decode(&ids)
		if len(ids.IDs) == 1 {
			return ids.IDs, nil
		}
		return nil, nil
	case 400:
		return nil, fmt.Errorf("invalid event data")
	case 401:
		return nil, fmt.Errorf("unknown ingest key")
	case 403:
		// The ingest key has an IP or event type allow/denylist.
		return nil, fmt.Errorf("this ingest key is not authorized to send this event")
	}

	return nil, fmt.Errorf("unknown status code sending event: %d", resp.StatusCode)
}

// eventAPIResponse is the API response sent when responding to incoming events.
type eventAPIResponse struct {
	IDs    []string `json:"ids"`
	Status int      `json:"status"`
	Error  error    `json:"error,omitempty"`
}
