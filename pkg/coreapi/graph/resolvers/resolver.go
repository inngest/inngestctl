package resolvers

// THIS CODE IS A STARTING POINT ONLY. IT WILL NOT BE UPDATED WITH SCHEMA CHANGES.

import (
	"github.com/inngest/inngest/pkg/api"
	"github.com/inngest/inngest/pkg/coreapi/generated"
	"github.com/inngest/inngest/pkg/cqrs"
	"github.com/inngest/inngest/pkg/execution"
	"github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/runner"
	"github.com/inngest/inngest/pkg/history_reader"
	"github.com/inngest/inngest/pkg/keys"
)

type Resolver struct {
	Data          cqrs.Manager
	HistoryReader history_reader.Reader
	Runner        runner.Runner
	Queue         queue.JobQueueReader
	EventHandler  api.EventHandler
	Executor      execution.Executor
	ServerKind    string

	// LocalSigningKey is the key used to sign events for self-hosted services.
	LocalSigningKey *keys.SigningKey

	// RequireKeys defines whether event and signing keys are required for the
	// server to function. If this is true and signing keys are not defined,
	// the server will still boot but core actions such as syncing, runs, and
	// ingesting events will not work.
	RequireKeys bool
}

// Query returns generated.QueryResolver implementation.
func (r *Resolver) Query() generated.QueryResolver { return &queryResolver{r} }

func (r *Resolver) Mutation() generated.MutationResolver { return &mutationResolver{r} }

func (r *Resolver) Event() generated.EventResolver { return &eventResolver{r} }

func (r *Resolver) FunctionRun() generated.FunctionRunResolver { return &functionRunResolver{r} }

func (r *Resolver) FunctionRunV2() generated.FunctionRunV2Resolver { return &functionRunV2Resolver{r} }

func (r *Resolver) App() generated.AppResolver { return &appResolver{r} }

func (r *Resolver) Function() generated.FunctionResolver { return &functionResolver{r} }

func (r *Resolver) StreamItem() generated.StreamItemResolver { return &streamItemResolver{r} }

func (r *Resolver) RunsV2Connection() generated.RunsV2ConnectionResolver {
	return &runsV2ConnResolver{r}
}

type mutationResolver struct{ *Resolver }
type queryResolver struct{ *Resolver }
type eventResolver struct{ *Resolver }
type appResolver struct{ *Resolver }
type functionRunResolver struct{ *Resolver }
type functionRunV2Resolver struct{ *Resolver }
type functionResolver struct{ *Resolver }
type streamItemResolver struct{ *Resolver }
type runsV2ConnResolver struct{ *Resolver }
