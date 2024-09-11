package devcdc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/inngest/dbcap/pkg/changeset"
	"github.com/inngest/dbcap/pkg/eventwriter"
	"github.com/inngest/dbcap/pkg/replicator"
	"github.com/inngest/inngest/pkg/logger"
	"github.com/inngest/inngestgo"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
)

type DevCDC interface {
	AddAPIRoutes(r chi.Router)

	// Stop stops all replicators from running.
	Stop()
}

func New(ctx context.Context) DevCDC {
	client := inngestgo.NewClient(inngestgo.ClientOpts{
		EventKey: inngestgo.StrPtr("cdc"),
		EventURL: inngestgo.StrPtr("http://127.0.0.1:8288"),
	})

	cdc := &devcdc{
		l:           &sync.Mutex{},
		replicators: map[string]replicator.Replicator{},
	}

	cdc.writer = eventwriter.NewAPIClientWriter(ctx, client, 50)
	cdc.cs = cdc.writer.Listen(ctx, cdc)

	return cdc
}

type devcdc struct {
	l      *sync.Mutex
	writer eventwriter.EventWriter
	cs     chan *changeset.Changeset

	// replicators is a map of connection strings -> replicator
	replicators map[string]replicator.Replicator
}

// AddAPIRoutes adds new API routes to the given router for exposing development
// CDC endpoints.
func (d devcdc) AddAPIRoutes(r chi.Router) {

	r.Post("/test", d.apiTestConnection)
	r.Post("/add", d.apiAddConnection)
	r.Post("/setup/pg", d.apiSetupPostgres)
	r.Get("/", d.apiGetConnections)

	r.Get("/lol", func(w http.ResponseWriter, req *http.Request) {
		w.Write([]byte("lol"))
	})
}

func (devcdc) Commit(changeset.Watermark) {
	// Not implemented.  The dev server does not commit.
}

// AddAPIRoutes adds new API routes to the given router for exposing development
// CDC endpoints.
func (d devcdc) Stop() {
	for _, r := range d.replicators {
		r.Stop()
	}
	d.writer.Wait()
}

//
// CDC API
//

func (d devcdc) apiGetConnections(w http.ResponseWriter, r *http.Request) {
	// TODO: List connections.
}

func (d devcdc) apiTestConnection(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	req := addConnectionRequest{}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error()))
		return
	}

	ctx := context.Background()

	if _, err := req.NewReplicator(ctx); err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error()))
		return
	}
}

func (d devcdc) apiAddConnection(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	req := addConnectionRequest{}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error()))
		return
	}

	ctx := context.Background()

	repl, err := req.NewReplicator(ctx)
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error()))
		return
	}

	logger.StdlibLogger(r.Context()).Info("adding cdc replicator", "engine", req.Engine)

	d.l.Lock()
	d.replicators[req.ConnString] = repl
	d.l.Unlock()

	go repl.Pull(ctx, d.cs)
}

func (d devcdc) apiSetupPostgres(w http.ResponseWriter, r *http.Request) {
	// TODO: Create the new users, roles, replica slot, and publications.
}

type addConnectionRequest struct {
	// Engine refers to the database engine we're connecting to.  Valid engines are:
	// "postgres", "mariadb", "mysql".
	Engine string `json:"engine"`
	// ConnString is the connection string.
	ConnString string `json:"conn"`
}

func (a addConnectionRequest) NewReplicator(ctx context.Context) (replicator.Replicator, error) {
	var r replicator.Replicator

	switch a.Engine {
	case "postgres":
		cfg, err := pgx.ParseConfig(a.ConnString)
		if err != nil {
			return nil, fmt.Errorf("Invalid postgres connection string: %w", err)
		}

		r, err = replicator.Postgres(ctx, replicator.PostgresOpts{Config: *cfg})
		if err != nil {
			return nil, fmt.Errorf("Error creating postgres replicator: %w", err)
		}
	default:
		return nil, fmt.Errorf("unknown engine type: %s", a.Engine)
	}

	if err := r.TestConnection(ctx); err != nil {
		return nil, err
	}

	return r, nil
}
