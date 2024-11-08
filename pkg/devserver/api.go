package devserver

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/inngest/inngest/pkg/api/tel"
	"github.com/inngest/inngest/pkg/consts"
	"github.com/inngest/inngest/pkg/cqrs"
	"github.com/inngest/inngest/pkg/headers"
	"github.com/inngest/inngest/pkg/inngest"
	"github.com/inngest/inngest/pkg/inngest/log"
	"github.com/inngest/inngest/pkg/inngest/version"
	"github.com/inngest/inngest/pkg/logger"
	"github.com/inngest/inngest/pkg/publicerr"
	"github.com/inngest/inngest/pkg/sdk"
	"github.com/oklog/ulid/v2"
	ptrace "go.opentelemetry.io/collector/pdata/ptrace"
)

type devapi struct {
	chi.Router

	// loader stores all registered functions in the dev server.
	//
	// TODO: Refactor this so that it's a part of the devserver, instead
	// of holding a reference which is a weird pattern (tonyhb)
	devserver *devserver
}

func NewDevAPI(d *devserver) chi.Router {
	// Return a chi router, which lets us attach routes to a handler.
	api := &devapi{
		Router:    chi.NewMux(),
		devserver: d,
	}
	api.addRoutes()
	return api
}

func (a *devapi) addRoutes() {
	a.Use(func(next http.Handler) http.Handler {
		fn := func(w http.ResponseWriter, r *http.Request) {
			l := logger.From(r.Context()).With().Str("caller", a.devserver.Name()).Logger()
			r = r.WithContext(logger.With(r.Context(), l))
			next.ServeHTTP(w, r)
		}
		return http.HandlerFunc(fn)
	})
	a.Use(headers.StaticHeadersMiddleware(a.devserver.Opts.Config.GetServerKind()))

	a.Get("/dev", a.Info)
	a.Post("/dev/traces", a.OTLPTrace)
	a.Post("/fn/register", a.Register)
	// This allows tests to remove apps by URL
	a.Delete("/fn/remove", a.RemoveApp)

	// This allows tests to update step limits per function
	a.Post("/fn/step-limit", a.SetStepLimit)
	a.Delete("/fn/step-limit", a.RemoveStepLimit)
	a.Post("/fn/state-size-limit", a.SetStateSizeLimit)
	a.Delete("/fn/state-size-limit", a.RemoveStateSizeLimit)

	// Go embeds files relative to the current source, which embeds
	// all under ./static.  We remove the ./static
	// directory by using fs.Sub: https://pkg.go.dev/io/fs#Sub.
	staticFS, _ := fs.Sub(static, "static")
	a.Get("/images/*", http.FileServer(http.FS(staticFS)).ServeHTTP)

	a.Get("/assets/*", http.FileServer(http.FS(staticFS)).ServeHTTP)
	a.Get("/_next/*", http.FileServer(http.FS(staticFS)).ServeHTTP)
	a.Get("/{file}.txt", http.FileServer(http.FS(staticFS)).ServeHTTP)
	a.Get("/{file}.svg", http.FileServer(http.FS(staticFS)).ServeHTTP)
	a.Get("/{file}.jpg", http.FileServer(http.FS(staticFS)).ServeHTTP)
	a.Get("/{file}.png", http.FileServer(http.FS(staticFS)).ServeHTTP)
	// Everything else loads the UI.
	a.NotFound(a.UI)
}

func (a devapi) UI(w http.ResponseWriter, r *http.Request) {
	// If there's a file that exists within `static` for this particular route,
	// return it as a static asset.
	path := r.URL.Path
	if f, err := static.Open("static" + path); err == nil {
		if stat, err := f.Stat(); err == nil && !stat.IsDir() {
			_, _ = io.Copy(w, f)
			return
		}
	}

	// If there's a trailing slash, redirect to non-trailing slashes.
	if strings.HasSuffix(r.URL.Path, "/") && r.URL.Path != "/" {
		r.URL.Path = strings.TrimSuffix(r.URL.Path, "/")
		http.Redirect(w, r, r.URL.String(), 303)
		return
	}

	m := tel.NewMetadata(r.Context())
	tel.SendEvent(r.Context(), "cli/dev_ui.loaded", m)
	tel.SendMetadata(r.Context(), m)

	byt := parsedRoutes.serve(r.Context(), r.URL.Path)
	_, _ = w.Write(byt)
}

// Info returns information about the dev server and its registered functions.
func (a devapi) Info(w http.ResponseWriter, r *http.Request) {
	a.devserver.handlerLock.Lock()
	defer a.devserver.handlerLock.Unlock()

	all, _ := a.devserver.Data.GetFunctions(r.Context())
	funcs := make([]inngest.Function, len(all))
	for n, i := range all {
		f := inngest.Function{}
		_ = json.Unmarshal([]byte(i.Config), &f)
		funcs[n] = f
	}

	features := map[string]bool{}
	for _, flag := range featureFlags {
		enabled, _ := strconv.ParseBool(os.Getenv(flag))
		features[flag] = enabled
	}

	ir := InfoResponse{
		Version:             version.Print(),
		StartOpts:           a.devserver.Opts,
		Functions:           funcs,
		Handlers:            a.devserver.handlers,
		IsSingleNodeService: a.devserver.IsSingleNodeService(),
		IsMissingSigningKey: a.devserver.Opts.RequireKeys && !a.devserver.HasValidSigningKey(),
		IsMissingEventKeys:  a.devserver.Opts.RequireKeys && !a.devserver.HasEventKeys(),
		Features:            features,
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	byt, _ := json.MarshalIndent(ir, "", "  ")
	_, _ = w.Write(byt)
}

// Register regsters functions served via SDKs
func (a devapi) Register(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	ctx := r.Context()

	if a.devserver.Opts.RequireKeys && !a.devserver.HasValidSigningKey() {
		errMsg := "no signing key provided in server; add a signing key to allow syncing and runs"

		logger.From(ctx).Warn().Msg(errMsg)
		a.err(ctx, w, 401, fmt.Errorf(errMsg))
		return
	}

	expectedServerKind := r.Header.Get(headers.HeaderKeyExpectedServerKind)
	if expectedServerKind != "" && expectedServerKind != a.devserver.Opts.Config.GetServerKind() {
		a.err(ctx, w, 400, fmt.Errorf("Expected server kind %s, got %s", a.devserver.Opts.Config.GetServerKind(), expectedServerKind))
		return
	}

	if a.devserver.Opts.Config.GetServerKind() == headers.ServerKindCloud {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			errMsg := "no authorization header provided"

			logger.From(ctx).Warn().Msg(errMsg)
			a.err(ctx, w, 401, fmt.Errorf(errMsg))
			return
		}

		if len(authHeader) > 7 && strings.ToUpper(authHeader[0:6]) == "BEARER" {
			authHeader = authHeader[7:]
		}

		if authHeader == "" {
			errMsg := "empty authorization header provided"

			logger.From(ctx).Warn().Msg(errMsg)
			a.err(ctx, w, 401, fmt.Errorf(errMsg))
			return
		}

		if !a.devserver.Opts.SigningKey.Equal(authHeader) {
			errMsg := "invalid authorization header provided"

			logger.From(ctx).Warn().Msg(errMsg)
			a.err(ctx, w, 401, fmt.Errorf(errMsg))
			return
		}
	}

	a.devserver.handlerLock.Lock()
	defer a.devserver.handlerLock.Unlock()

	req, err := sdk.FromReadCloser(r.Body, sdk.FromReadCloserOpts{})
	if err != nil {
		logger.From(ctx).Warn().Msgf("Invalid request:\n%s", err)
		a.err(ctx, w, 400, fmt.Errorf("Invalid request: %w", err))
		return
	}

	if err := a.register(ctx, req); err != nil {
		logger.From(ctx).Warn().Msgf("Error registering functions:\n%s", err)
		_ = publicerr.WriteHTTP(w, err)
		return
	}

	// Re-initialize our cron manager.
	if err := a.devserver.Runner.InitializeCrons(ctx); err != nil {
		logger.From(ctx).Warn().Msgf("Error initializing crons:\n%s", err)
		a.err(ctx, w, 400, err)
		return
	}

	_, _ = w.Write([]byte(`{"ok":true}`))
}

func (a devapi) register(ctx context.Context, r sdk.RegisterRequest) (err error) {
	sum, err := r.Checksum()
	if err != nil {
		return publicerr.Wrap(err, 400, "Invalid request")
	}

	if app, err := a.devserver.Data.GetAppByChecksum(ctx, sum); err == nil {
		if !app.Error.Valid {
			// Skip registration since the app was already successfully
			// registered.
			return nil
		}

		// Clear app error.
		_, err = a.devserver.Data.UpdateAppError(
			ctx,
			cqrs.UpdateAppErrorParams{
				ID:    app.ID,
				Error: sql.NullString{},
			},
		)
		if err != nil {
			return publicerr.Wrap(err, 500, "Error updating app error")
		}
	}

	// Attempt to get the existing app by URL, and delete it if possible.
	// We're going to recreate it below.
	//
	// We need to do this as we always create an app when entering the URL
	// via the UI.  This is a dev-server specific quirk.
	appID := inngest.DeterministicAppUUID(r.URL)

	tx, err := a.devserver.Data.WithTx(ctx)
	if err != nil {
		return publicerr.Wrap(err, 500, "Error starting registration tx")
	}

	defer func() {
		appParams := cqrs.UpsertAppParams{
			// Use a deterministic ID for the app in dev.
			ID:          appID,
			Name:        r.AppName,
			SdkLanguage: r.SDKLanguage(),
			SdkVersion:  r.SDKVersion(),
			Framework: sql.NullString{
				String: r.Framework,
				Valid:  r.Framework != "",
			},
			Url:      r.URL,
			Checksum: sum,
		}

		// We want to save an app at the end, after handling each error.
		if err != nil {
			appParams.Error = sql.NullString{
				String: err.Error(),
				Valid:  true,
			}
		}
		_, _ = tx.UpsertApp(ctx, appParams)
		err = tx.Commit(ctx)
		if err != nil {
			logger.From(ctx).Error().Err(err).Msg("error registering functions")
		}
	}()

	// Get a list of all functions
	existing, _ := tx.GetFunctionsByAppInternalID(ctx, uuid.UUID{}, appID)
	// And get a list of functions that we've upserted.  We'll delete all existing functions not in
	// this set.
	seen := map[uuid.UUID]struct{}{}

	// XXX (tonyhb): If we're authenticated, we can match the signing key against the workspace's
	// signing key and warn if the user has an invalid key.
	funcs, err := r.Parse(ctx)
	if err != nil && err != sdk.ErrNoFunctions {
		return publicerr.Wrap(err, 400, "At least one function is invalid")
	}

	// For each function,
	for _, fn := range funcs {
		// Create a new UUID for the function.
		fn.ID = fn.DeterministicUUID()

		// Mark as seen.
		seen[fn.ID] = struct{}{}

		config, err := json.Marshal(fn)
		if err != nil {
			return publicerr.Wrap(err, 500, "Error marshalling function")
		}

		if _, err := tx.GetFunctionByInternalUUID(ctx, uuid.UUID{}, fn.ID); err == nil {
			// Update the function config.
			_, err = tx.UpdateFunctionConfig(ctx, cqrs.UpdateFunctionConfigParams{
				ID:     fn.ID,
				Config: string(config),
			})
			if err != nil {
				return publicerr.Wrap(err, 500, "Error updating function config")
			}
			continue
		}

		_, err = tx.InsertFunction(ctx, cqrs.InsertFunctionParams{
			ID:        fn.ID,
			Name:      fn.Name,
			Slug:      fn.Slug,
			AppID:     appID,
			Config:    string(config),
			CreatedAt: time.Now(),
		})
		if err != nil {
			err = fmt.Errorf("Function %s is invalid: %w", fn.Slug, err)
			return publicerr.Wrap(err, 500, "Error saving function")
		}
	}

	// Remove all unseen functions.
	deletes := []uuid.UUID{}
	for _, fn := range existing {
		if _, ok := seen[fn.ID]; !ok {
			deletes = append(deletes, fn.ID)
		}
	}
	if len(deletes) == 0 {
		return nil
	}

	if err = tx.DeleteFunctionsByIDs(ctx, deletes); err != nil {
		return publicerr.Wrap(err, 500, "Error deleting removed function")
	}
	return nil
}

func (a devapi) OTLPTrace(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	body, err := io.ReadAll(r.Body)
	if err != nil {
		_ = publicerr.WriteHTTP(w, publicerr.Error{
			Status:  400,
			Err:     err,
			Message: err.Error(),
		})
	}
	defer r.Body.Close()

	var encoder ptrace.Unmarshaler
	cnt := r.Header.Get("Content-Type")
	switch cnt {
	case "application/x-protobuf":
		encoder = &ptrace.ProtoUnmarshaler{}
	case "application/json":
		encoder = &ptrace.JSONUnmarshaler{}
	default:
		log.From(ctx).Error().Str("content-type", cnt).Msg("unknown content type for traces")
		err = fmt.Errorf("unable to handle unknown content type for traces: %s", cnt)
		_ = publicerr.WriteHTTP(w, publicerr.Error{
			Status:  400,
			Err:     err,
			Message: err.Error(),
		})
		return
	}

	traces, err := encoder.UnmarshalTraces(body)
	if err != nil {
		_ = publicerr.WriteHTTP(w, publicerr.Error{
			Status:  400,
			Err:     err,
			Message: err.Error(),
		})
		return
	}
	log.From(ctx).Trace().Int("len", traces.SpanCount()).Msg("recording otel trace spans")

	handler := newSpanIngestionHandler(a.devserver.Data)

	for i := 0; i < traces.ResourceSpans().Len(); i++ {
		rs := traces.ResourceSpans().At(i)

		rattr, err := convertMap(rs.Resource().Attributes().AsRaw())
		if err != nil {
			log.From(ctx).Warn().Err(err).Interface("resource", rs.Resource().Attributes().AsRaw()).Msg("error parsing resource attributes")
		}

		var serviceName string
		if v, ok := rattr["service.name"]; ok {
			serviceName = v
		}

		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)

			scopeName := ss.Scope().Name()
			scopeVersion := ss.Scope().Version()

			for k := 0; k < ss.Spans().Len(); k++ {
				span := ss.Spans().At(k)

				dur := span.EndTimestamp().AsTime().Sub(span.StartTimestamp().AsTime())
				sattr, err := convertMap(span.Attributes().AsRaw())
				if err != nil {
					log.From(ctx).Warn().Err(err).Interface("span attr", span.Attributes().AsRaw()).Msg("error parsing span attributes")

				}

				evts := []cqrs.SpanEvent{}
				for ei := 0; ei < span.Events().Len(); ei++ {
					evt := span.Events().At(ei)
					attr, err := convertMap(evt.Attributes().AsRaw())
					if err != nil {
						log.From(ctx).Error().Err(err).Interface("span event", evt.Attributes().AsRaw()).Msg("error parsing span event")
						continue
					}

					evts = append(evts, cqrs.SpanEvent{
						Timestamp:  evt.Timestamp().AsTime(),
						Name:       evt.Name(),
						Attributes: attr,
					})
				}

				links := []cqrs.SpanLink{}
				for li := 0; li < span.Links().Len(); li++ {
					link := span.Links().At(li)
					attr, err := convertMap(link.Attributes().AsRaw())
					if err != nil {
						log.From(ctx).Error().Err(err).Interface("span link", link.Attributes().AsRaw()).Msg("error parsing span link")
					}

					links = append(links, cqrs.SpanLink{
						TraceID:    link.TraceID().String(),
						SpanID:     link.SpanID().String(),
						TraceState: link.TraceState().AsRaw(),
						Attributes: attr,
					})
				}

				cqrsspan := &cqrs.Span{
					Timestamp:          span.StartTimestamp().AsTime(),
					TraceID:            span.TraceID().String(),
					SpanID:             span.SpanID().String(),
					SpanName:           span.Name(),
					SpanKind:           span.Kind().String(),
					ServiceName:        serviceName,
					ResourceAttributes: rattr,
					ScopeName:          scopeName,
					ScopeVersion:       scopeVersion,
					SpanAttributes:     sattr,
					Duration:           dur,
					StatusCode:         span.Status().Code().String(),
					Events:             evts,
					Links:              links,
				}

				if !span.ParentSpanID().IsEmpty() {
					id := span.ParentSpanID().String()
					cqrsspan.ParentSpanID = &id
				}
				if span.TraceState().AsRaw() != "" {
					state := span.TraceState().AsRaw()
					cqrsspan.TraceState = &state
				}
				if span.Status().Message() != "" {
					msg := span.Status().Message()
					cqrsspan.StatusMessage = &msg
				}

				if v, ok := sattr[consts.OtelAttrSDKRunID]; ok {
					if rid, err := ulid.Parse(v); err == nil {
						cqrsspan.RunID = &rid
					}
				}

				handler.Add(ctx, cqrsspan)
			}
		}
	}

	for _, s := range handler.Spans() {
		if err := a.devserver.Data.InsertSpan(ctx, s); err != nil {
			log.From(ctx).Error().Err(err).Interface("span", *s).Msg("error inserting span")
		}
	}

	for _, r := range handler.TraceRuns() {
		// log.From(ctx).Debug().Interface("run", r).Msg("trace run")
		if err := a.devserver.Data.InsertTraceRun(ctx, r); err != nil {
			log.From(ctx).Error().Err(err).Interface("trace run", r).Msg("error inserting trace run")
		}
	}
}

// RemoveApp allows users to de-register an app by its URL
func (a devapi) RemoveApp(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	url := r.FormValue("url")

	app, err := a.devserver.Data.GetAppByURL(ctx, url)
	if err != nil {
		_ = publicerr.WriteHTTP(w, publicerr.Wrapf(err, 404, "App not found: %s", url))
		return
	}

	if err := a.devserver.Data.DeleteFunctionsByAppID(ctx, app.ID); err != nil {
		_ = publicerr.WriteHTTP(w, publicerr.Wrap(err, 500, "Error deleting functions"))
		return
	}

	if err := a.devserver.Data.DeleteApp(ctx, app.ID); err != nil {
		_ = publicerr.WriteHTTP(w, publicerr.Wrap(err, 500, "Error deleting app"))
		return
	}
}

func (a devapi) SetStepLimit(w http.ResponseWriter, r *http.Request) {
	functionId := r.FormValue("functionId")
	limitStr := r.FormValue("limit")

	limit, err := strconv.Atoi(limitStr)
	if err != nil {
		_ = publicerr.WriteHTTP(w, publicerr.Wrapf(err, 400, "Invalid limit: %s", limitStr))
		return
	}

	a.devserver.stepLimitOverrides[functionId] = limit
}

func (a devapi) RemoveStepLimit(w http.ResponseWriter, r *http.Request) {
	functionId := r.FormValue("functionId")

	if _, ok := a.devserver.stepLimitOverrides[functionId]; !ok {
		_ = publicerr.WriteHTTP(w, publicerr.Wrapf(nil, 404, "No step limit set for function: %s", functionId))
		return
	}

	delete(a.devserver.stepLimitOverrides, functionId)
}

func (a devapi) SetStateSizeLimit(w http.ResponseWriter, r *http.Request) {
	functionId := r.FormValue("functionId")
	limitStr := r.FormValue("limit")

	limit, err := strconv.Atoi(limitStr)
	if err != nil {
		_ = publicerr.WriteHTTP(w, publicerr.Wrapf(err, 400, "Invalid limit: %s", limitStr))
		return
	}

	a.devserver.stateSizeLimitOverrides[functionId] = limit
}

func (a devapi) RemoveStateSizeLimit(w http.ResponseWriter, r *http.Request) {
	functionId := r.FormValue("functionId")

	if _, ok := a.devserver.stateSizeLimitOverrides[functionId]; !ok {
		_ = publicerr.WriteHTTP(w, publicerr.Wrapf(nil, 404, "No state size limit set for function: %s", functionId))
		return
	}

	delete(a.devserver.stateSizeLimitOverrides, functionId)
}

func (a devapi) err(ctx context.Context, w http.ResponseWriter, status int, err error) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
	logger.From(ctx).Error().Msg(err.Error())
}

type InfoResponse struct {
	// Version lists the version of the development server
	Version             string             `json:"version"`
	Authenticated       bool               `json:"authed"`
	StartOpts           StartOpts          `json:"startOpts"`
	Functions           []inngest.Function `json:"functions"`
	Handlers            []SDKHandler       `json:"handlers"`
	IsSingleNodeService bool               `json:"isSingleNodeService"`

	// If true, the server is running in a mode where it requires a signing key
	// to function and it is not set.
	IsMissingSigningKey bool `json:"isMissingSigningKey"`

	// If true, the server is running in a mode where it requires one or more
	// event keys to function and they are not set.
	IsMissingEventKeys bool `json:"isMissingEventKeys"`

	// Features acts as an in-memory feature flag for the UI
	Features map[string]bool `json:"features"`
}
