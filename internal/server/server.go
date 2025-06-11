package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/client-go/rest"

	"pgdistbench/api/benchdriverapi"
	"pgdistbench/internal/worker"
	"pgdistbench/internal/worker/chbench"
	"pgdistbench/internal/worker/k8stress"
	"pgdistbench/internal/worker/runner"
	"pgdistbench/internal/worker/tpcc"
	"pgdistbench/internal/worker/tpch"
)

type Handler struct {
	runner          *runner.Runner
	getk8RestConfig func() (*rest.Config, error)

	Metrics *prometheus.Registry
}

type okResponse struct {
	Status string
}

var ok = okResponse{Status: "ok"}

func NewHandler(
	w *runner.Runner,
	getk8RestConfig func() (*rest.Config, error),
) *Handler {
	return &Handler{
		runner:          w,
		getk8RestConfig: getk8RestConfig,
	}
}

func (h *Handler) RegisterRoutes(r chi.Router) {
	r.Get("/", routeListHandler(r))
	r.Get("/status", statusHandler(func(ctx context.Context) (benchdriverapi.APIWorkerStatus, error) {
		return h.runner.Status(ctx), nil
	}))
	r.Get("/healthz", statusHandler(func(ctx context.Context) (benchdriverapi.StatusCode, error) {
		return h.runner.Healthcheck(ctx)
	}))

	work := chi.NewRouter()
	work.Post("/stop", statusHandler(func(ctx context.Context) (okResponse, error) {
		return ok, h.runner.CancelActive(ctx)
	}))
	r.Mount("/work", work)

	work.Mount("/tpcc", h.tpccRoutes())
	work.Mount("/tpch", h.tpchRoutes())
	work.Mount("/chbench", h.chbenchRoutes())
	work.Mount("/k8stress", h.stressk8Routes())
}

func (h *Handler) tpccRoutes() chi.Router {
	taskFactory := tpcc.NewFactory(h.runner.Config).WithMetrics(h.Metrics)
	return benchWorkRouter(h.runner, taskFactory)
}

func (h *Handler) tpchRoutes() chi.Router {
	taskFactory := tpch.NewFactory(h.runner.Config)
	return benchWorkRouter(h.runner, taskFactory)
}

func (h *Handler) chbenchRoutes() chi.Router {
	taskFactory := chbench.NewFactory(h.runner.Config)
	return benchWorkRouter(h.runner, taskFactory)
}

func (h *Handler) stressk8Routes() chi.Router {
	taskFactory := k8stress.NewLazyFactory(h.getk8RestConfig).WithMetrics(h.Metrics)
	return benchWorkRouter(h.runner, taskFactory)
}

func benchWorkRouter[T any, F worker.TaskFactory[T]](r *runner.Runner, factory F) chi.Router {
	worker := runner.NewBenchmarkWorker(r, factory)
	router := chi.NewRouter()
	router.Post("/prepare", requHandler(func(ctx context.Context, requ T) (okResponse, error) {
		return ok, worker.Prepare(ctx, requ)
	}))
	router.Post("/cleanup", statusHandler(func(ctx context.Context) (okResponse, error) {
		return ok, worker.Cleanup(ctx)
	}))
	router.Post("/run", requHandler(func(ctx context.Context, requ T) (okResponse, error) {
		return ok, worker.Run(ctx, requ)
	}))

	router.Get("/", routeListHandler(router))
	return router
}

func routeListHandler(router chi.Routes) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// list of available endpoints

		type routePath struct {
			Method string `json:"method"`
			Path   string `json:"path"`
		}

		var routes []routePath
		err := chi.Walk(router, func(method, route string, handler http.Handler, middlewares ...func(http.Handler) http.Handler) error {
			routes = append(routes, routePath{Method: method, Path: route})
			return nil
		})

		type response struct {
			Routes []routePath `json:"routes"`
		}
		writeResponse(w, response{Routes: routes}, err)
	}
}

func statusHandler[O any](fn func(context.Context) (O, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		resp, err := fn(r.Context())
		writeResponse(w, resp, err)
	}
}

func requHandler[I any, O any](fn func(ctx context.Context, w I) (O, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var requ I

		if r.ContentLength > 0 {
			if r.Header.Get("Content-Type") != "application/json" {
				writeError(w, benchdriverapi.ErrorBadRequest(fmt.Errorf("invalid content type: %s", r.Header.Get("Content-Type"))))
				return
			}

			if err := json.NewDecoder(r.Body).Decode(&requ); err != nil {
				writeError(w, benchdriverapi.ErrorBadRequest(fmt.Errorf("failed to decode request: %w", err)))
				return
			}
		}

		resp, err := fn(r.Context(), requ)
		writeResponse(w, resp, err)
	}
}

func writeResponse[T any](w http.ResponseWriter, resp T, err error) {
	if err != nil {
		writeError(w, err)
		return
	}

	enc, err := json.Marshal(resp)
	if err != nil {
		writeError(w, fmt.Errorf("failed to marshal response: %w", err))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(enc)
}

func writeError(w http.ResponseWriter, err error) {
	log.Printf("Error: %v", err)

	w.WriteHeader(getErrorStatusCode(err))

	enc, _ := json.Marshal(map[string]string{"error": getDisplayError(err).Error()})
	w.Header().Set("Content-Type", "application/json")
	w.Write(enc)
}
