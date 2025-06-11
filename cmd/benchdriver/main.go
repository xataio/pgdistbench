package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	"pgdistbench/internal/server"
	"pgdistbench/internal/worker"
	"pgdistbench/internal/worker/runner"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	// Link all k8s auth plugins
	_ "k8s.io/client-go/plugin/pkg/client/auth"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	flag.Parse()

	cfg, err := readWorkerConfig()
	if err != nil {
		return err
	}

	loadRestConfig := loadK8sClusterRestConfig
	runner := runner.New(cfg)
	go runner.Run(context.Background())

	router := chi.NewRouter()
	router.Use(middleware.CleanPath)
	router.Use(middleware.Recoverer)
	router.Use(middleware.RequestLogger(
		&middleware.DefaultLogFormatter{
			Logger:  log.New(os.Stderr, "", log.LstdFlags),
			NoColor: true,
		},
	))
	router.Use(middleware.NoCache)
	router.Use(middleware.StripSlashes)
	router.Use(middleware.AllowContentType("application/json"))
	router.Use(middleware.Heartbeat("/ping"))

	metrics := prometheus.NewRegistry()
	h := server.NewHandler(runner, loadRestConfig)
	h.Metrics = metrics

	router.Get("/metrics", promhttp.HandlerFor(metrics, promhttp.HandlerOpts{}).ServeHTTP)
	h.RegisterRoutes(router)

	fmt.Println("Listening on :8080")
	defer fmt.Println("Goodbye!")
	return http.ListenAndServe(":8080", router)
}

func getEnvOr(name, def string) string {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	return v
}

func parseEnv[T any](name string, or T, parser func(string) (T, error)) (T, error) {
	v := os.Getenv(name)
	if v == "" {
		return or, nil
	}
	parsed, err := parser(v)
	if err != nil {
		return or, fmt.Errorf("read env var %s: %w", name, err)
	}
	return parsed, nil
}

func loadK8sClusterRestConfig() (*rest.Config, error) {
	config, inClusterErr := rest.InClusterConfig()
	if inClusterErr != nil {
		var err error
		kubeconfig := clientcmd.NewDefaultClientConfigLoadingRules().GetDefaultFilename()
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, errors.Join(inClusterErr, err)
		}
	}
	return config, nil
}

func readWorkerConfig() (worker.Config, error) {
	cfg := worker.Config{
		PGHost:     os.Getenv("PGHOST"),
		PGPort:     os.Getenv("PGPORT"),
		PGUser:     getEnvOr("PGUSER", "postgres"),
		PGPass:     getEnvOr("PGPASS", "postgres"),
		PGDatabase: os.Getenv("PGDATABASE"),
		PGSSLMode:  getEnvOr("PGSSLMODE", "disable"),
	}
	if cfg.PGDatabase == "" {
		cfg.PGDatabase = cfg.PGUser
	}
	return cfg, nil
}
