package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"runtime"
	"runtime/debug"
	"time"

	"pgdistbench/api/benchdriverapi"
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

// getVersionInfo extracts version information from build info
func getVersionInfo() benchdriverapi.VersionInfo {
	info := benchdriverapi.VersionInfo{
		Version:   "dev",
		Commit:    "unknown",
		GoVersion: runtime.Version(),
	}

	buildInfo, ok := debug.ReadBuildInfo()
	if !ok {
		return info
	}

	// Get main module info
	info.MainModule = buildInfo.Main.Path
	if buildInfo.Main.Version != "" && buildInfo.Main.Version != "(devel)" {
		info.Version = buildInfo.Main.Version
	}

	// Extract VCS information from build settings
	for _, setting := range buildInfo.Settings {
		switch setting.Key {
		case "vcs.revision":
			info.Commit = setting.Value
		case "vcs.time":
			if t, err := time.Parse(time.RFC3339, setting.Value); err == nil {
				info.BuildTime = t
			}
		case "vcs.modified":
			info.DirtyBuild = setting.Value == "true"
		}
	}

	// If we have a dirty build, indicate it in the commit
	if info.DirtyBuild && info.Commit != "unknown" {
		info.Commit += "+dirty"
	}

	return info
}

func main() {
	// Get version information
	versionInfo := getVersionInfo()

	// Print version information at startup
	log.Printf("pgdistbench benchdriver starting")
	log.Printf("Version: %s", versionInfo.Version)
	log.Printf("Commit: %s", versionInfo.Commit)
	if !versionInfo.BuildTime.IsZero() {
		log.Printf("Build Time: %s", versionInfo.BuildTime.Format(time.RFC3339))
	}
	log.Printf("Go Version: %s", versionInfo.GoVersion)
	log.Printf("Main Module: %s", versionInfo.MainModule)
	if versionInfo.DirtyBuild {
		log.Printf("Warning: Built from dirty repository")
	}

	if err := run(versionInfo); err != nil {
		fmt.Fprintf(os.Stderr, "Failed: %v\n", err)
		os.Exit(1)
	}
}

func run(versionInfo benchdriverapi.VersionInfo) error {
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
	router.Get("/version", func(w http.ResponseWriter, r *http.Request) {
		versionHandler(w, r, versionInfo)
	})
	h.RegisterRoutes(router)

	fmt.Printf("Listening on :8080 (version %s, commit %s)\n", versionInfo.Version, versionInfo.Commit)
	defer fmt.Println("Goodbye!")
	return http.ListenAndServe(":8080", router)
}

// versionHandler returns version information as JSON
func versionHandler(w http.ResponseWriter, r *http.Request, versionInfo benchdriverapi.VersionInfo) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(versionInfo)
}

func getEnvOr(name, def string) string {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	return v
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
