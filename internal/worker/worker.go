package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"pgdistbench/api/benchdriverapi"

	_ "github.com/lib/pq"
)

type Config struct {
	PGHost     string
	PGPort     string
	PGUser     string
	PGPass     string
	PGDatabase string
	PGSSLMode  string
}

func (cfg *Config) ConnString() string {
	if cfg.PGHost == "" {
		return ""
	}

	params := map[string]string{
		"host":     cfg.PGHost,
		"port":     cfg.PGPort,
		"user":     cfg.PGUser,
		"password": cfg.PGPass,
		"dbname":   cfg.PGDatabase,
		"sslmode":  cfg.PGSSLMode,
	}

	connStr := ""
	for key, value := range params {
		if value == "" {
			continue
		}
		if connStr != "" {
			connStr += " "
		}
		connStr += fmt.Sprintf("%s=%s", key, value)
	}
	return connStr
}

func OpenDB(cfg Config) (*sql.DB, error) {
	connstr := cfg.ConnString()
	if connstr == "" {
		return nil, errors.New("No DB endpoint configured")
	}
	return sql.Open("postgres", connstr)
}

type Task struct {
	Name       benchdriverapi.TaskName
	Task       func(context.Context) (any, error)
	CheckReady func(context.Context) (bool, error)
}

func (t *Task) IsReady(ctx context.Context) (bool, error) {
	if t.CheckReady == nil {
		return true, nil
	}
	return t.CheckReady(ctx)
}

type TaskFactory[Config any] interface {
	Prepare(config Config) (Task, error)
	Cleanup() (Task, error)
	Run(config Config) (Task, error)
}
