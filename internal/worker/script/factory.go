package script

import (
	"context"

	"pgdistbench/api/benchdriverapi"
	"pgdistbench/internal/worker"
)

type Factory struct {
	cfg worker.Config
}

func NewFactory(cfg worker.Config) *Factory {
	return &Factory{cfg: cfg}
}

func (f *Factory) Prepare(req benchdriverapi.BenchmarkScriptConfig) (cmd worker.Task, err error) {
	return worker.Task{
		Name: benchdriverapi.TaskScriptPrepare,
		Task: f.benchScriptTask(func(ctx context.Context, bench *Tester) (any, error) {
			return bench.Prepare(ctx, req)
		}),
	}, nil
}

func (f *Factory) Cleanup(req benchdriverapi.BenchmarkScriptConfig) (worker.Task, error) {
	return worker.Task{
		Name: benchdriverapi.TaskScriptCleanup,
		Task: f.benchScriptTask(func(ctx context.Context, bench *Tester) (any, error) {
			return bench.Cleanup(ctx, req)
		}),
	}, nil
}

func (f *Factory) Run(req benchdriverapi.BenchmarkScriptConfig) (cmd worker.Task, err error) {
	return worker.Task{
		Name: benchdriverapi.TaskScriptRun,
		Task: f.benchScriptTask(func(ctx context.Context, bench *Tester) (any, error) {
			return bench.Run(ctx, req)
		}),
	}, nil
}

func (f *Factory) benchScriptTask(fn func(context.Context, *Tester) (any, error)) func(context.Context) (any, error) {
	return func(ctx context.Context) (any, error) {
		bench, err := New(f.cfg)
		if err != nil {
			return nil, err
		}
		defer bench.Close()
		return fn(ctx, &bench)
	}
}
