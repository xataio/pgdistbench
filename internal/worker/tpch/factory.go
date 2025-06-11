package tpch

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

func (f *Factory) Prepare(req benchdriverapi.BenchmarkGoTPCHConfig) (cmd worker.Task, err error) {
	cfg, err := TPCHConfigFromAPI(&req)
	if err != nil {
		return cmd, err
	}
	dropData := benchdriverapi.GetOptValue(req.DropData, false)

	return worker.Task{
		Name: benchdriverapi.TaskTPCHPrepare,
		Task: f.benchTPCHTask(func(ctx context.Context, bench *Tester) (any, error) {
			return nil, bench.Prepare(ctx, cfg, dropData)
		}),
	}, nil
}

func (f *Factory) Cleanup() (worker.Task, error) {
	return worker.Task{
		Name: benchdriverapi.TaskTPCHCleanup,
		Task: f.benchTPCHTask(func(ctx context.Context, bench *Tester) (any, error) {
			return nil, bench.Cleanup(ctx)
		}),
	}, nil
}

func (f *Factory) Run(requ benchdriverapi.BenchmarkGoTPCHConfig) (cmd worker.Task, err error) {
	cfg, err := TPCHConfigFromAPI(&requ)
	if err != nil {
		return cmd, err
	}
	count := benchdriverapi.GetOptValue(requ.Count, 1)

	return worker.Task{
		Name: benchdriverapi.TaskTPCHRun,
		Task: f.benchTPCHTask(func(ctx context.Context, bench *Tester) (any, error) {
			return bench.Run(ctx, cfg, count)
		}),
	}, nil
}

func (f *Factory) benchTPCHTask(fn func(context.Context, *Tester) (any, error)) func(context.Context) (any, error) {
	return func(ctx context.Context) (any, error) {
		bench, err := New(f.cfg)
		if err != nil {
			return nil, err
		}
		defer bench.Close()
		return fn(ctx, &bench)
	}
}
