package chbench

import (
	"context"
	"time"

	"github.com/pingcap/go-tpc/tpcc"

	"pgdistbench/api/benchdriverapi"
	"pgdistbench/internal/worker"
)

type Factory struct {
	cfg worker.Config
}

func NewFactory(cfg worker.Config) *Factory {
	return &Factory{cfg: cfg}
}

func (f *Factory) Prepare(req benchdriverapi.BenchmarkCHBenchConfig) (cmd worker.Task, err error) {
	tpccConfig, err := TPCCConfigFromCHBenchAPI(&req)
	if err != nil {
		return cmd, err
	}
	olapConfig, err := CHBenchConfigFromAPI(&req)
	if err != nil {
		return cmd, err
	}
	check := benchdriverapi.GetOptValue(req.Check, true)
	dropData := benchdriverapi.GetOptValue(req.DropData, false)

	return worker.Task{
		Name: benchdriverapi.TaskCHBenchPrepare,
		Task: f.benchCHBenchTask(func(ctx context.Context, bench *Tester) (any, error) {
			return nil, bench.Prepare(ctx, tpccConfig, olapConfig, check, dropData)
		}),
	}, nil
}

func (f *Factory) Cleanup() (worker.Task, error) {
	return worker.Task{
		Name: benchdriverapi.TaskCHBenchCleanup,
		Task: f.benchCHBenchTask(func(ctx context.Context, bench *Tester) (any, error) {
			return nil, bench.Cleanup(ctx)
		}),
	}, nil
}

func (f *Factory) Run(requ benchdriverapi.BenchmarkCHBenchConfig) (cmd worker.Task, err error) {
	tpccConfig, err := TPCCConfigFromCHBenchAPI(&requ)
	if err != nil {
		return cmd, err
	}
	tpccConfig.OutputType = "json"
	_, err = tpcc.NewWorkloader(nil, &tpccConfig)
	if err != nil {
		return cmd, err
	}

	defaultTimeout := 5 * time.Minute
	olapConfig, err := CHBenchConfigFromAPI(&requ)
	if err != nil {
		return cmd, err
	}

	waitOlAPCount := benchdriverapi.GetOptValue(requ.WaitOLAPCount, 0)
	waitOlap := benchdriverapi.GetOptValue(requ.WaitOLAP, waitOlAPCount > 0)
	cfg := RunConfig{
		TPCC:          tpccConfig,
		OLAP:          olapConfig,
		OlapThreads:   benchdriverapi.GetOptValue(requ.OlapThreads, 1),
		Duration:      benchdriverapi.GetOptValue(requ.Duration, benchdriverapi.Duration{Duration: defaultTimeout}).Duration,
		Check:         benchdriverapi.GetOptValue(requ.Check, false),
		WaitOLAP:      waitOlap,
		WaitOLAPCount: waitOlAPCount,
	}

	return worker.Task{
		Name: benchdriverapi.TaskCHBenchRun,
		Task: f.benchCHBenchTask(func(ctx context.Context, bench *Tester) (any, error) {
			return bench.Run(ctx, cfg)
		}),
	}, nil
}

func (f *Factory) benchCHBenchTask(fn func(context.Context, *Tester) (any, error)) func(context.Context) (any, error) {
	return func(ctx context.Context) (any, error) {
		bench, err := newBenchCHBench(f.cfg)
		if err != nil {
			return nil, err
		}
		defer bench.Close()
		return fn(ctx, &bench)
	}
}
