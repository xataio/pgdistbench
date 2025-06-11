package tpcc

import (
	"context"
	"errors"
	"time"

	"pgdistbench/api/benchdriverapi"
	"pgdistbench/internal/worker"
	"pgdistbench/internal/worker/gotpcutil"
	"pgdistbench/pkg/timeutil"

	"github.com/pingcap/go-tpc/tpcc"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
)

type Factory struct {
	cfg     worker.Config
	metrics *gotpcutil.LazyMetrics[*tpccMetrics]
}

type BenchRunTask struct {
	*Tester
	cfg      tpcc.Config
	count    int
	duration time.Duration
	check    bool
}

func (t *BenchRunTask) Run(ctx context.Context) (benchdriverapi.TPCCRunStats, error) {
	return t.Tester.Run(ctx, t.cfg, t.count, t.duration, t.check)
}

func NewFactory(cfg worker.Config) *Factory {
	return &Factory{
		cfg:     cfg,
		metrics: nil,
	}
}

func (f *Factory) WithMetrics(r prometheus.Registerer) *Factory {
	f.metrics = gotpcutil.NewLazyMetrics(new(tpccMetrics), r)
	return f
}

func (f *Factory) Prepare(req benchdriverapi.BenchmarkGoTPCCConfig) (cmd worker.Task, err error) {
	cfg, err := TPCCConfigFromAPI(&req)
	if err != nil {
		return cmd, err
	}
	check := benchdriverapi.GetOptValue(req.Check, false)
	dropData := benchdriverapi.GetOptValue(req.DropData, false)

	bench, err := New(f.cfg)
	if err != nil {
		return cmd, nil
	}
	return worker.Task{
		Name:       benchdriverapi.TaskTPCCPrepare,
		CheckReady: bench.Ping,
		Task: func(ctx context.Context) (any, error) {
			if dropData {
				var err error
				if f.metrics != nil {
					err = runManagementOp(&f.metrics.Register().Cleanup, func() error {
						return bench.Cleanup(ctx)
					})
				} else {
					err = bench.Cleanup(ctx)
				}
				if err != nil {
					return nil, err
				}
			}

			var err error
			if f.metrics != nil {
				err = runManagementOp(&f.metrics.Register().Prepare, func() error {
					return bench.Prepare(ctx, cfg, check)
				})
			} else {
				err = bench.Prepare(ctx, cfg, check)
			}
			return nil, err
		},
	}, nil
}

func (f *Factory) Cleanup() (cmd worker.Task, err error) {
	bench, err := New(f.cfg)
	if err != nil {
		return cmd, nil
	}

	return worker.Task{
		Name: benchdriverapi.TaskTPCCCleanup,
		Task: func(ctx context.Context) (any, error) {
			var err error
			if f.metrics != nil {
				err = runManagementOp(&f.metrics.Register().Cleanup, func() error {
					return bench.Cleanup(ctx)
				})
			} else {
				err = bench.Cleanup(ctx)
			}
			return nil, err
		},
	}, nil
}

func runManagementOp(statsGroup *managementOps, fn func() error) error {
	start := time.Now()
	err := fn()
	dur := time.Since(start)

	if err == nil {
		statsGroup.Ok.Inc()
		statsGroup.Duration.Observe(dur.Seconds())
	} else {
		statsGroup.Err.Inc()
	}
	return err
}

func (f *Factory) Run(requ benchdriverapi.BenchmarkGoTPCCConfig) (cmd worker.Task, err error) {
	t, err := f.RunStress(requ)
	if err != nil {
		return cmd, err
	}

	minAge := benchdriverapi.GetOptValue(requ.LiveMinAge, benchdriverapi.Duration{Duration: 0 * time.Second}).Duration
	liveStatsPeriod := benchdriverapi.GetOptValue(requ.LiveStatsPeriod, benchdriverapi.Duration{Duration: 10 * time.Second}).Duration

	return worker.Task{
		Name:       benchdriverapi.TaskTPCCRun,
		CheckReady: t.Ping,
		Task: func(ctx context.Context) (any, error) {
			if f.metrics == nil {
				return t.Run(ctx)
			}

			var summary benchdriverapi.TPCCRunStats
			statsGroup := f.metrics.Register()

			eg, ctx := errgroup.WithContext(ctx)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			eg.Go(func() error {
				start := time.Now()
				for range timeutil.IterTick(ctx, liveStatsPeriod) {
					if time.Since(start) >= minAge {
						statsGroup.Run.LiveStats.Observe(t.LiveStats())
					}
				}
				return nil
			})
			eg.Go(func() error {
				defer cancel() // need to stop metrics worker once we are done
				summary, err = t.Run(ctx)
				return err
			})

			if err := eg.Wait(); err != nil && !errors.Is(err, context.Canceled) {
				return summary, err
			}

			if len(summary.Stats) > 0 && summary.Summary.TPMC > 0 && summary.Summary.TPMTotal > 0 && summary.Summary.Efficiency > 0 {
				statsGroup.Run.SummaryStats.Efficiency.Observe(float64(summary.Summary.Efficiency))
				statsGroup.Run.SummaryStats.TPMC.Observe(float64(summary.Summary.TPMC))
				statsGroup.Run.SummaryStats.TPMTotal.Observe(float64(summary.Summary.TPMTotal))
				for _, op := range summary.Stats {
					statsGroup.Run.SummaryStats.OpMetricsCount.WithLabelValues(op.Operation).Observe(float64(op.Count))
					statsGroup.Run.SummaryStats.OpMetricsAvg.WithLabelValues(op.Operation).Observe(float64(op.Avg))
					statsGroup.Run.SummaryStats.OpMetricsMedian.WithLabelValues(op.Operation).Observe(float64(op.Median))
					statsGroup.Run.SummaryStats.OpMetricsMax.WithLabelValues(op.Operation).Observe(float64(op.Max))
				}
			}

			return summary, nil
		},
	}, nil
}

func (f *Factory) RunStress(requ benchdriverapi.BenchmarkGoTPCCConfig) (*BenchRunTask, error) {
	cfg, err := TPCCConfigFromAPI(&requ)
	if err != nil {
		return nil, err
	}
	count := benchdriverapi.GetOptValue(requ.Count, 0)
	duration := benchdriverapi.GetOptValue(requ.Duration, benchdriverapi.Duration{Duration: 5 * time.Minute}).Duration
	check := benchdriverapi.GetOptValue(requ.Check, false)

	bench, err := New(f.cfg)
	if err != nil {
		return nil, err
	}
	return &BenchRunTask{
		Tester:   &bench,
		cfg:      cfg,
		count:    count,
		duration: duration,
		check:    check,
	}, nil
}
