package k8stress

import (
	"context"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/client-go/rest"

	"pgdistbench/api/benchdriverapi"
	"pgdistbench/internal/worker"
	"pgdistbench/internal/worker/gotpcutil"
)

type Factory struct {
	restConfig *rest.Config
	metrics    *gotpcutil.LazyMetrics[*stressMetrics]
}

func NewFactory(restConfig *rest.Config) *Factory {
	return &Factory{
		restConfig: restConfig,
		metrics:    gotpcutil.NewLazyMetrics(new(stressMetrics), prometheus.DefaultRegisterer),
	}
}

func (f *Factory) WithMetrics(r prometheus.Registerer) *Factory {
	f.metrics.WithRegistrar(r)
	return f
}

func (f *Factory) Prepare(req benchdriverapi.BenchmarkK8StressConfig) (cmd worker.Task, err error) {
	return worker.Task{
		Name: benchdriverapi.TaskStressPrepare,
		Task: f.stressTask(func(ctx context.Context, s *Tester) (any, error) {
			return nil, s.Prepare(ctx, req)
		}),
	}, nil
}

func (f *Factory) Cleanup() (worker.Task, error) {
	return worker.Task{
		Name: benchdriverapi.TaskStressCleanup,
		Task: f.stressTask(func(ctx context.Context, s *Tester) (any, error) {
			return nil, s.Cleanup(ctx)
		}),
	}, nil
}

func (f *Factory) Run(req benchdriverapi.BenchmarkK8StressConfig) (cmd worker.Task, err error) {
	return worker.Task{
		Name: benchdriverapi.TaskStressRun,
		Task: f.stressTask(func(ctx context.Context, s *Tester) (any, error) {
			return s.Run(ctx, req)
		}),
	}, nil
}

func (f *Factory) stressTask(fn func(context.Context, *Tester) (any, error)) func(context.Context) (any, error) {
	return func(ctx context.Context) (any, error) {
		metricsGroup := f.metrics.Register()
		s := New(f.restConfig, metricsGroup)
		return fn(ctx, s)
	}
}

type LazyFactory struct {
	factory          *Factory
	metricsRegistrar prometheus.Registerer

	err error

	init          sync.Once
	getRestConfig func() (*rest.Config, error)
}

func NewLazyFactory(init func() (*rest.Config, error)) *LazyFactory {
	return &LazyFactory{
		getRestConfig: init,
	}
}

func (f *LazyFactory) WithMetrics(r prometheus.Registerer) *LazyFactory {
	f.metricsRegistrar = r
	return f
}

func (f *LazyFactory) Prepare(req benchdriverapi.BenchmarkK8StressConfig) (cmd worker.Task, err error) {
	factory, err := f.getFactory()
	if err != nil {
		return cmd, err
	}
	return factory.Prepare(req)
}

func (f *LazyFactory) Cleanup() (worker.Task, error) {
	factory, err := f.getFactory()
	if err != nil {
		return worker.Task{}, err
	}
	return factory.Cleanup()
}

func (f *LazyFactory) Run(req benchdriverapi.BenchmarkK8StressConfig) (cmd worker.Task, err error) {
	factory, err := f.getFactory()
	if err != nil {
		return cmd, err
	}
	return factory.Run(req)
}

func (f *LazyFactory) getFactory() (*Factory, error) {
	f.init.Do(func() {
		rc, err := f.getRestConfig()
		if err != nil {
			f.err = err
		} else {
			f.factory = NewFactory(rc).WithMetrics(f.metricsRegistrar)
		}
	})

	if f.err != nil {
		return nil, f.err
	}
	return f.factory, nil
}
