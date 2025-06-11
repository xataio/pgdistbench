package client

import (
	"context"
)

type K8StressReport struct{}

type BenchmarkK8StressInstance BenchmarkInstance

func (inst *BenchmarkInstance) K8Stress() *BenchmarkK8StressInstance {
	return (*BenchmarkK8StressInstance)(inst)
}

func (stress *BenchmarkK8StressInstance) access() *BenchmarkInstance {
	return (*BenchmarkInstance)(stress)
}

func (stress *BenchmarkK8StressInstance) Result(ctx context.Context, wait bool) (report K8StressReport, err error) {
	return report, nil
}

func (stress *BenchmarkK8StressInstance) Cleanup(ctx context.Context) error {
	return nil
}
