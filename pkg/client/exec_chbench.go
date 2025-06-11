package client

import (
	"context"

	"pgdistbench/api/benchdriverapi"
	"pgdistbench/pkg/stats"
)

type CHBenchReport struct {
	Runners        []benchdriverapi.CHBenchRunStats
	TPM            stats.DistMetrics
	QueriesPerHour stats.DistMetrics `json:"queries_per_hour"`
	GeometricMean  stats.DistMetrics `json:"geometric_mean"`
}

type BenchmarkCHBenchInstance BenchmarkInstance

func (inst *BenchmarkInstance) CHBench() *BenchmarkCHBenchInstance {
	return (*BenchmarkCHBenchInstance)(inst)
}

func (chbench *BenchmarkCHBenchInstance) access() *BenchmarkInstance {
	return (*BenchmarkInstance)(chbench)
}

func (chbench *BenchmarkCHBenchInstance) Result(ctx context.Context, wait bool, allowErr bool) (report CHBenchReport, err error) {
	type chbenchResultCollector = runResultCollector[benchdriverapi.CHBenchWorkerStatus]

	inst := chbench.access()
	collector := chbenchResultCollector{
		restConfig: inst.restConfig,
		name:       benchdriverapi.TaskCHBenchRun,
		path:       []string{"status"},
		Validate:   ValidateStatus[benchdriverapi.CHBenchRunStats](benchdriverapi.TaskCHBenchRun, false),
		Decoder:    JSONDecoder[benchdriverapi.CHBenchWorkerStatus],
	}
	status, err := collector.Collect(ctx, inst.EachPodProxyURL, wait)
	if err != nil {
		return report, err
	}

	results := benchdriverapi.CollectValues(status)

	return CHBenchReport{
		Runners: results,
		TPM: stats.DistMetricStatsFrom(results, func(r benchdriverapi.CHBenchRunStats) float64 {
			return float64(r.OLTPStats.Summary.TPMC)
		}),
		QueriesPerHour: stats.DistMetricStatsFrom(results, func(r benchdriverapi.CHBenchRunStats) float64 {
			return r.OLAPStats.QueryPerHour
		}),
		GeometricMean: stats.DistMetricStatsFrom(results, func(r benchdriverapi.CHBenchRunStats) float64 {
			return r.OLAPStats.GeometricMean
		}),
	}, nil
}
