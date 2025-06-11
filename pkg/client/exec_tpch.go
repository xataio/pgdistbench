package client

import (
	"context"
	"maps"
	"slices"
	"time"

	"pgdistbench/api/benchdriverapi"
	"pgdistbench/pkg/stats"
)

type TPCHReport struct {
	Runners        []TPCHRunnerStats `json:"runners"`
	QueriesPerHour stats.DistMetrics `json:"queries_per_hour"`
	GeometricMean  stats.DistMetrics `json:"geometric_mean"`
}

type TPCHRunnerStats struct {
	Operations     map[string]TPCHOpStats `json:"operations"`
	Total          Duration               `json:"total"`            // total time of all queries
	QuerySetTotal  Duration               `json:"query_set_total"`  // sum of average time of all queries
	GeometricMean  float64                `json:"geometric_mean"`   // geometric mean of average query times
	QueriesPerHour float64                `json:"queries_per_hour"` // number of queries per hour
}

type TPCHOpStats struct {
	Measurements []Duration `json:"measurements"`
	Min          Duration   `json:"min"`
	Max          Duration   `json:"max"`
	Avg          float64    `json:"avg"`
	Median       Duration   `json:"median"`
	Stddev       float64    `json:"stddev"`
}

type Duration = benchdriverapi.Duration

type BenchmarkTPCHInstance BenchmarkInstance

func (inst *BenchmarkInstance) TPCH() *BenchmarkTPCHInstance {
	return (*BenchmarkTPCHInstance)(inst)
}

func (tpch *BenchmarkTPCHInstance) access() *BenchmarkInstance {
	return (*BenchmarkInstance)(tpch)
}

func (tpch *BenchmarkTPCHInstance) Result(ctx context.Context, wait bool, allowErr bool) (report TPCHReport, err error) {
	type tpchCollector = runResultCollector[benchdriverapi.TPCHWorkerStatus]

	inst := tpch.access()
	collector := tpchCollector{
		restConfig: inst.restConfig, name: benchdriverapi.TaskTPCHRun,
		path:     []string{"status"},
		Validate: ValidateStatus[benchdriverapi.TPCHRunStats](benchdriverapi.TaskTPCHRun, allowErr),
		Decoder:  JSONDecoder[benchdriverapi.TPCHWorkerStatus],
	}
	results, err := collector.Collect(ctx, inst.EachPodProxyURL, wait)
	if err != nil {
		return report, err
	}

	runnerStats := make([]TPCHRunnerStats, 0, len(results))
	for _, r := range results {
		if r.Last.Error == nil {
			runnerStats = append(runnerStats, computeTPCHRunnerStats(r.Last.Value))
		}
	}

	return TPCHReport{
		Runners:        runnerStats,
		QueriesPerHour: stats.DistMetricStatsFrom(runnerStats, func(r TPCHRunnerStats) float64 { return r.QueriesPerHour }),
		GeometricMean:  stats.DistMetricStatsFrom(runnerStats, func(r TPCHRunnerStats) float64 { return r.GeometricMean }),
	}, nil
}

func computeTPCHRunnerStats(r benchdriverapi.TPCHRunStats) TPCHRunnerStats {
	ops := make(map[string]TPCHOpStats)
	for _, q := range r.List {
		op := ops[q.Query]
		op.Measurements = append(op.Measurements, q.Measurements...)
		ops[q.Query] = op
	}

	asMillis := func(d Duration) float64 { return d.Seconds() * 1000 }

	for k, op := range ops {
		if len(op.Measurements) == 0 {
			continue
		}

		slices.SortFunc(op.Measurements, func(a, b Duration) int {
			return int(a.Duration - b.Duration)
		})
		op.Min = op.Measurements[0]
		op.Max = op.Measurements[len(op.Measurements)-1]
		op.Median = op.Measurements[len(op.Measurements)/2]
		op.Avg = stats.SliceAverageFunc(op.Measurements, asMillis)
		op.Stddev = stats.SliceStddevFunc(op.Measurements, asMillis)
		ops[k] = op
	}

	var total Duration
	var querySetTotal Duration
	for _, op := range ops {
		for _, m := range op.Measurements {
			total.Duration += m.Duration
		}
		querySetTotal.Duration += time.Duration(op.Avg * float64(time.Millisecond))
	}

	geom := stats.GeometricMeanFunc(maps.Values(ops), func(op TPCHOpStats) float64 { return op.Avg })
	return TPCHRunnerStats{
		Operations:     ops,
		Total:          total,
		QuerySetTotal:  querySetTotal,
		QueriesPerHour: 3600 / querySetTotal.Seconds() * float64(len(ops)),
		GeometricMean:  geom,
	}
}
