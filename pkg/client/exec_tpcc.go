package client

import (
	"context"

	"pgdistbench/api/benchdriverapi"
	"pgdistbench/pkg/stats"
)

type TPCCReport struct {
	Runners  []benchdriverapi.TPCCRunStats
	TPMC     stats.DistMetrics
	TPMTotal stats.DistMetrics
}

type BenchmarkTPCCInstance BenchmarkInstance

func (inst *BenchmarkInstance) TPCC() *BenchmarkTPCCInstance {
	return (*BenchmarkTPCCInstance)(inst)
}

func (tpcc *BenchmarkTPCCInstance) access() *BenchmarkInstance {
	return (*BenchmarkInstance)(tpcc)
}

func (tpcc *BenchmarkTPCCInstance) Result(ctx context.Context, wait bool, allowErr bool) (report TPCCReport, err error) {
	type tpccCollector = runResultCollector[benchdriverapi.TPCCWorkerStatus]

	validator := ValidateStatus[benchdriverapi.TPCCRunStats](benchdriverapi.TaskTPCCRun, allowErr)

	inst := tpcc.access()
	collector := tpccCollector{
		restConfig: inst.restConfig,
		name:       benchdriverapi.TaskTPCCRun,
		path:       []string{"status"},
		Validate:   validator,
		Decoder:    JSONDecoder[benchdriverapi.TPCCWorkerStatus],
	}
	status, err := collector.Collect(ctx, inst.EachPodProxyURL, wait)
	if err != nil {
		return report, err
	}
	results := benchdriverapi.CollectValues(status)

	return TPCCReport{
		Runners:  results,
		TPMC:     stats.DistMetricStatsFrom(results, func(r benchdriverapi.TPCCRunStats) float64 { return float64(r.Summary.TPMC) }),
		TPMTotal: stats.DistMetricStatsFrom(results, func(r benchdriverapi.TPCCRunStats) float64 { return float64(r.Summary.TPMTotal) }),
	}, nil
}
