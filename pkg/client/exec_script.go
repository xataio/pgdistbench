package client

import (
	"context"
	"fmt"

	"pgdistbench/api/benchdriverapi"
	"pgdistbench/pkg/stats"
)

type ScriptReport struct {
	Runners          []benchdriverapi.ScriptRunStats `json:"runners"`
	AggregatedFields []AggregatedFieldSummary        `json:"aggregated_fields,omitempty"`
}

type AggregatedFieldSummary struct {
	FieldName string            `json:"field_name"`
	Stats     stats.DistMetrics `json:"stats"`
}

type BenchmarkScriptInstance BenchmarkInstance

func (inst *BenchmarkInstance) Script() *BenchmarkScriptInstance {
	return (*BenchmarkScriptInstance)(inst)
}

func (script *BenchmarkScriptInstance) access() *BenchmarkInstance {
	return (*BenchmarkInstance)(script)
}

// Result collects results from the specified script phase
func (script *BenchmarkScriptInstance) Result(ctx context.Context, phase string, wait bool, allowErr bool) (report ScriptReport, err error) {
	var taskName benchdriverapi.TaskName
	switch phase {
	case "prepare":
		taskName = benchdriverapi.TaskScriptPrepare
	case "run":
		taskName = benchdriverapi.TaskScriptRun
	case "cleanup":
		taskName = benchdriverapi.TaskScriptCleanup
	default:
		return report, fmt.Errorf("unknown script phase: %s", phase)
	}
	return script.collectScriptResults(ctx, taskName, wait, allowErr)
}

// collectScriptResults is a generic function to collect results from any script task
func (script *BenchmarkScriptInstance) collectScriptResults(ctx context.Context, taskName benchdriverapi.TaskName, wait bool, allowErr bool) (report ScriptReport, err error) {
	type scriptCollector = runResultCollector[benchdriverapi.ScriptWorkerStatus]

	inst := script.access()
	collector := scriptCollector{
		restConfig: inst.restConfig,
		name:       taskName,
		path:       []string{"status"},
		Validate:   ValidateStatus[benchdriverapi.ScriptRunStats](taskName, allowErr),
		Decoder:    JSONDecoder[benchdriverapi.ScriptWorkerStatus],
	}
	status, err := collector.Collect(ctx, inst.EachPodProxyURL, wait)
	if err != nil {
		return report, err
	}

	results := benchdriverapi.CollectValues(status)

	report = ScriptReport{
		Runners: results,
	}

	// Aggregate field statistics across all runners
	fieldMap := make(map[string][]float64)
	for _, r := range results {
		for _, fieldStats := range r.AggregatedStats {
			if fieldStats.Avg > 0 {
				fieldMap[fieldStats.FieldName] = append(fieldMap[fieldStats.FieldName], fieldStats.Avg)
			}
		}
	}

	// Create distribution metrics for each field
	for fieldName, values := range fieldMap {
		if len(values) > 0 {
			report.AggregatedFields = append(report.AggregatedFields, AggregatedFieldSummary{
				FieldName: fieldName,
				Stats:     stats.DistMetricStatsFrom(values, func(v float64) float64 { return v }),
			})
		}
	}

	return report, nil
}
