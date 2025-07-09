package benchdriverapi

type ScriptWorkerStatus = WorkerStatus[Result[ScriptRunStats]]

const (
	TaskScriptPrepare TaskName = "script/prepare"
	TaskScriptRun     TaskName = "script/run"
	TaskScriptCleanup TaskName = "script/cleanup"
)

type AggregationType string

const (
	AggAvg   AggregationType = "avg"
	AggSum   AggregationType = "sum"
	AggMin   AggregationType = "min"
	AggMax   AggregationType = "max"
	AggCount AggregationType = "count"
	AggP99   AggregationType = "p99"
)

type OutputFormat string

const (
	FormatLog  OutputFormat = "log"  // Default, just captures stdout
	FormatJSON OutputFormat = "json" // Expects JSON objects per line
	FormatCSV  OutputFormat = "csv"  // Expects a CSV with a header
)

type FieldAggregationConfig struct {
	// List of aggregations to perform on this field.
	Aggregations []AggregationType `json:"aggregations"`
}

type BenchmarkScriptConfig struct {
	// A directory containing the benchmark scripts. This directory will be mounted
	// into the worker pod, for example, via a Kubernetes ConfigMap.
	// The commands will be executed from this directory.
	ScriptsPath *string `json:"scripts_path"`

	// The command to execute for the 'prepare' phase. The command string
	// supports full shell syntax including environment variable expansion (e.g. $VAR or ${VAR}).
	PrepareCmd string `json:"prepare_cmd"`

	// The command to execute for the 'run' phase. The command string
	// supports full shell syntax including environment variable expansion.
	RunCmd string `json:"run_cmd"`

	// The command to execute for the 'cleanup' phase. The command string
	// supports full shell syntax including environment variable expansion.
	CleanupCmd string `json:"cleanup_cmd"`

	// A map of parameters that will be exposed as environment variables
	// to the script execution environment.
	Parameters map[string]string `json:"parameters"`

	// Duration of the benchmark. This will be exposed as the BENCH_DURATION
	// environment variable (e.g. "300s").
	Duration *string `json:"duration"`

	// Defines the format of the script's standard output.
	// "log" (default), "json", or "csv".
	OutputFormat *OutputFormat `json:"output_format"`

	// Defines how to aggregate fields from the structured output (json/csv).
	// The key is the field name from the output.
	AggregationFields map[string]FieldAggregationConfig `json:"aggregation_fields"`

	// CSV-specific configuration: explicit column headers for CSV processing.
	// If provided, the first line of CSV output will be treated as data (not headers).
	// If not provided, the first line will be treated as column headers.
	// If not provided and first line is not valid headers, columns will be named "1", "2", "3", etc.
	CSVHeaders []string `json:"csv_headers,omitempty"`
}

type AggregatedFieldStats struct {
	FieldName string  `json:"field_name"`
	Count     int64   `json:"count"`
	Sum       float64 `json:"sum,omitempty"`
	Avg       float64 `json:"avg,omitempty"`
	Min       float64 `json:"min,omitempty"`
	Max       float64 `json:"max,omitempty"`
	P99       float64 `json:"p99,omitempty"`
}

type ScriptRunStats struct {
	AggregatedStats []AggregatedFieldStats `json:"aggregated_stats,omitempty"`
	// Command execution results
	Stdout   string `json:"stdout,omitempty"`
	Stderr   string `json:"stderr,omitempty"`
	ExitCode int    `json:"exit_code"`
}
