package benchdriverapi

type CHBenchWorkerStatus = WorkerStatus[Result[CHBenchRunStats]]

type BenchmarkCHBenchConfig struct {
	// Check database after prepare phase. Default: true.
	Check *bool `json:"check"`

	DropData *bool `json:"drop_data"`

	// Transaction isolation level. Default: default.
	IsolationLevel *IsolationLevel `json:"isolation_level"`

	// Number of warehouses for the TPCC benchmark. Default: 1.
	Warehouses *int `json:"warehouses"`

	// Number of concurrently active transactions. Note: chbench has no thinking or waiting times.
	Threads *int `json:"threads"`

	// Number of concurrent active OLAP queries. Default: 1.
	OlapThreads *int `json:"olap_threads"`

	// Number to partition warehouses by. Default: 1.
	Partitions    *int           `json:"partitions"`
	PartitionType *PartitionType `json:"partition_type"`

	// Use foreign keys. Default: true.
	UseForeignKeys *bool `json:"use_foreign_keys"`

	// List of queries to run
	Queries []string `json:"queries,omitempty"`

	// Duration of the benchmark. Defaults to 5m.
	Duration      *Duration `json:"duration"`
	WaitOLAP      *bool     `json:"wait_olap"`       // if true we wait for olap queries to finish before exiting
	WaitOLAPCount *int      `json:"wait_olap_count"` // Minimal number of total OLAP queries we want to run to completion before exiting the run. If a timeout is configured we will continue the test until all olap queries are completed

	// Analyze tables after data loaded. Default: true.
	Analyze *bool `json:"analyze"`
}

const (
	TaskCHBenchPrepare TaskName = "chbench/prepare"
	TaskCHBenchCleanup TaskName = "chbench/cleanup"
	TaskCHBenchRun     TaskName = "chbench/run"
)

type CHBenchRunStats struct {
	OLTPStats TPCCRunStats     `json:"oltp_stats"`
	OLAPStats CHBenchOlapStats `json:"olap_stats"`
}

type CHBenchOlapStats struct {
	Queries          []OpStats `json:"queries"`
	GeometricMean    float64   `json:"geometric_mean"`
	QuerySetDuration float64   `json:"query_set_duration"`
	QueryPerHour     float64   `json:"query_per_hour"`
}
