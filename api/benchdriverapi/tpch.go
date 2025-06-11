package benchdriverapi

type TPCHWorkerStatus = WorkerStatus[Result[TPCHRunStats]]

const (
	TaskTPCHPrepare TaskName = "tpch/prepare"
	TaskTPCHRun     TaskName = "tpch/run"
	TaskTPCHCleanup TaskName = "tpch/cleanup"
)

type BenchmarkGoTPCHConfig struct {
	// Check database after prepare phase. Default: false.
	Check *bool `json:"check"`

	// Drop all data when preparing the database. Default: false.
	DropData *bool `json:"drop_data"`

	// Transaction isolation level. Default: default.
	IsolationLevel *IsolationLevel `json:"isolation_level"`

	// ScaleFactor for the TPCH benchmark. Default: 1.0.
	// The scale factor is used to determine the size of the database. A scale
	// factor of 1.0 corresponds to a database size of 1GB.
	ScaleFactor *int `json:"scale_factor"`

	// List of queries to run
	Queries []string `json:"queries,omitempty"`

	// Number of runs per query
	Count *int `json:"count"`

	// Tune queries by setting some session variables known effective for tpch (default true)
	EnableQueryTuning *bool `json:"enable_query_tuning"`

	// Analyze tables after data loaded. Default: true.
	Analyze *bool `json:"analyze"`
}

type TPCHRunStats struct {
	List []QueryStats `json:"list"`
}

type QueryStats struct {
	Query        string     `json:"Query"`
	Count        int        `json:"Count"`
	Measurements []Duration `json:"Measurements"`
}
