package benchdriverapi

type TPCCWorkerStatus = WorkerStatus[Result[TPCCRunStats]]

const (
	TaskTPCCPrepare TaskName = "tpcc/prepare"
	TaskTPCCRun     TaskName = "tpcc/run"
	TaskTPCCCleanup TaskName = "tpcc/cleanup"
)

// Result type of TPCC benchmark run.
type TPCCRunStats struct {
	Stats   []OpStats
	Summary TPCCSummary
}

type TPCCSummary struct {
	Efficiency Percentage `json:"efficiency"`
	TPMC       Sample     `json:"tpmC"`
	TPMTotal   Sample     `json:"tpmTotal"`
}

type BenchmarkGoTPCCConfig struct {
	// Check database after prepare phase. Default: true.
	Check *bool `json:"verify"`

	DropData *bool `json:"drop_data"`

	// Transaction isolation level. Default: default.
	IsolationLevel *IsolationLevel `json:"isolation_level"`

	// Number of warehouses for the TPCC benchmark. Default: 1.
	Warehouses *int `json:"warehouses"`

	// Number of concurrently active terminals to use.
	Threads *int `json:"active_terminals"`

	// Total execution count. Default: 0 (unlimited).
	Count *int `json:"count"`

	// Number to partition warehouses by. Default: 1.
	Partitions    *int           `json:"partitions"`
	PartitionType *PartitionType `json:"partition_type"`

	// Use foreign keys. Default: true.
	UseForeignKeys *bool `json:"use_foreign_keys"`

	// Wait for thinking time. Default: true.
	WaitThinking *bool `json:"wait_thinking"`

	// Duration of the benchmark. Defaults to 5m.
	Duration *Duration `json:"duration"`

	LiveStatsPeriod *Duration `json:"live_stats_period"` // Period to collect live stats. Default: 10s.
	LiveMinAge      *Duration `json:"live_min_age"`      // Minimum age of live stats to consider. Default: 1m.
}

type TPCCWeights struct {
	// Weight for the NewOrder transaction. Default: 45
	NewOrder *int `json:"new_order"`

	// NewOrder pre-execution wait time. Default: 0 (no wait)
	NewOrderWait *int `json:"new_order_wait"`

	// Weight for the Payment transaction. Default: 43
	Payment *int `json:"payment"`

	// Payment pre-execution wait time. Default: 0 (no wait)
	PaymentWait *int `json:"payment_wait"`

	// Weight for the OrderStatus transaction. Default: 4
	OrderStatus *int `json:"order_status"`

	// OrderStatus pre-execution wait time. Default: 0 (no wait)
	OderStatusWait *int `json:"order_status_wait"`

	// Weight for the Delivery transaction. Default: 4
	Delivery *int `json:"delivery"`

	// Delivery pre-execution wait time. Default: 0 (no wait)
	DeliveryWait *int `json:"delivery_wait"`

	// Weight for the StockLevel transaction. Default: 4
	StockLevel *int `json:"stock_level"`

	// StockLevel pre-execution wait time. Default: 0 (no wait)
	SockLevelWait *int `json:"stock_level_wait"`
}
