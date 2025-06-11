package benchdriverapi

type BenchmarkK8StressConfig struct {
	// Duration of the stress test. Test will not stop if no duration is configured.
	Duration *Duration `json:"duration"`

	Users []K8StressUser `json:"users"`
}

type K8StressUser struct {
	// Name of the user configuration.
	Name string `json:"name"`

	// Number of users with this same configuration. Default: 1.
	Copies *int `json:"copies"`

	// Minimal number of postgres clusters. Default: 1.
	MinPostgres *int `json:"min_postgres"`

	// Maximum number of postgres clusters. Default: 1.
	MaxPostgres *int `json:"max_postgres"`

	// Interval between postgres instance creation/deletion operations in seconds. Defaults to {300s, 0s}.
	UpdatesInterval *JitterDuration `json:"updates_interval"`

	PostgresConfigs []K8StressPostgresConfig `json:"configs"`

	Rampup K8StressRampup `json:"rampup"`
}

type K8StressRampup struct {
	// Duration of the ramp up phase. Default: 1s.
	CreateInterval *JitterDuration `json:"create_interval"`

	// Max number of instances to create during the initialization phase. Default: 1.
	MaxInstances *int `json:"max_instances"`

	// Min number of instances to create during the initialization phase. Default: `MaxInstances`.
	MinInstances *int `json:"min_instances"`
}

type K8StressPostgresConfig struct {
	// Weight of this postgres configuration. Default: 10.
	// The weight is used to determine the probability of selecting this configuration.
	Weight      *int      `json:"weight"`
	MinLifetime *Duration `json:"min_lifetime"`
	MaxLifetime *Duration `json:"max_lifetime"`

	Instance K8StressPostgresInstance `json:"instance"`
	Test     K8StressPostgresTest     `json:"test"`
}

type K8StressPostgresInstance struct {
	Name          *string        `json:"name"` // Name prefix of the postgres instances.
	Namespace     *string        `json:"namespace"`
	Metadata      map[string]any `json:"metadata"`       // optional CNPG Cluster meta-data
	Spec          map[string]any `json:"spec"`           // optional CNPG Cluster spec
	CreateTimeout *Duration      `json:"create_timeout"` // cluster creation timeout. Default: 1m.
}

type K8StressPostgresTest struct {
	LiveStatsPeriod *Duration `json:"live_stats_period"` // Period to collect live stats. Default: 10s.
	LiveMinAge      *Duration `json:"live_min_age"`      // Minimum age of live stats to consider. Default: 1m.

	// Transaction isolation level. Default: default.
	IsolationLevel *IsolationLevel `json:"isolation_level"`

	// Number of warehouses for the TPCC benchmark. Default: 1.
	Warehouses *int `json:"warehouses"`

	// Number of concurrently active terminals to use.
	Threads *int `json:"active_terminals"`

	// Number to partition warehouses by. Default: 1.
	Partitions    *int           `json:"partitions"`
	PartitionType *PartitionType `json:"partition_type"`

	// Use foreign keys. Default: true.
	UseForeignKeys *bool `json:"use_foreign_keys"`

	// Wait for thinking time. Default: true.
	WaitThinking *bool `json:"wait_thinking"`

	// Duration of the benchmark. Defaults to 5m.
	Duration *Duration `json:"duration"`
}

type StressRunStats struct{}

const (
	TaskStressPrepare TaskName = "stressk8/prepare"
	TaskStressCleanup TaskName = "stressk8/cleanup"
	TaskStressRun     TaskName = "stressk8/run"
)
