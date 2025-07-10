import .cnpg

schema System:
    name: str
    namespace?: str
    kind: str
    count: int = 1

schema CNPGSystem(System):
    count: int = 1
    metadata?: {:}
    spec?: cnpg.Cluster
    # TODO checks

    kind: str = "CNPG"
    check:
        kind == "CNPG", "kind must be 'CNPG'"

schema StaticCluster(System):
    metadata?: {:}
    spec?: StaticClusterSettings

    kind: str = "static"
    count: int = 1
    check:
        kind == "static", "kind must be 'static'"
        count == 1, "count must be 1"

schema StaticClusterSettings:
    host: str
    port: int = 5432
    user: str = "postgres"
    password: str = "postgres"
    database: str = user
    sslmode: str = "disable"
    updateMissingOnly: bool = False

schema Runner:
    name: str
    namespace?: str
    systems?: [SystemRef] # standalone runner for system tests
    metadata?: {:}
    spec?: {:}
    resources?: {:}
    image?: str
    imagePullPolicy?: str
    check:
        not imagePullPolicy or imagePullPolicy in ["Always", "IfNotPresent", "Never"], "imagePullPolicy must be one of 'Always', 'IfNotPresent', 'Never'"

schema SystemRef:
    name: str
    namespace?: str
    kind: str

schema Benchmark:
    name: str
    runner: RunnerRef
    config: TPCCConfig | TPCHConfig | CHBenchConfig | K8StressConfig | ScriptConfig

    _benchmark: str
    if typeof(config) == "TPCCConfig":
        _benchmark = "tpcc"
    elif typeof(config) == "TPCHConfig":
        _benchmark = "tpch"
    elif typeof(config) == "CHBenchConfig":
        _benchmark = "chbench"
    elif typeof(config) == "K8StressConfig":
        _benchmark = "k8stress"
    elif typeof(config) == "ScriptConfig":
        _benchmark = "script"
    else:
        _benchmark = Undefined
    benchmark: str = _benchmark

schema TPCCBenchmark(Benchmark):
    config: TPCCConfig
    benchmark: str = "tpcc"
    
schema TPCHBenchmark(Benchmark):
    config: TPCHConfig
    benchmark: str = "tpch"

schema RunnerRef:
    name : str
    namespace?: str

schema TPCCConfig:
    verify?: bool
    warehouses?: int
    count?: int
    active_terminals?: int
    duration?: str
    wait_thinking?: bool
    isolation_level?: str
    partitions?: int
    partition_type?: str
    use_foreign_keys?: bool

schema TPCHConfig:
    analyze?: bool # run "ANALYZE" after creating the test tables
    scale_factor?: int
    queries?: [str]
    count?: int # number of runs per query
    enable_query_tuning?: bool

    # TODO checks

schema CHBenchConfig:
    # OLTP settings
    warehouses?: int
    active_terminals?: int
    duration?: str
    isolation_level?: str
    partitions?: int
    partition_type?: str
    use_foreign_keys?: bool

    olap_threads?: int
    analyze?: bool
    queries?: [str]
    wait_olap?: bool
    wait_olap_count?: int

schema K8StressConfig:
    duration?: str
    users: [K8StressUserProfile]

schema K8StressUserProfile:
    name: str
    copies?: int # number of active user profiles with this same configuration
    min_postgres?: int # min number of active postgres instances
    max_postgres?: int # max number of active postgres instances
    updates_interval?: JitteredInterval
    rampup: K8StressUersRampup
    configs: [K8StressPostgresConfig] # list of possible postgres configuration and testing profiles

schema K8StressUersRampup:
    create_interval?: JitteredInterval
    min_instances?: int
    max_instances?: int

schema JitteredInterval:
    duration: str
    jitter: str

schema K8StressPostgresConfig:
    weight?: int

    # min time the postgres instance will be active
    min_lifetime?: str

    # max time the postgres instance should life. The stress tester will select
    # an outlived instance with higher chance when the test decides to delete a
    # DB.
    max_lifetime?: str
    instance: K8StressPostgresInstance
    test?: K8StressTPCCConfig

schema K8StressTPCCConfig(TPCCConfig):
    live_stats_period?: str  # live stats collection period. Default 10s.
    live_min_age?: str # min instance age before we start collecting live stats from the test. default 1m.

schema K8StressPostgresInstance:
    name: str
    namespace?: str
    metadata?: {:}
    annotations?: {:}
    spec: cnpg.Cluster

schema FieldAggregationConfig:
    aggregations: [str]
    check:
        all a in aggregations {
            a in ["avg", "sum", "min", "max", "count"]
        }, "Invalid aggregation type in aggregations"

schema ErrorDetectionConfig:
    sources: [str]
    patterns: [str]
    override_exit_code: bool = True
    check:
        all source in sources {
            source in ["stdout", "stderr"]
        }, "Error detection sources must be 'stdout' or 'stderr'"
        len(patterns) > 0, "At least one error pattern must be specified"

schema ScriptConfig:
    scripts_path?: str
    prepare_cmd?: str
    run_cmd?: str
    cleanup_cmd?: str
    parameters?: {str: str}
    duration?: str
    output_format: str = "log"
    aggregation_fields?: {str: FieldAggregationConfig}
    csv_headers?: [str]
    collect_raw_records: bool = False
    error_detection?: ErrorDetectionConfig
    check:
        output_format in ["log", "json", "csv"], "output_format must be one of 'log', 'json', 'csv'"

schema ScriptBenchmark(Benchmark):
    config: ScriptConfig
    benchmark: str = "script"


systemRefList = lambda systems:[System] {
    [SystemRef {
        name = s.name,
        namespace = s.namespace,
        kind = s.kind,
    } for s in systems]
}

runnerRef = lambda runner:Runner {
    RunnerRef {
        name = runner.name,
        namespace = runner.namespace,
    }
}
