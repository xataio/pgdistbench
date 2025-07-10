# Configuration

This document provides a comprehensive guide to configuring pgdistbench for your benchmarking needs.

## Configuration Overview

pgdistbench uses a declarative configuration approach where you define:

- **Systems**: PostgreSQL instances and clusters to benchmark
- **Runners**: Benchdriver pod deployments that execute the tests
- **Benchmarks**: Test specifications and parameters

Configurations can be written in YAML or KCL, with k8runner automatically discovering and loading them from your working directory.

### YAML vs KCL

**YAML Configuration:**

- Simple, familiar syntax
- Good for straightforward scenarios
- Direct mapping to Kubernetes resources
- Examples in `demos/singledb_yaml/`

**KCL Configuration:**

- Type safety and compile-time validation
- Modular and reusable configurations
- External dependencies can be listed in the kcl.mod file
- Advanced templating and logic capabilities
- Schema definitions in `scripts/kcl/k8dbbench/`
- Examples in `demos/singledb_kcl/` and `demos/clusterload/`

### Configuration Discovery

k8runner loads configurations from:

1. Current working directory (default)
2. Directory specified with `-w/--workdir` flag
3. Specific file with `-m/--main` flag

Configuration files are typically named `main.yaml`, `main.k`, or `main.kcl`. In case of KCL, one can declare dependencies via `kcl.mod`. Use the `kcl mod init` CLI command to initialize a module and `kcl mod add ...` to add a dependency.

## Core Configuration Components

### Systems

Systems define the system under test. This can be PostgreSQL or the Kubernetes cluster itself. In the latter case, no configuration is required.

### Runners

Runners define how benchdriver pods are deployed and configured. They specify the execution environment for your benchmarks.

Runners hold references to SUTs.

### Benchmarks

Benchmarks define the actual tests to run, including parameters like duration, scale factors, and workload characteristics.

A Benchmark references a single Runner configuration used to execute the actual benchmark.

## Systems Configuration

### CNPG Systems

CloudNativePG systems create Kubernetes-native PostgreSQL clusters:

**KCL Example:**

```kcl
systems = {
  postgres_cluster = bench.CNPGSystem {
    name: "postgres-cluster"
    namespace: "benchmark"
    spec: {
      instances: 1
      storage.size: "10Gi"
      resources: {
        requests: { memory: "2Gi", cpu: "1000m" }
        limits: { memory: "4Gi", cpu: "2000m" }
      }
      postgresql: {
        parameters: {
          max_connections: "200"
          shared_buffers: "256MB"
          effective_cache_size: "1GB"
        }
      }
    }
  }
}
```

**YAML Example:**

```yaml
systems:
  postgres-cluster:
    kind: CNPG
    name: postgres-cluster
    namespace: benchmark
    spec:
      instances: 1
      storage:
        size: 10Gi
      resources:
        requests:
          memory: 2Gi
          cpu: 1000m
      postgresql:
        parameters:
          max_connections: "200"
          shared_buffers: "256MB"
```

### Static Systems

Static systems connect to pre-existing PostgreSQL instances:

The k8runner will create a Kubernetes secret when creating the system. The secret is then used by the benchdriver.

**KCL Example:**

```kcl
systems = {
  external_db = bench.StaticCluster {
    name: "external-db"
    namespace: "benchmark"
    spec: {
      host: "postgres.example.com"
      port: 5432
      user: "benchuser"
      password: "benchpass"
      database: "benchdb"
      sslmode: "require"
    }
  }
}
```

**YAML Example:**

```yaml
systems:
  external-db:
    kind: static
    name: external-db
    spec:
      host: postgres.example.com
      port: 5432
      user: benchuser
      password: benchpass
      database: benchdb
      sslmode: require
```

**Static System Parameters:**

- `host`: PostgreSQL server hostname
- `port`: PostgreSQL server port (default: 5432)
- `user`: Database username
- `password`: Database password
- `database`: Database name
- `sslmode`: SSL connection mode (disable, require, verify-ca, verify-full)

## Runners Configuration

Runners define how benchdriver pods are deployed and managed.

### Basic Runner Configuration

**KCL Example:**

```kcl
runners = {
  benchmark_runner = bench.Runner {
    name: "benchmark-runner"
    namespace: "benchmark"
    systems: bench.systemRefList([systems.postgres_cluster])
    image: "..."
    imagePullPolicy: "Always"
    spec: {
      replicas: 2
      template.spec: {
        resources: {
          requests: { cpu: "500m", memory: "1Gi" }
          limits: { cpu: "1000m", memory: "2Gi" }
        }
      }
    }
  }
}
```

**YAML Example:**

```yaml
runners:
  benchmark-runner:
    name: benchmark-runner
    namespace: benchmark
    systems:
      - name: postgres-cluster
        kind: CNPG
    image: ...
    imagePullP
    spec:
      replicas: 2
      template:
        spec:
          resources:
            requests:
              cpu: 500m
              memory: 1Gi
            limits:
              cpu: 1000m
              memory: 2Gi
```

### Runner Configuration Options

**Basic Settings:**

- `name`: Runner deployment name
- `namespace`: Kubernetes namespace
- `systems`: List of systems this runner executes the benchmark against

**Scaling Configuration:**

- `spec.replicas`: Number of benchdriver pod replicas
- `spec.template.spec.resources`: CPU and memory limits/requests

**Advanced Options:**

- `spec.template.spec.serviceAccountName`: Custom service account. Required when benchmark the k8s cluster itself.
- `spec.template.spec.nodeSelector`: Node selection constraints
- `spec.template.spec.tolerations`: Pod tolerations
- `metadata`: Additional labels and annotations

## Benchmarks Configuration

Benchmarks define the actual tests to execute, referencing runners and specifying test parameters.

### TPC-C Benchmarks

TPC-C simulates OLTP workloads with order processing transactions:

**KCL Example:**

```kcl
benchmarks = {
  tpcc_small = bench.TPCCBenchmark {
    name: "tpcc-small"
    runner: bench.runnerRef(runners.benchmark_runner)
    config: {
      warehouses: 10
      active_terminals: 50
      duration: "5m"
      wait_thinking: False
    }
  }
}
```

**TPC-C Configuration Parameters:**

- `warehouses`: Number of warehouses (scale factor)
- `active_terminals`: Concurrent database connections
- `duration`: Test duration (e.g., "5m", "1h")
- `wait_thinking`: Include thinking time between transactions
- `isolation_level`: Transaction isolation level
- `verify`: Verify data consistency after test
- `partitions`: Number of warehouse partitions
- `partition_type`: Partitioning strategy (hash, range)

### TPC-H Benchmarks

TPC-H provides OLAP workloads with complex analytical queries:

**KCL Example:**

```kcl
benchmarks = {
  tpch_analysis = bench.TPCHBenchmark {
    name: "tpch-analysis"
    runner: bench.runnerRef(runners.benchmark_runner)
    config: {
      scale_factor: 1
      queries: ["q1", "q3", "q5", "q7", "q10"]
      count: 3
      enable_query_tuning: true
      analyze: true
    }
  }
}
```

**TPC-H Configuration Parameters:**

- `scale_factor`: Database size multiplier (1 = ~1GB)
- `queries`: List of queries to execute (q1-q22)
- `count`: Number of runs per query
- `enable_query_tuning`: Apply PostgreSQL query optimizations
- `analyze`: Run ANALYZE after data loading

### CHBench Benchmarks

CHBench is based on TPC-C, adding OLAP queries similar to TPC-H to ensure we can run a mixed workload against the same tables.

**KCL Example:**

```kcl
benchmarks = {
  mixed_workload = bench.Benchmark {
    name: "mixed-workload"
    runner: bench.runnerRef(runners.benchmark_runner)
    config: bench.CHBenchConfig {
      # OLTP settings
      warehouses: 5
      active_terminals: 25
      duration: "10m"
      
      # OLAP settings
      olap_threads: 1
      queries: ["q1", "q2", "q3"]
      wait_olap: true
      wait_olap_count: 10
      analyze: true
    }
  }
}
```

### K8Stress Benchmarks

K8Stress tests Kubernetes cluster behavior with dynamic PostgreSQL lifecycles:

**KCL Example:**

```kcl
benchmarks = {
  cluster_stress = bench.Benchmark {
    name: "cluster-stress"
    runner: bench.runnerRef(runners.cluster_runner)
    config: bench.K8StressConfig {
      users: [
        {
          name: "user-profile-1"
          min_postgres: 1
          max_postgres: 5
          updates_interval: {
            duration: "60s"
            jitter: "30s"
          }
          rampup: {
            create_interval: {
              duration: "10s"
              jitter: "5s"
            }
            min_instances: 1
            max_instances: 3
          }
          configs: [
            {
              weight: 1
              min_lifetime: "2m"
              max_lifetime: "10m"
              instance: {
                name: "stress-instance"
                spec: cnpg.Cluster {
                  instances: 1
                  storage.size: "1Gi"
                }
              }
              test: {
                warehouses: 1
                active_terminals: 5
                duration: "1m"
              }
            }
          ]
        }
      ]
    }
  }
}
```

**K8Stress Configuration Parameters:**

- `duration`: Total stress test duration
- `users`: List of user behavior profiles
- `users[].min_postgres`/`max_postgres`: PostgreSQL instance limits
- `users[].updates_interval`: Frequency of lifecycle operations
- `users[].rampup`: Startup instance creation configuration
- `users[].configs`: PostgreSQL configurations and test profiles

### Script Benchmarks

Script benchmarks allow you to run custom shell scripts or third-party tools (such as sysbench) as part of your benchmarking workflow. This is useful for integrating external tools, custom workloads, or advanced scenarios not covered by built-in benchmark types.

**KCL Example (sysbench-tpcc):**

```kcl
benchmarks = {
  sysbench_tpcc = bench.ScriptBenchmark {
    name: "sysbench-tpcc"
    runner: bench.runnerRef(runners.local)
    config: bench.ScriptConfig {
      scripts_path: "/opt/sysbench-tpcc"
      prepare_cmd: "./tpcc.lua --pgsql-host=$PGHOST --pgsql-port=$PGPORT --pgsql-password=$PGPASSWORD --pgsql-user=$PGUSER --pgsql-db=$PGDATABASE --tables=2 --scale=1 --use-fk=0 --threads=10 --report-interval=1 --db-driver=pgsql prepare"
      run_cmd: "/scripts/tpcc-runner.sh --pgsql-host=$PGHOST --pgsql-port=$PGPORT --pgsql-password=$PGPASSWORD --pgsql-user=$PGUSER --pgsql-db=$PGDATABASE --tables=2 --scale=1 --use-fk=0 --time=10 --threads=10 --report-interval=1"
      cleanup_cmd: "./tpcc.lua --pgsql-host=$PGHOST --pgsql-port=$PGPORT --pgsql-password=$PGPASSWORD --pgsql-user=$PGUSER --pgsql-db=$PGDATABASE --tables=2 --scale=1 --use-fk=0 --threads=10 --report-interval=1 --db-driver=pgsql cleanup"
      parameters: {
        PGHOST: "localhost"
        PGPORT: "5432"
        PGPASSWORD: "postgres"
        PGUSER: "postgres"
        PGDATABASE: "postgres"
      }
      output_format: "csv"
      aggregation_fields: {
        tps: { aggregations: ["avg", "max"] }
        lat_avg: { aggregations: ["avg"] }
      }
    }
  }
}
```

**Script Benchmark Configuration Parameters:**

- `scripts_path`: (optional) Path to the directory containing your scripts. All commands are executed from this directory.
- `prepare_cmd`: Shell command to prepare the environment (e.g., load data). Supports environment variable expansion (e.g., `$PGHOST`).
- `run_cmd`: Shell command to execute the main benchmark. Supports environment variable expansion.
- `cleanup_cmd`: Shell command to clean up after the benchmark. Supports environment variable expansion.
- `parameters`: Map of environment variables to inject into the script environment. Use these to pass connection info, credentials, or custom values.
- `duration`: (optional) Benchmark duration (e.g., "300s"). Exposed as the `BENCH_DURATION` environment variable.
- `output_format`: Output format of the script's stdout. One of `log` (default), `json`, or `csv`.
- `aggregation_fields`: Map of output field names to aggregation configuration. Each value is a list of aggregation types: `avg`, `sum`, `min`, `max`, `count`.
- `csv_headers`: (optional) List of CSV column headers. If not provided, the first line of CSV output is treated as headers.

**Environment Variable Injection:**
All key-value pairs in `parameters` are injected as environment variables for all commands. Use shell expansion (e.g., `$PGHOST`) in your command strings to reference these variables. The `duration` field, if set, is also available as `BENCH_DURATION`.

**Output Format Handling:**

- `log`: Captures raw stdout/stderr. No structured aggregation.
- `json`: Expects one JSON object per line. Fields can be aggregated using `aggregation_fields`.
- `csv`: Expects CSV output. Fields (columns) can be aggregated using `aggregation_fields`. If `csv_headers` is set, the first line is treated as data; otherwise, the first line is treated as headers.

**Aggregation Configuration Example:**

```kcl
aggregation_fields: {
  tps: { aggregations: ["avg", "max"] },
  lat_avg: { aggregations: ["avg"] },
  errors: { aggregations: ["sum"] }
}
```

**Example Usage:**

- The example above runs a sysbench-tpcc workload using the provided runner script for the run phase, and the Percona `tpcc.lua` script for prepare and cleanup.
- PostgreSQL connection parameters are injected via environment variables and referenced in the command strings.
- The output is parsed as CSV, and the `tps` and `lat_avg` fields are aggregated.

See the [epic-3.md](../epic-3.md) and `scripts/tpcc-runner.sh` for more details on sysbench integration.

## Configuration Schema and Validation

### KCL Schema Integration

The KCL configuration schema is defined in `scripts/kcl/k8dbbench/` and provides:

- **Type Safety**: Compile-time validation of configuration values
- **Schema Validation**: Ensures required fields and valid combinations
- **Auto-completion**: IDE support for configuration editing
- **Documentation**: Inline schema documentation

### API Schema Reference

Configuration parameters map directly to the benchdriver HTTP API. For detailed information about:

- **API Endpoints**: See the [Architecture documentation](./architecture.md#benchdriver-http-api)
- **Configuration Structs**: See the `api/benchdriverapi/` package for complete type definitions:
  - `BenchmarkGoTPCCConfig` - TPC-C configuration parameters
  - `BenchmarkGoTPCHConfig` - TPC-H configuration parameters  
  - `BenchmarkCHBenchConfig` - CHBench configuration parameters
  - `BenchmarkK8StressConfig` - K8Stress configuration parameters
- **Data Types**: Common types like `Duration`, `IsolationLevel`, `PartitionType` are defined in `api/benchdriverapi/benchdriverapi.go`

The k8runner automatically handles the translation from KCL/YAML configurations to the appropriate JSON request bodies for the benchdriver HTTP API.

## Common Configuration Patterns

### Single Database Setup

Minimal configuration for testing one PostgreSQL instance:

```kcl
import k8dbbench as bench

systems = {
  db = bench.CNPGSystem {
    name: "test-db"
    namespace: "benchmark"
    spec.storage.size: "5Gi"
  }
}

runners = {
  runner = bench.Runner {
    name: "test-runner"
    namespace: "benchmark"
    systems: bench.systemRefList([systems.db])
  }
}

benchmarks = {
  tpcc = bench.TPCCBenchmark {
    name: "tpcc-test"
    runner: bench.runnerRef(runners.runner)
    config.warehouses: 1
  }
}
```

### Multi-Replica Distributed Load

Configuration for distributed load generation:

```kcl
systems = {
  large_db = bench.StaticCluster { ... }
}

runners = {
  distributed = bench.Runner {
    name: "distributed-runner"
    namespace: "benchmark"
    systems: bench.systemRefList([systems.large_db])
    spec: {
      replicas: 3  # 3 benchdriver pods
      template.spec.resources: {
        requests: { cpu: "10000m" }
      }
    }
  }
}

benchmarks = {
  high_load = bench.TPCCBenchmark {
    name: "high-load-tpcc"
    runner: bench.runnerRef(runners.distributed)
    config: {
      warehouses: 100
      active_terminals: 200
      duration: "30m"
      wait_thinking: False
    }
  }
}
```

### Environment-Specific Configuration

Using KCL for environment-specific values:

```kcl
# Environment-specific settings
env = option("env") or "dev"
namespace = "benchmark-${env}"
storage_size = "5Gi" if env == "dev" else "50Gi"
replicas = 1 if env == "dev" else 3

systems = {
  db = bench.CNPGSystem {
    name: "postgres-${env}"
    namespace: namespace
    spec: {
      instances: replicas
      storage.size: storage_size
    }
  }
}
```
