import k8dbbench as bench

# 1. Define the System Under Test (SUT)
# This example uses a static system, assuming a PostgreSQL instance is
# already running and accessible. The connection parameters are passed
# to the script via environment variables.
systems = {
    local = bench.CNPGSystem {
        name: "local"
        namespace: "benchmark"
        spec.storage.size: "5Gi"
    }
}

# 2. Define the Runner
# The runner specifies how the benchmark client (benchdriver) is deployed.
# It references the system it will run tests against.
runners = {
    local = bench.Runner {
        name: "local"
        namespace: "benchmark"
        systems: bench.systemRefList([systems.local])
        imagePullPolicy: "IfNotPresent"
        spec: {
            template.spec: {
                resources.requests.cpu: "100m"
            }
        }
    }
}

# 3. Define the Benchmark
# This ScriptBenchmark runs a sysbench-tpcc test. It uses the custom
# `tpcc-runner.sh` for the run command and the standard `tpcc.lua` for
# prepare and cleanup.
benchmarks = {
    sysbench_tpcc = bench.ScriptBenchmark {
        name: "sysbench-tpcc-demo"
        runner: bench.runnerRef(runners.local)
        config: bench.ScriptConfig {
            # Path inside the benchdriver container where sysbench-tpcc was cloned.
            scripts_path: "/opt/sysbench-tpcc"

            # Prepare command: creates tables and loads initial data.
            prepare_cmd: "./tpcc.lua --pgsql-host=$PGHOST --pgsql-port=$PGPORT --pgsql-password=$PGPASS --pgsql-user=$PGUSER --pgsql-db=$PGDATABASE --tables=2 --scale=1 --use-fk=0 --threads=10 --report-interval=1 --db-driver=pgsql prepare"
            
            # Run command: executes the benchmark using the custom runner script.
            run_cmd: "./tpcc-runner.sh --pgsql-host=$PGHOST --pgsql-port=$PGPORT --pgsql-password=$PGPASS --pgsql-user=$PGUSER --pgsql-db=$PGDATABASE --tables=2 --scale=1 --use-fk=0 --time=10 --threads=10 --report-interval=1"
            
            # Cleanup command: drops the tables created during prepare.
            cleanup_cmd: "./tpcc.lua --pgsql-host=$PGHOST --pgsql-port=$PGPORT --pgsql-password=$PGPASS --pgsql-user=$PGUSER --pgsql-db=$PGDATABASE --tables=2 --scale=1 --use-fk=0 --threads=10 --report-interval=1 --db-driver=pgsql cleanup"
            
            # The runner script outputs CSV. We tell the system to expect it.
            output_format: "csv"
            
            # Collect raw records
            collect_raw_records: True

            # Define how to aggregate the results from the CSV output.
            aggregation_fields: {
                tps: { aggregations: ["avg", "max"] }
                lat_avg: { aggregations: ["avg"] }
            }

            # Error detection: look for 'FATAL' in stdout or stderr
            # The sysbench scripts tend to exit with code 0 after an error
            error_detection: bench.ErrorDetectionConfig {
                sources: ["stdout", "stderr"]
                patterns: ["FATAL"]
                override_exit_code: True
            }
        }
    }
} 