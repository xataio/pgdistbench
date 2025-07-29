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
    _sysbench_conn_config = "--db-driver=pgsql --pgsql-host=$PGHOST --pgsql-port=$PGPORT --pgsql-password=$PGPASS --pgsql-user=$PGUSER --pgsql-db=$PGDATABASE"

    _tpcc_tables = "2"
    _tpcc_scale = "1"
    _tpcc_threads = "10"
    _tpcc_run_s = "10" # execute test for 10s
    _sysbench_tpcc_config = "--tables=${_tpcc_tables} --scale=${_tpcc_scale} --use-fk=0 --report-interval=1 --histogram=off --percentile=99"

    _oltp_run_s = "10"
    _oltp_threads = "10"
    _oltp_readonly_config = "--tables=2 --table-size=130000 --report-interval=1 --histogram=off --percentile=99"


    sysbench_tpcc = bench.ScriptBenchmark {
        name: "sysbench-tpcc-demo"
        runner: bench.runnerRef(runners.local)
        config: bench.ScriptConfig {
            # Path inside the benchdriver container where sysbench-tpcc was cloned.
            scripts_path: "/opt/sysbench-tpcc"

            # Prepare command: creates tables and loads initial data.
            prepare_cmd: "./tpcc.lua ${_sysbench_conn_config} ${_sysbench_tpcc_config} --threads=${_tpcc_tables} prepare"

            # Run command: executes the benchmark using the custom runner script.
            run_cmd: "./tpcc-runner.sh ${_sysbench_conn_config} ${_sysbench_tpcc_config} --time=${_tpcc_run_s} --threads=${_tpcc_threads}"

            # Cleanup command: drops the tables created during prepare.
            cleanup_cmd: "./tpcc.lua ${_sysbench_conn_config} ${_sysbench_tpcc_config} --threads=1 cleanup"

            # Use the native sysbench output format for direct parsing
            output_format: "csv"

            log_stdout: True

            # Collect raw records
            collect_raw_records: True

            # Define how to aggregate the results from the sysbench output.
            aggregation_fields: {
                tps: { aggregations: ["avg", "max"] }
                qps: { aggregations: ["avg", "max"] }
                lat_99th: { aggregations: ["avg", "max"] }
                errors: { aggregations: ["sum"] }
                reconnects: { aggregations: ["sum"] }
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

    sysbench_oltp_read_only = bench.ScriptBenchmark {
        name: "sysbench-oltp-read-only-demo"
        runner: bench.runnerRef(runners.local)
        config: bench.ScriptConfig {
            prepare_cmd: "sysbench oltp_read_only ${_sysbench_conn_config} ${_oltp_readonly_config}  --threads=2  prepare"

            run_cmd: "sysbench oltp_read_only ${_sysbench_conn_config} ${_oltp_readonly_config} --time=${_oltp_run_s} --threads=${_oltp_threads}  --threads=2 run"

            cleanup_cmd: "sysbench oltp_read_only ${_sysbench_conn_config} ${_oltp_readonly_config} --threads=1  cleanup"

            # Use the native sysbench output format for direct parsing
            output_format: "sysbench"
            log_stdout: True

            # Collect raw records
            collect_raw_records: True

            # Define how to aggregate the results from the sysbench output.
            aggregation_fields: {
                tps: { aggregations: ["avg", "max"] }
                qps: { aggregations: ["avg", "max"] }
                lat_99th: { aggregations: ["avg", "max"] }
                errors: { aggregations: ["sum"] }
                reconnects: { aggregations: ["sum"] }
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
