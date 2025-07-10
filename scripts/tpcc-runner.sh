#!/bin/bash

BENCH_SCRIPT_HOME=${BENCH_SCRIPT_HOME:-"/opt/sysbench-tpcc"}

# This script prints a CSV header, runs the sysbench-tpcc tool,
# and filters out the "DB SCHEMA public" line from the output.
# It passes all its arguments directly to the sysbench command.

# Print the CSV header
echo "time,threads,tps,qps,lat_min,lat_avg,lat_max,lat_99th,errors,reconnects"

# Change to the script directory
cd "$BENCH_SCRIPT_HOME"

# Execute the tpcc.lua script with all arguments, and filter the output
./tpcc.lua --histogram=off --percentile=99 --db-driver=pgsql --report_csv=yes --verbosity=0 "$@" run | grep -v "DB SCHEMA public" 