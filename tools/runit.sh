#!/usr/bin/env bash

USAGE=$(cat <<EOF
Usage: $0 [-m filename] [-r run_prefix] [-c k8s_context] <testname> <run> [<run-to>]
  -m filename: The filename to use for the benchmark
  -r run_prefix: The prefix to use for the run
  -c k8s_context: The k8s context to use
  -h: Show this help message

This script automates running database benchmarks (TPC-C/TPC-H) multiple times.
It handles preparing, running, and collecting results for each benchmark run,
with options to customize output filenames and Kubernetes context.
EOF
)

usage() {
  echo "$USAGE"
}

while getopts ":m:r:c:h" opt; do
  case $opt in
    m)
      FILENAME="$OPTARG"
      ;;
    r)
      RUN_PREFIX="$OPTARG"
      ;;
    c)
      K8SCTX="$OPTARG"
      ;;
    h)
      usage
      exit 0
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
  esac
done

shift $((OPTIND -1))

if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
    usage
    exit 1
fi

TESTNAME=$1
RUN=$2
RUN_TO=${3:-$RUN}

if ! [[ "$RUN" =~ ^[0-9]+$ ]]; then
    echo "Error: Run must be a number."
    exit 1
fi
if ! [[ "$RUN_TO" =~ ^[0-9]+$ ]]; then
    echo "Error: Run-to must be a number."
    exit 1
fi

RUNNER_CMD="k8runner"
if [ -n "$K8SCTX" ]; then
    RUNNER_CMD="$RUNNER_CMD --context $K8SCTX"
fi

exec_cleanup() {
    $RUNNER_CMD exec cleanup --wait -m "$FILENAME" "$TESTNAME"
}

exec_prepare() {
    $RUNNER_CMD exec prepare --wait -m "$FILENAME" "$TESTNAME"
}

exec_start_run() {
    $RUNNER_CMD exec run -m "$FILENAME" "$TESTNAME"
}

exec_wait_done() {
    local run=$1
    $RUNNER_CMD exec results -m "$FILENAME" "$TESTNAME" |& tee "$RUN_PREFIX-$TESTNAME-$run.out"
}

set -x
set -e

exec_bench_step() {
  local run=$1

  echo "Starting initial cleanup step"
  exec_cleanup || { echo "Error during cleanup step"; exit 1; }

  echo "Starting prepare step"
  exec_prepare || { echo "Error during prepare step"; exit 1; }

  echo "Starting start-run step"
  exec_start_run || { echo "Error during start-run step"; exit 1; }

  echo "Starting wait step"
  exec_wait_done $run || { echo "Error during wait step"; exit 1; }
  sleep 30

  echo "Starting final cleanup step"
  exec_cleanup || { echo "Error during cleanup step"; exit 1; }
}

for run in $(seq $RUN $RUN_TO); do
  echo "Running test $TESTNAME run $run"
  exec_bench_step $run
done
