#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd )"

# optional arguments with defaults
OUT="results"
CONTEXT=''
SETUP=${SETUP:-"default"}
TEST_PREFIX=${TEST_PREFIX:-"benchmark"}

USAGE_STRING=$(cat <<EOF
Usage: $0 [options] <config> <run_from> [<run_to> | <scale_factors...>]

Options:
  -o, --output DIR       Output directory (default: results)
  -c, --context CONTEXT  Kubernetes context
  -s, --setup SETUP      Test setup name (default: default)
  -p, --prefix PREFIX    Test prefix (default: benchmark)
  -h, --help             Show this help message

Arguments:
  <config>               Configuration file
  <run_from>             Starting run number
  <run_to>               Optional: Ending run number (default: same as run_from)
<scale_factors...>       Optional: Custom TPCC values (default: 1 10 20 35)

This script automates running TPC-C benchmarks at different scale factors (warehouse counts).
It handles preparing, running, and collecting results for each benchmark run, with options
to customize output directory, Kubernetes context, and test setup configuration.
EOF
)

# Parse command line options
while [[ $# -gt 0 ]]; do
  case "$1" in
    -o|--output)
      OUT="$2"
      shift 2
      ;;
    -c|--context)
      CONTEXT="$2"
      shift 2
      ;;
    -s|--setup)
      SETUP="$2"
      shift 2
      ;;
    -p|--prefix)
      TEST_PREFIX="$2"
      shift 2
      ;;
    -h|--help)
      echo "$USAGE_STRING"
      exit
      ;;
    -*)
      echo "Error: Unknown option: $1"
      echo "$USAGE_STRING"
      exit 1
      ;;
    *)
      # First non-option argument is treated as the start of positional arguments
      break
      ;;
  esac
done

# Check for required positional arguments
if [ $# -lt 2 ]; then
  echo "Error: Missing required arguments\n"
  echo "$USAGE_STRING"
  exit 1
fi

# positional arguments
CONFIG="$1"
RUN="$2"
shift 2

RUN_TO="$RUN
SCALE_FACTORS=(1 10 20 35)
if [ $# -ge 1 ]; then
  RUN_TO="$1"
  shift 1
fi
if [ $# -ge 1 ]; then
  SCALE_FACTORS=("$@")
fi


RESULTS="${OUT)/${TEST_PREFIX}/${SETUP}"
if [ ! -d "$RESULTS" ]; then
  mkdir -p "$RESULTS"
fi

set -e
set -x

for run in $(seq $RUN $RUN_TO); do
  for tpcc in "${SCALE_FACTORS[@]}"; do
    echo "Running ${TEST_PREFIX}-bench-tpcc-${tpcc} for run ${run}"
    "${SCRIPT_DIR}/runit.sh" -m "${CONFIG}" -r "${RESULTS}/run" -c "${CONTEXT}"  "${TEST_PREFIX}-bench-tpcc-${tpcc}" ${run} ${run}
  done
done