# Script Run Summary Tool

This tool processes multiple script benchmark run output files, aggregating and analyzing the results to generate a summary table with various performance metrics. It's designed to help analyze and compare the results of multiple script benchmark runs (such as sysbench), calculating both averages and medians for key performance indicators.

## Usage

```bash
./script_run_summary [options] <file1.json> [file2.json] [file3.json] ...
```

### Options

- `--use-aggregates`: Use aggregated_stats instead of raw_records (default: false)
- `--fields string`: Comma-separated list of fields to include (default: auto-detect)
- `--aggregations string`: Comma-separated list of aggregations (default: "avg,max")
- `--units string`: Field units as field:unit pairs (e.g., "tps:rate/s,lat_avg:ms")

## Input File Format

The expected format is created by running script benchmarks using the pgdistbench framework. The input JSON files should contain a structure with the following fields:

- `runners`: Array of script run statistics
- `aggregated_fields`: Optional aggregated field statistics

Each runner in the `runners` array should include:

- `aggregated_stats`: Pre-computed statistics for configured fields
- `raw_records`: Raw data records from the benchmark execution (preferred for calculation)
- `exit_code`: Process exit code (ignored for statistics)

## Field Auto-Detection

The tool automatically detects interesting numeric fields from the raw records while filtering out:

- Constant fields (e.g., `threads` that don't change across records)
- Sequence fields (e.g., `time` that just indicates timestamp)
- Non-statistical fields (e.g., `exit_code`)

## Default Units

The tool includes default unit mappings for common fields:

- `time`: s (seconds)
- `tps`: rate/s (transactions per second)
- `qps`: rate/s (queries per second)
- `reconnects`: count
- `errors`: count
- `threads`: count

Additional fields starting with `lat_` are assumed to be latency metrics in milliseconds.

## Example Usage

### Basic usage with auto-detection

```bash
./script_run_summary run1.json run2.json run3.json
```

### Specify custom fields and aggregations

```bash
./script_run_summary --fields "tps,lat_avg,errors" --aggregations "avg,max,min" run*.json
```

### Force use of aggregated stats instead of raw records

```bash
./script_run_summary --use-aggregates run*.json
```

### Custom units

```bash
./script_run_summary --units "tps:ops/sec,lat_avg:μs" run*.json
```

## Example Output

```
Summary Table:
tps(avg)_rate/s  tps(max)_rate/s  lat_avg(avg)_ms lat_avg(max)_ms 
--------------- --------------- --------------- ---------------
1975.93         2093.74         29630.05        31233.54        
1903.91         2104.01         28668.94        32107.19        
2030.82         2224.85         30555.46        33781.55        
--------------- --------------- --------------- ---------------
Averages
1970.22         2140.87         29618.15        32374.09        
Medians
1975.93         2104.01         29630.05        32107.19        
```

## Data Processing

The tool prioritizes raw record processing over pre-aggregated statistics:

1. **Raw Records Processing**: When `raw_records` are available, the tool calculates statistics directly from the individual data points, providing more accurate aggregation across multiple runners.

2. **Aggregated Stats Fallback**: When raw records are not available or `--use-aggregates` is specified, the tool uses the pre-computed `aggregated_stats`.

3. **Multi-Runner Aggregation**: When a file contains multiple runners, their results are first aggregated within the file, then across files.

## Notes

- The tool calculates both averages and medians across all runs to provide a comprehensive statistical picture
- Numeric precision is automatically formatted for readability
- Missing data is displayed as "N/A"
- All timing metrics are assumed to be in the units specified (default: milliseconds for latency fields)
- The tool uses the `pgdistbench/pkg/stats` package for statistical calculations
