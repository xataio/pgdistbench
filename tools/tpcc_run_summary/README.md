# TPC-C Run Summary Tool

This tool processes multiple TPC-C benchmark run output files, aggregating and analyzing the results to generate a summary table with various performance metrics. It's designed to help analyze and compare the results of multiple TPC-C benchmark runs, calculating both averages and medians for key performance indicators.

## Usage

```bash
./tpcc_run_summary <file1.json> [file2.json] [file3.json] ...
```

Provide one or more JSON files containing TPC-C benchmark results as positional arguments.

## Output Format

The tool generates a summary table with the following columns:

- `tpmC`: Transactions per minute for the TPC-C benchmark (New-Order transactions)
- `tpmTotal`: Total transactions per minute across all transaction types
- `opsCount`: Total number of operations performed
- `opsSuccessCount`: Number of successful operations
- `opsErrCount`: Number of failed operations
- `opsTimeMS`: Total operation time in milliseconds

## Input File Format

The expected format is created by running the exec results command using a TPC-C based benchmark:

```
k8runner exec results <testname> | tee run-tpcc-1.json
```

The input JSON files should contain a structure with the following fields:
- `Runners`: Array of TPC-C run statistics
- `TPM`: Distribution metrics

Each runner in the `Runners` array should include:
- `Summary`: Contains TPMC and TPMTotal metrics
- `Stats`: Array of operation statistics including operation type, count, and timing information

## Example Output

```
Summary Table:
tpmC            tpmTotal        opsCount        opsSuccessCount      opsErrCount      opsTimeMS
--------------- --------------- --------------- -------------------- --------------- ---------------
1234.56         5678.90         10000           9800                 200             15000.00
2345.67         6789.01         12000           11800                200             16000.00
--------------- --------------- --------------- -------------------- --------------- ---------------
Averages
1790.12         6233.96         11000           10800                200             15500.00
Medians
1790.12         6233.96         11000           10800                200             15500.00
```

## Notes

- The tool calculates both averages and medians to provide a more complete statistical picture
- All timing metrics are reported in milliseconds
