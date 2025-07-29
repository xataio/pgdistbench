package script

import (
	"strings"
	"testing"

	"pgdistbench/api/benchdriverapi"

	"github.com/stretchr/testify/require"
)

const sysbenchSampleOutput = `
sysbench 1.0.20 (using system LuaJIT 2.1.0-beta3)

Running the test with following options:
Number of threads: 2
Report intermediate results every 1 second(s)
Initializing random number generator from current time


Initializing worker threads...

Threads started!

[ 1s ] thds: 2 tps: 1441.25 qps: 23075.05 (r/w/o: 20190.55/0.00/2884.51) lat (ms,99%): 2.00 err/s: 0.00 reconn/s: 0.00
[ 2s ] thds: 2 tps: 1465.02 qps: 23445.39 (r/w/o: 20515.34/0.00/2930.05) lat (ms,99%): 1.79 err/s: 0.00 reconn/s: 0.00
[ 3s ] thds: 2 tps: 1466.02 qps: 23458.25 (r/w/o: 20526.22/0.00/2932.03) lat (ms,99%): 1.76 err/s: 0.00 reconn/s: 0.00
[ 4s ] thds: 2 tps: 1482.00 qps: 23707.96 (r/w/o: 20743.97/0.00/2964.00) lat (ms,99%): 1.76 err/s: 0.00 reconn/s: 0.00
[ 5s ] thds: 2 tps: 1439.03 qps: 23029.49 (r/w/o: 20151.43/0.00/2878.06) lat (ms,99%): 2.35 err/s: 0.00 reconn/s: 0.00
[ 6s ] thds: 2 tps: 1465.95 qps: 23435.15 (r/w/o: 20504.26/0.00/2930.89) lat (ms,99%): 2.22 err/s: 0.00 reconn/s: 0.00
[ 7s ] thds: 2 tps: 1501.04 qps: 24036.67 (r/w/o: 21033.59/0.00/3003.08) lat (ms,99%): 1.93 err/s: 0.00 reconn/s: 0.00
[ 8s ] thds: 2 tps: 1496.00 qps: 23918.01 (r/w/o: 20926.01/0.00/2992.00) lat (ms,99%): 1.73 err/s: 0.00 reconn/s: 0.00
[ 9s ] thds: 2 tps: 1515.96 qps: 24272.39 (r/w/o: 21240.47/0.00/3031.92) lat (ms,99%): 1.64 err/s: 0.00 reconn/s: 0.00
[ 10s ] thds: 2 tps: 1505.00 qps: 24077.08 (r/w/o: 21067.07/0.00/3010.01) lat (ms,99%): 1.70 err/s: 0.00 reconn/s: 0.00
SQL statistics:
    queries performed:
        read:                            206920
        write:                           0
        other:                           29560
        total:                           236480
    transactions:                        14780  (1477.61 per sec.)
    queries:                             236480 (23641.76 per sec.)
    ignored errors:                      0      (0.00 per sec.)
    reconnects:                          0      (0.00 per sec.)

General statistics:
    total time:                          10.0023s
    total number of events:              14780

Latency (ms):
         min:                                    0.38
         avg:                                    1.35
         max:                                    4.11
         99th percentile:                        1.93
         sum:                                19987.95

Threads fairness:
    events (avg/stddev):           7390.0000/2.00
    execution time (avg/stddev):   9.9940/0.00
`

func TestSysbenchProcessor_Output(t *testing.T) {
	tests := map[string]struct {
		input           string
		wantRecordCount int
		wantLastRecord  map[string]interface{}
	}{
		"full_sysbench_output": {
			input:           sysbenchSampleOutput,
			wantRecordCount: 10,
			wantLastRecord: map[string]interface{}{
				"time":     int64(10),
				"threads":  int64(2),
				"tps":      1505.00,
				"qps":      24077.08,
				"qps_read": 21067.07,
			},
		},
		"single_record_output": {
			input:           `[ 5s ] thds: 4 tps: 1500.00 qps: 24000.00 (r/w/o: 21000.00/0.00/3000.00) lat (ms,99%): 1.50 err/s: 0.00 reconn/s: 0.00`,
			wantRecordCount: 1,
			wantLastRecord: map[string]interface{}{
				"time":     int64(5),
				"threads":  int64(4),
				"tps":      1500.00,
				"qps":      24000.00,
				"qps_read": 21000.00,
			},
		},
		"multiple_records": {
			input: `[ 1s ] thds: 2 tps: 1441.25 qps: 23075.05 (r/w/o: 20190.55/0.00/2884.51) lat (ms,99%): 2.00 err/s: 0.00 reconn/s: 0.00
[ 2s ] thds: 2 tps: 1465.02 qps: 23445.39 (r/w/o: 20515.34/0.00/2930.05) lat (ms,99%): 1.79 err/s: 0.00 reconn/s: 0.00`,
			wantRecordCount: 2,
			wantLastRecord: map[string]interface{}{
				"time":     int64(2),
				"threads":  int64(2),
				"tps":      1465.02,
				"qps":      23445.39,
				"qps_read": 20515.34,
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			reader := strings.NewReader(tt.input)
			processor := NewSysbenchProcessor()
			processor.SetCollectRaw(true) // Enable raw data collection for testing

			processor.ProcessOutput(reader)
			results := processor.GetResults()

			require.Equal(t, tt.wantRecordCount, len(results.RawRecords))

			if tt.wantRecordCount > 0 {
				record := results.RawRecords[tt.wantRecordCount-1]
				for field, want := range tt.wantLastRecord {
					require.Equal(t, want, record[field], "Field %s mismatch", field)
				}
			}
		})
	}
}

func TestSysbenchProcessor_CSVCompatibility(t *testing.T) {
	// Test that parsed records contain all expected CSV fields
	wantFields := []string{"time", "threads", "tps", "qps", "qps_read", "qps_write", "qps_other", "lat_99th", "errors", "reconnects"}

	tests := map[string]struct {
		input string
		want  map[string]interface{}
	}{
		"basic_parsing_no_errors": {
			input: `[ 5s ] thds: 2 tps: 1439.03 qps: 23029.49 (r/w/o: 20151.43/0.00/2878.06) lat (ms,99%): 2.35 err/s: 0.00 reconn/s: 0.00`,
			want: map[string]interface{}{
				"time":       int64(5),
				"threads":    int64(2),
				"tps":        1439.03,
				"qps":        23029.49,
				"qps_read":   20151.43,
				"qps_write":  0.00,
				"qps_other":  2878.06,
				"lat_99th":   2.35,
				"errors":     int64(0), // 0.00 err/s * 5s = 0
				"reconnects": int64(0), // 0.00 reconn/s * 5s = 0
			},
		},
		"with_errors_and_reconnects": {
			input: `[ 10s ] thds: 4 tps: 1500.00 qps: 24000.00 (r/w/o: 21000.00/0.00/3000.00) lat (ms,99%): 1.50 err/s: 2.50 reconn/s: 1.00`,
			want: map[string]interface{}{
				"time":       int64(10),
				"threads":    int64(4),
				"tps":        1500.00,
				"qps":        24000.00,
				"qps_read":   21000.00,
				"qps_write":  0.00,
				"qps_other":  3000.00,
				"lat_99th":   1.50,
				"errors":     int64(25), // 2.5 err/s * 10s = 25
				"reconnects": int64(10), // 1.0 reconn/s * 10s = 10
			},
		},
		"fractional_errors": {
			input: `[ 8s ] thds: 3 tps: 1200.50 qps: 19208.00 (r/w/o: 16806.00/0.00/2402.00) lat (ms,99%): 3.25 err/s: 0.75 reconn/s: 0.25`,
			want: map[string]interface{}{
				"time":       int64(8),
				"threads":    int64(3),
				"tps":        1200.50,
				"qps":        19208.00,
				"qps_read":   16806.00,
				"qps_write":  0.00,
				"qps_other":  2402.00,
				"lat_99th":   3.25,
				"errors":     int64(6), // 0.75 err/s * 8s = 6
				"reconnects": int64(2), // 0.25 reconn/s * 8s = 2
			},
		},
		"zero_values": {
			input: `[ 1s ] thds: 1 tps: 0.00 qps: 0.00 (r/w/o: 0.00/0.00/0.00) lat (ms,99%): 0.00 err/s: 0.00 reconn/s: 0.00`,
			want: map[string]interface{}{
				"time":       int64(1),
				"threads":    int64(1),
				"tps":        0.00,
				"qps":        0.00,
				"qps_read":   0.00,
				"qps_write":  0.00,
				"qps_other":  0.00,
				"lat_99th":   0.00,
				"errors":     int64(0),
				"reconnects": int64(0),
			},
		},
		"high_thread_count": {
			input: `[ 15s ] thds: 16 tps: 3200.75 qps: 51212.00 (r/w/o: 44836.00/0.00/6376.00) lat (ms,99%): 0.85 err/s: 5.20 reconn/s: 2.10`,
			want: map[string]interface{}{
				"time":       int64(15),
				"threads":    int64(16),
				"tps":        3200.75,
				"qps":        51212.00,
				"qps_read":   44836.00,
				"qps_write":  0.00,
				"qps_other":  6376.00,
				"lat_99th":   0.85,
				"errors":     int64(78), // 5.20 err/s * 15s = 78
				"reconnects": int64(31), // 2.10 reconn/s * 15s = 31.5 -> 31
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			reader := strings.NewReader(tt.input)
			processor := NewSysbenchProcessor()
			processor.SetCollectRaw(true)

			processor.ProcessOutput(reader)
			results := processor.GetResults()

			require.Equal(t, 1, len(results.RawRecords), "Expected 1 record")
			record := results.RawRecords[0]

			// Verify all expected fields are present
			for _, field := range wantFields {
				require.Contains(t, record, field, "Missing expected field: %s", field)
			}

			// Verify specific field values
			for field, expectedValue := range tt.want {
				require.Equal(t, expectedValue, record[field], "Field %s mismatch", field)
			}

			// Verify data types for aggregation compatibility
			require.IsType(t, int64(0), record["time"], "Unexpected time type")
			require.IsType(t, int64(0), record["threads"], "Unexpected threads type")
			require.IsType(t, float64(0), record["tps"], "Unexpected tps type")
			require.IsType(t, float64(0), record["qps"], "Unexpected qps type")
			require.IsType(t, float64(0), record["qps_read"], "Unexpected qps_read type")
			require.IsType(t, float64(0), record["qps_write"], "Unexpected qps_write type")
			require.IsType(t, float64(0), record["qps_other"], "Unexpected qps_other type")
			require.IsType(t, float64(0), record["lat_99th"], "Unexpected lat_99th type")
			require.IsType(t, int64(0), record["errors"], "Unexpected errors type")
			require.IsType(t, int64(0), record["reconnects"], "Unexpected reconnects type")
		})
	}
}

func TestSysbenchProcessor_AggregationCompatibility(t *testing.T) {
	// Test that records are compatible with the aggregation framework
	// Create aggregation configuration like script_run_summary would use
	aggregationConfig := map[string]benchdriverapi.FieldAggregationConfig{
		"tps":        {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggAvg, benchdriverapi.AggMax}},
		"qps":        {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggAvg, benchdriverapi.AggSum}},
		"lat_99th":   {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggAvg, benchdriverapi.AggMax}},
		"errors":     {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggSum, benchdriverapi.AggCount}},
		"reconnects": {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggSum, benchdriverapi.AggCount}},
	}

	aggregator := NewAggregationEngine(aggregationConfig)
	processor := NewSysbenchProcessorWithConfig(aggregator, true)

	// Input with mixed data including errors and varying performance metrics
	in := `[ 1s ] thds: 2 tps: 1441.25 qps: 23075.05 (r/w/o: 20190.55/0.00/2884.51) lat (ms,99%): 2.00 err/s: 0.00 reconn/s: 0.00
[ 2s ] thds: 2 tps: 1465.02 qps: 23445.39 (r/w/o: 20515.34/0.00/2930.05) lat (ms,99%): 1.79 err/s: 1.00 reconn/s: 0.00
[ 3s ] thds: 2 tps: 1466.02 qps: 23458.25 (r/w/o: 20526.22/0.00/2932.03) lat (ms,99%): 1.76 err/s: 2.00 reconn/s: 1.00`

	reader := strings.NewReader(in)
	processor.ProcessOutput(reader)
	results := processor.GetResults()

	// Verify raw records were collected correctly
	require.Equal(t, 3, len(results.RawRecords), "Expected 3 raw records")

	// Verify aggregated stats were calculated
	require.Equal(t, 5, len(results.AggregatedStats), "Expected 5 aggregated fields")

	// Create a map for easier lookup
	fieldStats := make(map[string]*benchdriverapi.AggregatedFieldStats)
	for i := range results.AggregatedStats {
		fieldStats[results.AggregatedStats[i].FieldName] = &results.AggregatedStats[i]
	}

	// Verify TPS aggregation (should have correct avg and max)
	tpsStats := fieldStats["tps"]
	require.NotNil(t, tpsStats, "Expected tps field in aggregated stats")
	require.Equal(t, int64(3), tpsStats.Count, "Expected tps count 3")
	expectedAvg := (1441.25 + 1465.02 + 1466.02) / 3.0
	require.InDelta(t, expectedAvg, tpsStats.Avg, 0.01, "Expected tps avg %.2f", expectedAvg)
	require.Equal(t, 1466.02, tpsStats.Max, "Expected tps max 1466.02")

	// Verify errors aggregation (cumulative counts: 0, 2, 6)
	errorsStats := fieldStats["errors"]
	require.NotNil(t, errorsStats, "Expected errors field in aggregated stats")
	require.Equal(t, int64(3), errorsStats.Count, "Expected errors count 3")
	expectedSum := float64(0 + 2 + 6) // 0*1 + 1*2 + 2*3
	require.Equal(t, expectedSum, errorsStats.Sum, "Expected errors sum %.0f", expectedSum)

	// Verify reconnects aggregation (cumulative counts: 0, 0, 3)
	reconnectsStats := fieldStats["reconnects"]
	require.NotNil(t, reconnectsStats, "Expected reconnects field in aggregated stats")
	require.Equal(t, int64(3), reconnectsStats.Count, "Expected reconnects count 3")
	expectedReconnectsSum := float64(0 + 0 + 3) // 0*1 + 0*2 + 1*3
	require.Equal(t, expectedReconnectsSum, reconnectsStats.Sum, "Expected reconnects sum %.0f", expectedReconnectsSum)

	// Verify latency aggregation
	latStats := fieldStats["lat_99th"]
	require.NotNil(t, latStats, "Expected lat_99th field in aggregated stats")
	require.Equal(t, int64(3), latStats.Count, "Expected lat_99th count 3")
	require.Equal(t, 2.00, latStats.Max, "Expected lat_99th max 2.00")
}

func TestSysbenchProcessor_WithConfigConstructor(t *testing.T) {
	// Test the NewSysbenchProcessorWithConfig constructor specifically
	aggregationConfig := map[string]benchdriverapi.FieldAggregationConfig{
		"tps": {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggAvg, benchdriverapi.AggMax}},
		"qps": {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggSum}},
	}

	aggregator := NewAggregationEngine(aggregationConfig)
	processor := NewSysbenchProcessorWithConfig(aggregator, true)

	// Verify the processor was created with the right configuration
	require.Equal(t, aggregator, processor.engine, "Expected processor to use the provided aggregator")
	require.True(t, processor.collectRaw, "Expected collectRaw to be true")

	// Process some test data
	in := `[ 1s ] thds: 2 tps: 1500.00 qps: 24000.00 (r/w/o: 21000.00/0.00/3000.00) lat (ms,99%): 1.50 err/s: 0.00 reconn/s: 0.00
[ 2s ] thds: 2 tps: 1600.00 qps: 25600.00 (r/w/o: 22400.00/0.00/3200.00) lat (ms,99%): 1.40 err/s: 0.00 reconn/s: 0.00`

	reader := strings.NewReader(in)
	processor.ProcessOutput(reader)
	results := processor.GetResults()

	// Verify aggregation worked with the custom configuration
	require.Equal(t, 2, len(results.RawRecords), "Expected 2 raw records")

	// Should have aggregated stats for tps and qps based on configuration
	wantFields := map[string]bool{"tps": false, "qps": false}
	for _, stat := range results.AggregatedStats {
		if _, exists := wantFields[stat.FieldName]; exists {
			wantFields[stat.FieldName] = true
		}
	}

	for field, found := range wantFields {
		require.True(t, found, "Expected aggregated field %s not found", field)
	}
}
