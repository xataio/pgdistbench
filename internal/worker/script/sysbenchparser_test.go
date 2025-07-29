package script

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSysbenchParser_ParseIntermediateReport(t *testing.T) {
	parser := newSysbenchParser()

	tests := map[string]struct {
		input       string
		expectMatch bool
		want        map[string]interface{}
	}{
		"basic_intermediate_report": {
			input:       `[ 5s ] thds: 2 tps: 1439.03 qps: 23029.49 (r/w/o: 20151.43/0.00/2878.06) lat (ms,99%): 2.35 err/s: 0.00 reconn/s: 0.00`,
			expectMatch: true,
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
			input:       `[ 10s ] thds: 4 tps: 1500.00 qps: 24000.00 (r/w/o: 21000.00/0.00/3000.00) lat (ms,99%): 1.50 err/s: 2.50 reconn/s: 1.00`,
			expectMatch: true,
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
			input:       `[ 8s ] thds: 3 tps: 1200.50 qps: 19208.00 (r/w/o: 16806.00/0.00/2402.00) lat (ms,99%): 3.25 err/s: 0.75 reconn/s: 0.25`,
			expectMatch: true,
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
			input:       `[ 1s ] thds: 1 tps: 0.00 qps: 0.00 (r/w/o: 0.00/0.00/0.00) lat (ms,99%): 0.00 err/s: 0.00 reconn/s: 0.00`,
			expectMatch: true,
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
			input:       `[ 15s ] thds: 16 tps: 3200.75 qps: 51212.00 (r/w/o: 44836.00/0.00/6376.00) lat (ms,99%): 0.85 err/s: 5.20 reconn/s: 2.10`,
			expectMatch: true,
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
		"non_matching_line": {
			input:       `This is not a sysbench intermediate report line`,
			expectMatch: false,
		},
		"partial_match": {
			input:       `[ 5s ] thds: 2 tps: 1439.03`,
			expectMatch: false,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			isMatch := parser.isIntermediateReportLine(tt.input)
			require.Equal(t, tt.expectMatch, isMatch, "IsIntermediateReportLine result mismatch")

			if tt.expectMatch {
				record, err := parser.parseIntermediateReport(tt.input)
				require.NoError(t, err, "ParseIntermediateReport should not return error")
				require.NotNil(t, record, "ParseIntermediateReport should return record")

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
			} else {
				record, err := parser.parseIntermediateReport(tt.input)
				require.Error(t, err, "ParseIntermediateReport should return error for non-matching line")
				require.Nil(t, record, "ParseIntermediateReport should return nil record for non-matching line")
			}
		})
	}
}

func TestSysbenchParser_ParseSQLStatsLine(t *testing.T) {
	parser := newSysbenchParser()

	tests := map[string]struct {
		input       string
		expectMatch bool
		want        *ParsedSQLStat
	}{
		"basic_sql_stat": {
			input:       `    read:                            206920`,
			expectMatch: true,
			want: &ParsedSQLStat{
				Name:    "read",
				Value:   206920,
				HasRate: false,
			},
		},
		"sql_stat_with_rate": {
			input:       `    transactions:                        14780  (1477.61 per sec.)`,
			expectMatch: true,
			want: &ParsedSQLStat{
				Name:    "transactions",
				Value:   14780,
				PerSec:  1477.61,
				HasRate: true,
			},
		},
		"ignored_errors": {
			input:       `    ignored errors:                      0      (0.00 per sec.)`,
			expectMatch: true,
			want: &ParsedSQLStat{
				Name:    "ignored_errors",
				Value:   0,
				PerSec:  0.00,
				HasRate: true,
			},
		},
		"multi_word_stat": {
			input:       `    other:                               29560`,
			expectMatch: true,
			want: &ParsedSQLStat{
				Name:    "other",
				Value:   29560,
				HasRate: false,
			},
		},
		"non_matching_line": {
			input:       `This is not a SQL stats line`,
			expectMatch: false,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			stat, err := parser.parseSQLStatsLine(tt.input)

			if tt.expectMatch {
				require.NoError(t, err, "ParseSQLStatsLine should not return error")
				require.NotNil(t, stat, "ParseSQLStatsLine should return stat")
				require.Equal(t, tt.want.Name, stat.Name, "Name mismatch")
				require.Equal(t, tt.want.Value, stat.Value, "Value mismatch")
				require.Equal(t, tt.want.HasRate, stat.HasRate, "HasRate mismatch")
				if tt.want.HasRate {
					require.Equal(t, tt.want.PerSec, stat.PerSec, "PerSec mismatch")
				}
			} else {
				require.Error(t, err, "ParseSQLStatsLine should return error for non-matching line")
				require.Nil(t, stat, "ParseSQLStatsLine should return nil stat for non-matching line")
			}
		})
	}
}

func TestSysbenchParser_ParseGeneralStatsLine(t *testing.T) {
	parser := newSysbenchParser()

	tests := map[string]struct {
		input       string
		expectMatch bool
		want        *ParsedGeneralStat
	}{
		"total_time": {
			input:       `    total time:                          10.0023s`,
			expectMatch: true,
			want: &ParsedGeneralStat{
				Name:  "total_time",
				Value: 10.0023,
			},
		},
		"total_events": {
			input:       `    total number of events:              14780`,
			expectMatch: true,
			want: &ParsedGeneralStat{
				Name:  "total_number_of_events",
				Value: 14780,
			},
		},
		"non_matching_line": {
			input:       `This is not a general stats line`,
			expectMatch: false,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			stat, err := parser.parseGeneralStatsLine(tt.input)

			if tt.expectMatch {
				require.NoError(t, err, "ParseGeneralStatsLine should not return error")
				require.NotNil(t, stat, "ParseGeneralStatsLine should return stat")
				require.Equal(t, tt.want.Name, stat.Name, "Name mismatch")
				require.Equal(t, tt.want.Value, stat.Value, "Value mismatch")
			} else {
				require.Error(t, err, "ParseGeneralStatsLine should return error for non-matching line")
				require.Nil(t, stat, "ParseGeneralStatsLine should return nil stat for non-matching line")
			}
		})
	}
}

func TestSysbenchParser_ParseLatencyStatsLine(t *testing.T) {
	parser := newSysbenchParser()

	tests := map[string]struct {
		input       string
		expectMatch bool
		want        *ParsedLatencyStat
	}{
		"min_latency": {
			input:       `         min:                                    0.38`,
			expectMatch: true,
			want: &ParsedLatencyStat{
				Name:  "min",
				Value: 0.38,
			},
		},
		"avg_latency": {
			input:       `         avg:                                    1.35`,
			expectMatch: true,
			want: &ParsedLatencyStat{
				Name:  "avg",
				Value: 1.35,
			},
		},
		"99th_percentile": {
			input:       `         99th percentile:                        1.93`,
			expectMatch: true,
			want: &ParsedLatencyStat{
				Name:  "99th", // Should convert "99th percentile" to "99th"
				Value: 1.93,
			},
		},
		"sum_latency": {
			input:       `         sum:                                19987.95`,
			expectMatch: true,
			want: &ParsedLatencyStat{
				Name:  "sum",
				Value: 19987.95,
			},
		},
		"non_matching_line": {
			input:       `This is not a latency stats line`,
			expectMatch: false,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			stat, err := parser.parseLatencyStatsLine(tt.input)

			if tt.expectMatch {
				require.NoError(t, err, "ParseLatencyStatsLine should not return error")
				require.NotNil(t, stat, "ParseLatencyStatsLine should return stat")
				require.Equal(t, tt.want.Name, stat.Name, "Name mismatch")
				require.Equal(t, tt.want.Value, stat.Value, "Value mismatch")
			} else {
				require.Error(t, err, "ParseLatencyStatsLine should return error for non-matching line")
				require.Nil(t, stat, "ParseLatencyStatsLine should return nil stat for non-matching line")
			}
		})
	}
}

func TestSysbenchParser_StateManagement(t *testing.T) {
	parser := newSysbenchParser()

	// Test initial state
	require.Equal(t, parseStateMetrics, parser.state)

	// Test state transitions
	testCases := []struct {
		line          string
		expectedState parseState
	}{
		{"SQL statistics:", parseStateSQLStats},
		{"General statistics:", parseStateGeneralStats},
		{"Latency (ms):", parseStateLatencyStats},
		{"Threads fairness:", parseStateMetrics},
	}

	for _, tc := range testCases {
		parser.updateParseState(tc.line)
		require.Equal(t, tc.expectedState, parser.state, "State transition failed for line: %s", tc.line)
	}
}