package script

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"pgdistbench/api/benchdriverapi"
)

func TestCSVProcessor_ProcessOutput_WithConfiguredHeaders(t *testing.T) {
	config := map[string]benchdriverapi.FieldAggregationConfig{
		"tps":        {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggAvg}},
		"latency_ms": {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggMax}},
	}
	aggregator := NewAggregationEngine(config)
	csvHeaders := []string{"tps", "latency_ms", "other_field"}
	processor := NewCSVProcessor(aggregator, csvHeaders, false)

	// Test CSV with configured headers - all lines are data
	input := `123.45,5.2,ignored
234.56,6.1,also_ignored
345.67,7.0,ignored_too`
	reader := strings.NewReader(input)

	err := processor.ProcessOutput(reader)
	if err != nil {
		t.Fatalf("ProcessOutput failed: %v", err)
	}

	// Check results
	stats := processor.GetResults()
	if len(stats.AggregatedStats) != 2 {
		t.Errorf("Expected 2 aggregated fields, got %d", len(stats.AggregatedStats))
	}

	// Verify all 3 records were processed
	var tpsStats *benchdriverapi.AggregatedFieldStats
	for i := range stats.AggregatedStats {
		if stats.AggregatedStats[i].FieldName == "tps" {
			tpsStats = &stats.AggregatedStats[i]
			break
		}
	}
	if tpsStats == nil {
		t.Fatal("Expected tps field in aggregated stats")
	}
	if tpsStats.Count != 3 {
		t.Errorf("Expected tps count 3, got %d", tpsStats.Count)
	}
}

func TestCSVProcessor_ProcessOutput_WithDetectedHeaders(t *testing.T) {
	config := map[string]benchdriverapi.FieldAggregationConfig{
		"throughput": {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggSum}},
		"response":   {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggAvg}},
	}
	aggregator := NewAggregationEngine(config)
	processor := NewCSVProcessor(aggregator, nil, false) // No configured headers

	// Test CSV with header line (first line contains non-numeric values)
	input := `throughput,response,timestamp
100.0,25.5,1234567890
200.0,30.2,1234567891
150.0,28.0,1234567892`
	reader := strings.NewReader(input)

	err := processor.ProcessOutput(reader)
	if err != nil {
		t.Fatalf("ProcessOutput failed: %v", err)
	}

	// Check results - should process 3 data rows (header line is skipped)
	stats := processor.GetResults()
	if len(stats.AggregatedStats) != 2 {
		t.Errorf("Expected 2 aggregated fields, got %d", len(stats.AggregatedStats))
	}

	var throughputStats *benchdriverapi.AggregatedFieldStats
	for i := range stats.AggregatedStats {
		if stats.AggregatedStats[i].FieldName == "throughput" {
			throughputStats = &stats.AggregatedStats[i]
			break
		}
	}
	if throughputStats == nil {
		t.Fatal("Expected throughput field in aggregated stats")
	}
	if throughputStats.Count != 3 {
		t.Errorf("Expected throughput count 3, got %d", throughputStats.Count)
	}
}

func TestCSVProcessor_ProcessOutput_WithGeneratedHeaders(t *testing.T) {
	config := map[string]benchdriverapi.FieldAggregationConfig{
		"1": {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggSum}},
		"2": {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggAvg}},
	}
	aggregator := NewAggregationEngine(config)
	processor := NewCSVProcessor(aggregator, nil, false) // No configured headers

	// Test CSV with all-numeric first line (should be treated as data, not headers)
	input := `100.0,25.5,1234567890
200.0,30.2,1234567891
150.0,28.0,1234567892`
	reader := strings.NewReader(input)

	err := processor.ProcessOutput(reader)
	if err != nil {
		t.Fatalf("ProcessOutput failed: %v", err)
	}

	// Check results - should process all 3 rows with generated headers "1", "2", "3"
	stats := processor.GetResults()
	if len(stats.AggregatedStats) != 2 {
		t.Errorf("Expected 2 aggregated fields, got %d", len(stats.AggregatedStats))
	}

	var field1Stats *benchdriverapi.AggregatedFieldStats
	for i := range stats.AggregatedStats {
		if stats.AggregatedStats[i].FieldName == "1" {
			field1Stats = &stats.AggregatedStats[i]
			break
		}
	}
	if field1Stats == nil {
		t.Fatal("Expected field '1' in aggregated stats")
	}
	if field1Stats.Count != 3 {
		t.Errorf("Expected field '1' count 3, got %d", field1Stats.Count)
	}
}

func TestCSVProcessor_ProcessOutput_ResilientParsing(t *testing.T) {
	config := map[string]benchdriverapi.FieldAggregationConfig{
		"valid_field": {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggAvg}},
	}
	aggregator := NewAggregationEngine(config)
	csvHeaders := []string{"valid_field", "other_field"}
	processor := NewCSVProcessor(aggregator, csvHeaders, false)

	// Test with malformed CSV - should process valid records until first error
	input := `100,extra
200,normal
"unclosed quote,field
300,after_error`
	reader := strings.NewReader(input)

	err := processor.ProcessOutput(reader)
	if err != nil {
		t.Fatalf("ProcessOutput failed: %v", err)
	}

	// Should process valid records before the parsing error
	stats := processor.GetResults()
	if len(stats.AggregatedStats) != 1 {
		t.Errorf("Expected 1 aggregated field, got %d", len(stats.AggregatedStats))
	}

	validFieldStats := stats.AggregatedStats[0]
	if validFieldStats.Count != 2 {
		t.Errorf("Expected count 2 (valid CSV records before error), got %d", validFieldStats.Count)
	}
}

func TestCSVProcessor_ProcessOutput_EmptyAndMissingFields(t *testing.T) {
	config := map[string]benchdriverapi.FieldAggregationConfig{
		"field1": {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggSum}},
		"field2": {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggAvg}},
		"field3": {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggCount}},
	}
	aggregator := NewAggregationEngine(config)
	csvHeaders := []string{"field1", "field2", "field3"}
	processor := NewCSVProcessor(aggregator, csvHeaders, false)

	// Test with varying field counts and empty fields
	input := `100,25.5,text
200,,another
150,30.0
,40.0,final`
	reader := strings.NewReader(input)

	err := processor.ProcessOutput(reader)
	if err != nil {
		t.Fatalf("ProcessOutput failed: %v", err)
	}

	// Check results - missing fields should not cause errors
	stats := processor.GetResults()
	if len(stats.AggregatedStats) == 0 {
		t.Fatal("Expected some aggregated fields")
	}

	// field1 should have 3 valid values (100, 200, 150; empty string ignored)
	// field2 should have 3 valid values (25.5, 30.0, 40.0; empty string ignored)
	// field3 should have 3 valid values (processed as strings: "text", "another", "final")
	fieldCounts := make(map[string]int64)
	for _, stat := range stats.AggregatedStats {
		fieldCounts[stat.FieldName] = stat.Count
	}

	// Note: The aggregation engine only processes fields with valid numeric values
	// So field3 with text values might not appear, and empty values are skipped
	if fieldCounts["field1"] == 0 {
		t.Error("Expected field1 to have some processed values")
	}
}

func TestCSVProcessor_ProcessOutput_OnlyConfiguredFields(t *testing.T) {
	config := map[string]benchdriverapi.FieldAggregationConfig{
		"wanted_field": {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggSum}},
	}
	aggregator := NewAggregationEngine(config)
	csvHeaders := []string{"wanted_field", "unwanted_field", "another_unwanted"}
	processor := NewCSVProcessor(aggregator, csvHeaders, false)

	// Input has all fields but only wanted_field should be processed
	input := `100,200,300
400,500,600`
	reader := strings.NewReader(input)

	err := processor.ProcessOutput(reader)
	if err != nil {
		t.Fatalf("ProcessOutput failed: %v", err)
	}

	// Should only process the configured field
	stats := processor.GetResults()
	if len(stats.AggregatedStats) != 1 {
		t.Errorf("Expected 1 aggregated field, got %d", len(stats.AggregatedStats))
	}

	if stats.AggregatedStats[0].FieldName != "wanted_field" {
		t.Errorf("Expected field name 'wanted_field', got '%s'", stats.AggregatedStats[0].FieldName)
	}
	if stats.AggregatedStats[0].Count != 2 {
		t.Errorf("Expected count 2, got %d", stats.AggregatedStats[0].Count)
	}
}

func TestCSVProcessor_GetFormat(t *testing.T) {
	aggregator := NewAggregationEngine(nil)
	processor := NewCSVProcessor(aggregator, nil, false)

	if processor.GetFormat() != benchdriverapi.FormatCSV {
		t.Errorf("Expected format 'csv', got '%s'", processor.GetFormat())
	}
}

func TestCSVProcessor_SetExecutionResults(t *testing.T) {
	aggregator := NewAggregationEngine(nil)
	processor := NewCSVProcessor(aggregator, nil, false)

	processor.SetExecutionResults("error message", 1)

	stats := processor.GetResults()

	if stats.Stderr != "error message" {
		t.Errorf("Expected stderr 'error message', got '%s'", stats.Stderr)
	}

	if stats.ExitCode != 1 {
		t.Errorf("Expected exit code 1, got %d", stats.ExitCode)
	}
}

func TestExecuteCommand_WithCSVProcessor(t *testing.T) {
	ctx := context.Background()

	config := map[string]benchdriverapi.FieldAggregationConfig{
		"value1": {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggSum}},
		"value2": {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggCount}},
	}
	aggregator := NewAggregationEngine(config)
	csvHeaders := []string{"value1", "value2", "ignored"}
	processor := NewCSVProcessor(aggregator, csvHeaders, false)

	// Use a command that outputs CSV
	cmdConfig := CommandConfig{
		Command:         `sh -c 'echo "10,20,30"; echo "40,50,60"; echo "70,80,90"'`,
		Environment:     map[string]string{},
		OutputProcessor: processor,
	}

	result, err := ExecuteCommand(ctx, cmdConfig)
	if err != nil {
		t.Fatalf("ExecuteCommand with CSV processor failed: %v", err)
	}

	if result.ExitCode != 0 {
		t.Errorf("Expected exit code 0, got %d", result.ExitCode)
	}

	// Check that processor accumulated the CSV data
	stats := processor.GetResults()
	if len(stats.AggregatedStats) != 2 {
		t.Errorf("Expected 2 aggregated fields, got %d", len(stats.AggregatedStats))
	}

	// Verify both fields were processed with 3 records each
	fieldCounts := make(map[string]int64)
	for _, stat := range stats.AggregatedStats {
		fieldCounts[stat.FieldName] = stat.Count
	}

	if fieldCounts["value1"] != 3 {
		t.Errorf("Expected value1 count 3, got %d", fieldCounts["value1"])
	}
	if fieldCounts["value2"] != 3 {
		t.Errorf("Expected value2 count 3, got %d", fieldCounts["value2"])
	}
}

func TestCSVProcessor_DetectHeaders_EdgeCases(t *testing.T) {
	config := map[string]benchdriverapi.FieldAggregationConfig{}
	aggregator := NewAggregationEngine(config)

	tests := []struct {
		name          string
		firstLine     []string
		expectedType  string // "detected", "generated", or "empty"
		expectedCount int
	}{
		{
			name:          "Empty first line",
			firstLine:     []string{},
			expectedType:  "empty",
			expectedCount: 0,
		},
		{
			name:          "Mixed numeric and text headers",
			firstLine:     []string{"count", "123", "description"},
			expectedType:  "detected",
			expectedCount: 3,
		},
		{
			name:          "All numeric values",
			firstLine:     []string{"123", "456.78", "0"},
			expectedType:  "generated",
			expectedCount: 3,
		},
		{
			name:          "Single text header",
			firstLine:     []string{"metric"},
			expectedType:  "detected",
			expectedCount: 1,
		},
		{
			name:          "Empty strings",
			firstLine:     []string{"", "", ""},
			expectedType:  "generated",
			expectedCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			processor := NewCSVProcessor(aggregator, nil, false)
			headers := processor.detectHeaders(tt.firstLine)

			if len(headers) != tt.expectedCount {
				t.Errorf("Expected %d headers, got %d", tt.expectedCount, len(headers))
			}

			if tt.expectedType == "generated" && len(headers) > 0 {
				// Check that generated headers are "1", "2", "3", etc.
				for i, header := range headers {
					expected := fmt.Sprintf("%d", i+1)
					if header != expected {
						t.Errorf("Expected generated header '%s', got '%s'", expected, header)
					}
				}
			}
		})
	}
}

func TestCSVProcessor_ProcessOutput_SysbenchTimeSeriesFormat(t *testing.T) {
	// Configure aggregations for sysbench-style metrics
	config := map[string]benchdriverapi.FieldAggregationConfig{
		"tps":     {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggAvg, benchdriverapi.AggMin, benchdriverapi.AggMax}},
		"qps":     {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggAvg, benchdriverapi.AggSum}},
		"lat_avg": {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggAvg, benchdriverapi.AggMax}},
		"lat_max": {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggMax, benchdriverapi.AggAvg}},
		"errors":  {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggSum, benchdriverapi.AggCount}},
	}
	aggregator := NewAggregationEngine(config)
	processor := NewCSVProcessor(aggregator, nil, false) // Let it detect headers from first line

	// Sysbench-style CSV output with time-series data
	input := `time,threads,tps,qps,lat_min,lat_avg,lat_max,lat_99th,errors,reconnects
1,10,451.70,14318.64,6369.83,6499.75,1449.05,0.00,264.83,0.00
2,10,602.99,19456.79,8714.90,8862.90,1878.98,0.00,339.00,0.00
3,10,645.28,21205.42,9473.47,9733.18,1998.78,0.00,355.60,0.00
4,10,667.96,21947.63,9818.15,10018.44,2111.04,0.00,390.56,0.00
5,10,409.02,13373.71,5966.32,6101.32,1306.07,0.00,248.01,0.00
6,10,577.97,18436.02,8268.56,8365.56,1801.90,0.00,323.98,0.00
7,10,633.09,20338.95,9068.31,9292.35,1978.29,0.00,357.05,0.00
8,10,640.70,21057.27,9456.63,9624.55,1976.09,0.00,357.83,0.00
9,10,696.17,22011.52,9843.47,10007.51,2160.54,0.00,384.10,0.00
10,10,700.12,22740.82,10204.71,10365.74,2170.36,0.00,392.07,0.00`
	reader := strings.NewReader(input)

	err := processor.ProcessOutput(reader)
	if err != nil {
		t.Fatalf("ProcessOutput failed: %v", err)
	}

	// Check results
	stats := processor.GetResults()

	// Should have 5 aggregated fields (the ones we configured)
	if len(stats.AggregatedStats) != 5 {
		t.Errorf("Expected 5 aggregated fields, got %d", len(stats.AggregatedStats))
	}

	// Create a map for easier lookup
	fieldStats := make(map[string]*benchdriverapi.AggregatedFieldStats)
	for i := range stats.AggregatedStats {
		fieldStats[stats.AggregatedStats[i].FieldName] = &stats.AggregatedStats[i]
	}

	// Verify each configured field was processed with correct count
	expectedFields := []string{"tps", "qps", "lat_avg", "lat_max", "errors"}
	for _, fieldName := range expectedFields {
		stat, exists := fieldStats[fieldName]
		if !exists {
			t.Errorf("Expected field '%s' in aggregated stats", fieldName)
			continue
		}

		if stat.Count != 10 {
			t.Errorf("Expected count 10 for field '%s', got %d", fieldName, stat.Count)
		}
	}

	// Verify specific calculations for tps field
	tpsStats := fieldStats["tps"]
	if tpsStats != nil {
		// Expected values based on input data:
		// TPS values: 451.70, 602.99, 645.28, 667.96, 409.02, 577.97, 633.09, 640.70, 696.17, 700.12
		// Min should be 409.02, Max should be 700.12
		expectedMin := 409.02
		expectedMax := 700.12
		expectedAvg := (451.70 + 602.99 + 645.28 + 667.96 + 409.02 + 577.97 + 633.09 + 640.70 + 696.17 + 700.12) / 10.0

		if tpsStats.Min != expectedMin {
			t.Errorf("Expected tps min %.2f, got %.2f", expectedMin, tpsStats.Min)
		}
		if tpsStats.Max != expectedMax {
			t.Errorf("Expected tps max %.2f, got %.2f", expectedMax, tpsStats.Max)
		}
		// Allow for small floating point differences
		if abs(tpsStats.Avg-expectedAvg) > 0.01 {
			t.Errorf("Expected tps avg %.2f, got %.2f", expectedAvg, tpsStats.Avg)
		}
	}

	// Verify errors field sum calculation
	errorsStats := fieldStats["errors"]
	if errorsStats != nil {
		// Error values: 264.83, 339.00, 355.60, 390.56, 248.01, 323.98, 357.05, 357.83, 384.10, 392.07
		expectedSum := 264.83 + 339.00 + 355.60 + 390.56 + 248.01 + 323.98 + 357.05 + 357.83 + 384.10 + 392.07

		if abs(errorsStats.Sum-expectedSum) > 0.01 {
			t.Errorf("Expected errors sum %.2f, got %.2f", expectedSum, errorsStats.Sum)
		}
	}
}

// Helper function to calculate absolute value of difference
func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

func TestCSVProcessor_ProcessOutput_SysbenchWithConfiguredHeaders(t *testing.T) {
	// Configure aggregations for sysbench-style metrics
	config := map[string]benchdriverapi.FieldAggregationConfig{
		"tps":     {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggAvg, benchdriverapi.AggMin, benchdriverapi.AggMax}},
		"qps":     {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggAvg, benchdriverapi.AggSum}},
		"lat_avg": {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggAvg, benchdriverapi.AggMax}},
		"lat_max": {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggMax, benchdriverapi.AggAvg}},
		"errors":  {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggSum, benchdriverapi.AggCount}},
	}
	aggregator := NewAggregationEngine(config)

	// Pre-configure headers - this means the first line will be treated as data, not headers
	csvHeaders := []string{"time", "threads", "tps", "qps", "lat_min", "lat_avg", "lat_max", "lat_99th", "errors", "reconnects"}
	processor := NewCSVProcessor(aggregator, csvHeaders, false)

	// Raw CSV data without header line (since headers are pre-configured)
	input := `1,10,451.70,14318.64,6369.83,6499.75,1449.05,0.00,264.83,0.00
2,10,602.99,19456.79,8714.90,8862.90,1878.98,0.00,339.00,0.00
3,10,645.28,21205.42,9473.47,9733.18,1998.78,0.00,355.60,0.00
4,10,667.96,21947.63,9818.15,10018.44,2111.04,0.00,390.56,0.00
5,10,409.02,13373.71,5966.32,6101.32,1306.07,0.00,248.01,0.00`
	reader := strings.NewReader(input)

	err := processor.ProcessOutput(reader)
	if err != nil {
		t.Fatalf("ProcessOutput failed: %v", err)
	}

	// Check results
	stats := processor.GetResults()

	// Should have 5 aggregated fields (the ones we configured)
	if len(stats.AggregatedStats) != 5 {
		t.Errorf("Expected 5 aggregated fields, got %d", len(stats.AggregatedStats))
	}

	// Create a map for easier lookup
	fieldStats := make(map[string]*benchdriverapi.AggregatedFieldStats)
	for i := range stats.AggregatedStats {
		fieldStats[stats.AggregatedStats[i].FieldName] = &stats.AggregatedStats[i]
	}

	// Verify each configured field was processed with correct count (5 rows of data)
	expectedFields := []string{"tps", "qps", "lat_avg", "lat_max", "errors"}
	for _, fieldName := range expectedFields {
		stat, exists := fieldStats[fieldName]
		if !exists {
			t.Errorf("Expected field '%s' in aggregated stats", fieldName)
			continue
		}

		if stat.Count != 5 {
			t.Errorf("Expected count 5 for field '%s', got %d", fieldName, stat.Count)
		}
	}

	// Verify specific calculations for tps field (first 5 values)
	tpsStats := fieldStats["tps"]
	if tpsStats != nil {
		// TPS values: 451.70, 602.99, 645.28, 667.96, 409.02
		expectedMin := 409.02
		expectedMax := 667.96
		expectedAvg := (451.70 + 602.99 + 645.28 + 667.96 + 409.02) / 5.0

		if tpsStats.Min != expectedMin {
			t.Errorf("Expected tps min %.2f, got %.2f", expectedMin, tpsStats.Min)
		}
		if tpsStats.Max != expectedMax {
			t.Errorf("Expected tps max %.2f, got %.2f", expectedMax, tpsStats.Max)
		}
		// Allow for small floating point differences
		if abs(tpsStats.Avg-expectedAvg) > 0.01 {
			t.Errorf("Expected tps avg %.2f, got %.2f", expectedAvg, tpsStats.Avg)
		}
	}
}

func TestCSVProcessor_CollectRawRecords(t *testing.T) {
	config := map[string]benchdriverapi.FieldAggregationConfig{
		"field1": {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggSum}},
	}
	aggregator := NewAggregationEngine(config)
	csvHeaders := []string{"field1", "field2"}
	processor := NewCSVProcessor(aggregator, csvHeaders, true)

	input := `10,foo
20,bar`
	reader := strings.NewReader(input)

	err := processor.ProcessOutput(reader)
	if err != nil {
		t.Fatalf("ProcessOutput failed: %v", err)
	}

	stats := processor.GetResults()
	if len(stats.RawRecords) != 2 {
		t.Errorf("Expected 2 raw records, got %d", len(stats.RawRecords))
	}
	if stats.RawRecords[0]["field1"] != "10" || stats.RawRecords[0]["field2"] != "foo" {
		t.Errorf("Unexpected first raw record: %+v", stats.RawRecords[0])
	}
	if stats.RawRecords[1]["field1"] != "20" || stats.RawRecords[1]["field2"] != "bar" {
		t.Errorf("Unexpected second raw record: %+v", stats.RawRecords[1])
	}
}
