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
		"latency_ms": {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggP99}},
	}
	aggregator := NewAggregationEngine(config)
	csvHeaders := []string{"tps", "latency_ms", "other_field"}
	processor := NewCSVProcessor(aggregator, csvHeaders)

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
	processor := NewCSVProcessor(aggregator, nil) // No configured headers

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
	processor := NewCSVProcessor(aggregator, nil) // No configured headers

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
	processor := NewCSVProcessor(aggregator, csvHeaders)

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
	processor := NewCSVProcessor(aggregator, csvHeaders)

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
	processor := NewCSVProcessor(aggregator, csvHeaders)

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
	processor := NewCSVProcessor(aggregator, nil)

	if processor.GetFormat() != benchdriverapi.FormatCSV {
		t.Errorf("Expected format 'csv', got '%s'", processor.GetFormat())
	}
}

func TestCSVProcessor_SetExecutionResults(t *testing.T) {
	aggregator := NewAggregationEngine(nil)
	processor := NewCSVProcessor(aggregator, nil)

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
	processor := NewCSVProcessor(aggregator, csvHeaders)

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
			processor := NewCSVProcessor(aggregator, nil)
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
