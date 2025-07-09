package script

import (
	"context"
	"strings"
	"testing"

	"pgdistbench/api/benchdriverapi"
)

func TestJSONProcessor_ProcessOutput_SingleRecord(t *testing.T) {
	config := map[string]benchdriverapi.FieldAggregationConfig{
		"tps":        {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggAvg}},
		"latency_ms": {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggP99}},
	}
	aggregator := NewAggregationEngine(config)
	processor := NewJSONProcessor(aggregator)

	// Test reading a single JSON object
	input := `{"tps": 123.45, "latency_ms": 5.2, "non_numeric": "ignored"}`
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

	// Find tps field
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
	if tpsStats.Count != 1 {
		t.Errorf("Expected tps count 1, got %d", tpsStats.Count)
	}
}

func TestJSONProcessor_ProcessOutput_MultipleRecords(t *testing.T) {
	config := map[string]benchdriverapi.FieldAggregationConfig{
		"value": {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggSum}},
	}
	aggregator := NewAggregationEngine(config)
	processor := NewJSONProcessor(aggregator)

	// Test reading multiple JSON objects
	input := `{"value": 10.0}
{"value": 20.0}
{"value": 30.0}`
	reader := strings.NewReader(input)

	err := processor.ProcessOutput(reader)
	if err != nil {
		t.Fatalf("ProcessOutput failed: %v", err)
	}

	// Check results
	stats := processor.GetResults()
	if len(stats.AggregatedStats) != 1 {
		t.Errorf("Expected 1 aggregated field, got %d", len(stats.AggregatedStats))
	}

	valueStats := stats.AggregatedStats[0]
	if valueStats.FieldName != "value" {
		t.Errorf("Expected field name 'value', got '%s'", valueStats.FieldName)
	}
	if valueStats.Count != 3 {
		t.Errorf("Expected count 3, got %d", valueStats.Count)
	}
}

func TestJSONProcessor_ProcessOutput_ResilientParsing(t *testing.T) {
	config := map[string]benchdriverapi.FieldAggregationConfig{
		"valid_field": {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggAvg}},
	}
	aggregator := NewAggregationEngine(config)
	processor := NewJSONProcessor(aggregator)

	// Test with malformed JSON - should process valid records until first error
	input := `{"valid_field": 100}
invalid json line
{"valid_field": 200}
{incomplete
{"valid_field": 300}`
	reader := strings.NewReader(input)

	err := processor.ProcessOutput(reader)
	if err != nil {
		t.Fatalf("ProcessOutput failed: %v", err)
	}

	// Should process the first valid record, then stop on error and drain remaining stream
	stats := processor.GetResults()
	if len(stats.AggregatedStats) != 1 {
		t.Errorf("Expected 1 aggregated field, got %d", len(stats.AggregatedStats))
	}

	valueStats := stats.AggregatedStats[0]
	if valueStats.Count != 1 {
		t.Errorf("Expected count 1 (first valid JSON object before error), got %d", valueStats.Count)
	}
}

func TestJSONProcessor_ProcessOutput_DifferentNumericTypes(t *testing.T) {
	config := map[string]benchdriverapi.FieldAggregationConfig{
		"int_field":    {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggSum}},
		"float_field":  {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggAvg}},
		"string_field": {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggCount}},
	}
	aggregator := NewAggregationEngine(config)
	processor := NewJSONProcessor(aggregator)

	// Test different numeric types and string conversion
	input := `{"int_field": 42, "float_field": 3.14, "string_field": "123.45", "non_numeric_string": "abc"}`
	reader := strings.NewReader(input)

	err := processor.ProcessOutput(reader)
	if err != nil {
		t.Fatalf("ProcessOutput failed: %v", err)
	}

	// Check results
	stats := processor.GetResults()
	if len(stats.AggregatedStats) != 3 {
		t.Errorf("Expected 3 aggregated fields, got %d", len(stats.AggregatedStats))
	}

	// All configured fields should have been processed
	fieldCounts := make(map[string]int64)
	for _, stat := range stats.AggregatedStats {
		fieldCounts[stat.FieldName] = stat.Count
	}

	if fieldCounts["int_field"] != 1 {
		t.Errorf("Expected int_field count 1, got %d", fieldCounts["int_field"])
	}
	if fieldCounts["float_field"] != 1 {
		t.Errorf("Expected float_field count 1, got %d", fieldCounts["float_field"])
	}
	if fieldCounts["string_field"] != 1 {
		t.Errorf("Expected string_field count 1, got %d", fieldCounts["string_field"])
	}
}

func TestJSONProcessor_ProcessOutput_OnlyConfiguredFields(t *testing.T) {
	config := map[string]benchdriverapi.FieldAggregationConfig{
		"configured_field": {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggSum}},
	}
	aggregator := NewAggregationEngine(config)
	processor := NewJSONProcessor(aggregator)

	// Input has both configured and unconfigured fields
	input := `{"configured_field": 100, "unconfigured_field": 200, "another_field": 300}`
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

	if stats.AggregatedStats[0].FieldName != "configured_field" {
		t.Errorf("Expected field name 'configured_field', got '%s'", stats.AggregatedStats[0].FieldName)
	}
}

func TestJSONProcessor_GetFormat(t *testing.T) {
	aggregator := NewAggregationEngine(nil)
	processor := NewJSONProcessor(aggregator)

	if processor.GetFormat() != benchdriverapi.FormatJSON {
		t.Errorf("Expected format 'json', got '%s'", processor.GetFormat())
	}
}

func TestJSONProcessor_SetExecutionResults(t *testing.T) {
	aggregator := NewAggregationEngine(nil)
	processor := NewJSONProcessor(aggregator)

	processor.SetExecutionResults("error message", 1)

	stats := processor.GetResults()

	if stats.Stderr != "error message" {
		t.Errorf("Expected stderr 'error message', got '%s'", stats.Stderr)
	}

	if stats.ExitCode != 1 {
		t.Errorf("Expected exit code 1, got %d", stats.ExitCode)
	}
}

func TestExecuteCommand_WithJSONProcessor(t *testing.T) {
	ctx := context.Background()

	config := map[string]benchdriverapi.FieldAggregationConfig{
		"counter": {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggSum}},
	}
	aggregator := NewAggregationEngine(config)
	processor := NewJSONProcessor(aggregator)

	// Use a command that outputs JSON
	cmdConfig := CommandConfig{
		Command:         `sh -c 'echo "{\"counter\": 1}"; echo "{\"counter\": 2}"; echo "{\"counter\": 3}"'`,
		Environment:     map[string]string{},
		OutputProcessor: processor,
	}

	result, err := ExecuteCommand(ctx, cmdConfig)
	if err != nil {
		t.Fatalf("ExecuteCommand with JSON processor failed: %v", err)
	}

	if result.ExitCode != 0 {
		t.Errorf("Expected exit code 0, got %d", result.ExitCode)
	}

	// Check that processor accumulated the JSON data
	stats := processor.GetResults()
	if len(stats.AggregatedStats) != 1 {
		t.Errorf("Expected 1 aggregated field, got %d", len(stats.AggregatedStats))
	}

	counterStats := stats.AggregatedStats[0]
	if counterStats.FieldName != "counter" {
		t.Errorf("Expected field name 'counter', got '%s'", counterStats.FieldName)
	}
	if counterStats.Count != 3 {
		t.Errorf("Expected count 3, got %d", counterStats.Count)
	}
}

func TestAggregationEngine_ExtractNumericValue(t *testing.T) {
	aggregator := NewAggregationEngine(nil)

	tests := []struct {
		input    interface{}
		expected float64
		valid    bool
	}{
		{42, 42.0, true},
		{int32(42), 42.0, true},
		{int64(42), 42.0, true},
		{uint(42), 42.0, true},
		{uint32(42), 42.0, true},
		{uint64(42), 42.0, true},
		{float32(3.14), float64(float32(3.14)), true}, // Account for float32->float64 precision
		{float64(3.14), 3.14, true},
		{"123.45", 123.45, true},
		{"42", 42.0, true},
		{"not_a_number", 0, false},
		{true, 0, false},
		{[]int{1, 2, 3}, 0, false},
		{map[string]int{"key": 1}, 0, false},
	}

	for i, test := range tests {
		result, valid := aggregator.ExtractNumericValue(test.input)
		if valid != test.valid {
			t.Errorf("Test %d: expected valid=%v, got %v", i, test.valid, valid)
		}
		if valid && result != test.expected {
			t.Errorf("Test %d: expected value=%v, got %v", i, test.expected, result)
		}
	}
}

func TestAggregationEngine_ProcessRecord_FlexibleExtraction(t *testing.T) {
	// Test the new record-based approach for more flexibility
	config := map[string]benchdriverapi.FieldAggregationConfig{
		"metrics.tps":   {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggSum}},
		"response_time": {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggAvg}},
		"nested.count":  {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggCount}},
	}
	aggregator := NewAggregationEngine(config)

	// Test processing a record directly - demonstrates the new API's flexibility
	record := map[string]any{
		"metrics.tps":   150.5, // Field name with dot notation (could be jsonpath in future)
		"response_time": 25.0,  // Regular field
		"nested.count":  "42",  // String number that should be converted
		"ignored_field": 999,   // Should be ignored since not in config
		"non_numeric":   "abc", // Should be ignored since not numeric
	}

	// Process the record
	aggregator.ProcessRecord(record)

	// Verify results
	results := aggregator.GetResults()

	// Should have exactly 3 results (only configured fields that had valid numeric values)
	if len(results) != 3 {
		t.Errorf("Expected 3 aggregated fields, got %d", len(results))
	}

	// Check each field was processed
	fieldCounts := make(map[string]int64)
	for _, stat := range results {
		fieldCounts[stat.FieldName] = stat.Count
	}

	expectedFields := []string{"metrics.tps", "response_time", "nested.count"}
	for _, fieldName := range expectedFields {
		if fieldCounts[fieldName] != 1 {
			t.Errorf("Expected field %s to have count 1, got %d", fieldName, fieldCounts[fieldName])
		}
	}

	// Verify ignored fields are not present
	for _, stat := range results {
		if stat.FieldName == "ignored_field" || stat.FieldName == "non_numeric" {
			t.Errorf("Unexpected field %s in results", stat.FieldName)
		}
	}
}
