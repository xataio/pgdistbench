package script

import (
	"testing"

	"pgdistbench/api/benchdriverapi"
)

func TestAggregationEngine_Story4_2_SumAndAverage(t *testing.T) {
	// Story 4.2: Test sum and average calculations
	config := map[string]benchdriverapi.FieldAggregationConfig{
		"sum_field": {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggSum}},
		"avg_field": {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggAvg}},
		"both_field": {Aggregations: []benchdriverapi.AggregationType{
			benchdriverapi.AggSum, benchdriverapi.AggAvg, benchdriverapi.AggCount,
		}},
	}
	aggregator := NewAggregationEngine(config)

	// Process multiple records with known values
	testRecords := []map[string]any{
		{"sum_field": 10.0, "avg_field": 5.0, "both_field": 100.0},
		{"sum_field": 20.0, "avg_field": 15.0, "both_field": 200.0},
		{"sum_field": 30.0, "avg_field": 25.0, "both_field": 300.0},
	}

	for _, record := range testRecords {
		aggregator.ProcessRecord(record)
	}

	// Get results and verify calculations
	results := aggregator.GetResults()

	// Should have exactly 3 fields
	if len(results) != 3 {
		t.Errorf("Expected 3 aggregated fields, got %d", len(results))
	}

	// Create a map for easier verification
	fieldStats := make(map[string]benchdriverapi.AggregatedFieldStats)
	for _, stat := range results {
		fieldStats[stat.FieldName] = stat
	}

	// Test sum_field (sum only)
	if sumStat, exists := fieldStats["sum_field"]; exists {
		if sumStat.Count != 3 {
			t.Errorf("sum_field: expected count 3, got %d", sumStat.Count)
		}
		if sumStat.Sum != 60.0 { // 10 + 20 + 30
			t.Errorf("sum_field: expected sum 60.0, got %f", sumStat.Sum)
		}
		if sumStat.Avg != 0 { // Should not be calculated for sum-only field
			t.Errorf("sum_field: expected avg 0 (not configured), got %f", sumStat.Avg)
		}
	} else {
		t.Error("sum_field not found in results")
	}

	// Test avg_field (average only)
	if avgStat, exists := fieldStats["avg_field"]; exists {
		if avgStat.Count != 3 {
			t.Errorf("avg_field: expected count 3, got %d", avgStat.Count)
		}
		if avgStat.Sum != 0 { // Should not be calculated for avg-only field
			t.Errorf("avg_field: expected sum 0 (not configured), got %f", avgStat.Sum)
		}
		expectedAvg := 45.0 / 3.0 // (5 + 15 + 25) / 3 = 15.0
		if avgStat.Avg != expectedAvg {
			t.Errorf("avg_field: expected avg %f, got %f", expectedAvg, avgStat.Avg)
		}
	} else {
		t.Error("avg_field not found in results")
	}

	// Test both_field (sum, average, and count)
	if bothStat, exists := fieldStats["both_field"]; exists {
		if bothStat.Count != 3 {
			t.Errorf("both_field: expected count 3, got %d", bothStat.Count)
		}
		if bothStat.Sum != 600.0 { // 100 + 200 + 300
			t.Errorf("both_field: expected sum 600.0, got %f", bothStat.Sum)
		}
		expectedAvg := 600.0 / 3.0 // 200.0
		if bothStat.Avg != expectedAvg {
			t.Errorf("both_field: expected avg %f, got %f", expectedAvg, bothStat.Avg)
		}
	} else {
		t.Error("both_field not found in results")
	}
}

func TestAggregationEngine_Story4_2_EdgeCases(t *testing.T) {
	// Test edge cases for sum and average calculations
	config := map[string]benchdriverapi.FieldAggregationConfig{
		"single_value": {Aggregations: []benchdriverapi.AggregationType{
			benchdriverapi.AggSum, benchdriverapi.AggAvg,
		}},
		"zero_values": {Aggregations: []benchdriverapi.AggregationType{
			benchdriverapi.AggSum, benchdriverapi.AggAvg,
		}},
		"negative_values": {Aggregations: []benchdriverapi.AggregationType{
			benchdriverapi.AggSum, benchdriverapi.AggAvg,
		}},
	}
	aggregator := NewAggregationEngine(config)

	// Test single value
	aggregator.ProcessRecord(map[string]any{"single_value": 42.5})

	// Test zero values
	aggregator.ProcessRecord(map[string]any{"zero_values": 0.0})
	aggregator.ProcessRecord(map[string]any{"zero_values": 0.0})

	// Test negative values
	aggregator.ProcessRecord(map[string]any{"negative_values": -10.0})
	aggregator.ProcessRecord(map[string]any{"negative_values": 5.0})
	aggregator.ProcessRecord(map[string]any{"negative_values": -15.0})

	results := aggregator.GetResults()
	fieldStats := make(map[string]benchdriverapi.AggregatedFieldStats)
	for _, stat := range results {
		fieldStats[stat.FieldName] = stat
	}

	// Test single value case
	if singleStat, exists := fieldStats["single_value"]; exists {
		if singleStat.Count != 1 {
			t.Errorf("single_value: expected count 1, got %d", singleStat.Count)
		}
		if singleStat.Sum != 42.5 {
			t.Errorf("single_value: expected sum 42.5, got %f", singleStat.Sum)
		}
		if singleStat.Avg != 42.5 { // Average of single value should be the value itself
			t.Errorf("single_value: expected avg 42.5, got %f", singleStat.Avg)
		}
	} else {
		t.Error("single_value not found in results")
	}

	// Test zero values case
	if zeroStat, exists := fieldStats["zero_values"]; exists {
		if zeroStat.Count != 2 {
			t.Errorf("zero_values: expected count 2, got %d", zeroStat.Count)
		}
		if zeroStat.Sum != 0.0 {
			t.Errorf("zero_values: expected sum 0.0, got %f", zeroStat.Sum)
		}
		if zeroStat.Avg != 0.0 {
			t.Errorf("zero_values: expected avg 0.0, got %f", zeroStat.Avg)
		}
	} else {
		t.Error("zero_values not found in results")
	}

	// Test negative values case
	if negStat, exists := fieldStats["negative_values"]; exists {
		if negStat.Count != 3 {
			t.Errorf("negative_values: expected count 3, got %d", negStat.Count)
		}
		expectedSum := -10.0 + 5.0 + (-15.0) // -20.0
		if negStat.Sum != expectedSum {
			t.Errorf("negative_values: expected sum %f, got %f", expectedSum, negStat.Sum)
		}
		expectedAvg := expectedSum / 3.0 // -20.0 / 3 ≈ -6.67
		if negStat.Avg != expectedAvg {
			t.Errorf("negative_values: expected avg %f, got %f", expectedAvg, negStat.Avg)
		}
	} else {
		t.Error("negative_values not found in results")
	}
}

func TestAggregationEngine_Story4_2_NoConfiguredAggregations(t *testing.T) {
	// Test field with count only (no sum or avg configured)
	config := map[string]benchdriverapi.FieldAggregationConfig{
		"count_only": {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggCount}},
	}
	aggregator := NewAggregationEngine(config)

	// Process some records
	aggregator.ProcessRecord(map[string]any{"count_only": 100.0})
	aggregator.ProcessRecord(map[string]any{"count_only": 200.0})

	results := aggregator.GetResults()
	if len(results) != 1 {
		t.Errorf("Expected 1 aggregated field, got %d", len(results))
	}

	stat := results[0]
	if stat.Count != 2 {
		t.Errorf("Expected count 2, got %d", stat.Count)
	}
	if stat.Sum != 0 { // Should not be calculated when not configured
		t.Errorf("Expected sum 0 (not configured), got %f", stat.Sum)
	}
	if stat.Avg != 0 { // Should not be calculated when not configured
		t.Errorf("Expected avg 0 (not configured), got %f", stat.Avg)
	}
}

func TestAggregationEngine_Story4_3_MinAndMax(t *testing.T) {
	// Story 4.3: Test min and max calculations
	config := map[string]benchdriverapi.FieldAggregationConfig{
		"min_field": {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggMin}},
		"max_field": {Aggregations: []benchdriverapi.AggregationType{benchdriverapi.AggMax}},
		"minmax_field": {Aggregations: []benchdriverapi.AggregationType{
			benchdriverapi.AggMin, benchdriverapi.AggMax, benchdriverapi.AggCount,
		}},
		"all_stats": {Aggregations: []benchdriverapi.AggregationType{
			benchdriverapi.AggSum, benchdriverapi.AggAvg, benchdriverapi.AggMin, benchdriverapi.AggMax, benchdriverapi.AggCount,
		}},
	}
	aggregator := NewAggregationEngine(config)

	// Process multiple records with varying values
	testRecords := []map[string]any{
		{"min_field": 50.0, "max_field": 10.0, "minmax_field": 25.0, "all_stats": 100.0},
		{"min_field": 30.0, "max_field": 70.0, "minmax_field": 75.0, "all_stats": 200.0},
		{"min_field": 80.0, "max_field": 40.0, "minmax_field": 15.0, "all_stats": 50.0},
		{"min_field": 20.0, "max_field": 90.0, "minmax_field": 60.0, "all_stats": 150.0},
	}

	for _, record := range testRecords {
		aggregator.ProcessRecord(record)
	}

	// Get results and verify calculations
	results := aggregator.GetResults()
	if len(results) != 4 {
		t.Errorf("Expected 4 aggregated fields, got %d", len(results))
	}

	// Create a map for easier verification
	fieldStats := make(map[string]benchdriverapi.AggregatedFieldStats)
	for _, stat := range results {
		fieldStats[stat.FieldName] = stat
	}

	// Test min_field (min only): values were [50, 30, 80, 20]
	if minStat, exists := fieldStats["min_field"]; exists {
		if minStat.Count != 4 {
			t.Errorf("min_field: expected count 4, got %d", minStat.Count)
		}
		if minStat.Min != 20.0 { // Minimum of [50, 30, 80, 20]
			t.Errorf("min_field: expected min 20.0, got %f", minStat.Min)
		}
		if minStat.Max != 0 { // Should not be calculated for min-only field
			t.Errorf("min_field: expected max 0 (not configured), got %f", minStat.Max)
		}
	} else {
		t.Error("min_field not found in results")
	}

	// Test max_field (max only): values were [10, 70, 40, 90]
	if maxStat, exists := fieldStats["max_field"]; exists {
		if maxStat.Count != 4 {
			t.Errorf("max_field: expected count 4, got %d", maxStat.Count)
		}
		if maxStat.Max != 90.0 { // Maximum of [10, 70, 40, 90]
			t.Errorf("max_field: expected max 90.0, got %f", maxStat.Max)
		}
		if maxStat.Min != 0 { // Should not be calculated for max-only field
			t.Errorf("max_field: expected min 0 (not configured), got %f", maxStat.Min)
		}
	} else {
		t.Error("max_field not found in results")
	}

	// Test minmax_field (both min and max): values were [25, 75, 15, 60]
	if minmaxStat, exists := fieldStats["minmax_field"]; exists {
		if minmaxStat.Count != 4 {
			t.Errorf("minmax_field: expected count 4, got %d", minmaxStat.Count)
		}
		if minmaxStat.Min != 15.0 { // Minimum of [25, 75, 15, 60]
			t.Errorf("minmax_field: expected min 15.0, got %f", minmaxStat.Min)
		}
		if minmaxStat.Max != 75.0 { // Maximum of [25, 75, 15, 60]
			t.Errorf("minmax_field: expected max 75.0, got %f", minmaxStat.Max)
		}
	} else {
		t.Error("minmax_field not found in results")
	}

	// Test all_stats (all aggregations): values were [100, 200, 50, 150]
	if allStat, exists := fieldStats["all_stats"]; exists {
		if allStat.Count != 4 {
			t.Errorf("all_stats: expected count 4, got %d", allStat.Count)
		}
		if allStat.Sum != 500.0 { // 100 + 200 + 50 + 150
			t.Errorf("all_stats: expected sum 500.0, got %f", allStat.Sum)
		}
		expectedAvg := 500.0 / 4.0 // 125.0
		if allStat.Avg != expectedAvg {
			t.Errorf("all_stats: expected avg %f, got %f", expectedAvg, allStat.Avg)
		}
		if allStat.Min != 50.0 { // Minimum of [100, 200, 50, 150]
			t.Errorf("all_stats: expected min 50.0, got %f", allStat.Min)
		}
		if allStat.Max != 200.0 { // Maximum of [100, 200, 50, 150]
			t.Errorf("all_stats: expected max 200.0, got %f", allStat.Max)
		}
	} else {
		t.Error("all_stats not found in results")
	}
}

func TestAggregationEngine_Story4_3_EdgeCases(t *testing.T) {
	// Test edge cases for min and max calculations
	config := map[string]benchdriverapi.FieldAggregationConfig{
		"single_value": {Aggregations: []benchdriverapi.AggregationType{
			benchdriverapi.AggMin, benchdriverapi.AggMax,
		}},
		"identical_values": {Aggregations: []benchdriverapi.AggregationType{
			benchdriverapi.AggMin, benchdriverapi.AggMax,
		}},
		"negative_values": {Aggregations: []benchdriverapi.AggregationType{
			benchdriverapi.AggMin, benchdriverapi.AggMax,
		}},
		"zero_values": {Aggregations: []benchdriverapi.AggregationType{
			benchdriverapi.AggMin, benchdriverapi.AggMax,
		}},
	}
	aggregator := NewAggregationEngine(config)

	// Test single value
	aggregator.ProcessRecord(map[string]any{"single_value": 42.5})

	// Test identical values
	aggregator.ProcessRecord(map[string]any{"identical_values": 100.0})
	aggregator.ProcessRecord(map[string]any{"identical_values": 100.0})
	aggregator.ProcessRecord(map[string]any{"identical_values": 100.0})

	// Test negative values
	aggregator.ProcessRecord(map[string]any{"negative_values": -10.0})
	aggregator.ProcessRecord(map[string]any{"negative_values": -50.0})
	aggregator.ProcessRecord(map[string]any{"negative_values": -5.0})

	// Test zero values mixed with others
	aggregator.ProcessRecord(map[string]any{"zero_values": 0.0})
	aggregator.ProcessRecord(map[string]any{"zero_values": 5.0})
	aggregator.ProcessRecord(map[string]any{"zero_values": -3.0})

	results := aggregator.GetResults()
	fieldStats := make(map[string]benchdriverapi.AggregatedFieldStats)
	for _, stat := range results {
		fieldStats[stat.FieldName] = stat
	}

	// Test single value case
	if singleStat, exists := fieldStats["single_value"]; exists {
		if singleStat.Min != 42.5 {
			t.Errorf("single_value: expected min 42.5, got %f", singleStat.Min)
		}
		if singleStat.Max != 42.5 {
			t.Errorf("single_value: expected max 42.5, got %f", singleStat.Max)
		}
	} else {
		t.Error("single_value not found in results")
	}

	// Test identical values case
	if identicalStat, exists := fieldStats["identical_values"]; exists {
		if identicalStat.Min != 100.0 {
			t.Errorf("identical_values: expected min 100.0, got %f", identicalStat.Min)
		}
		if identicalStat.Max != 100.0 {
			t.Errorf("identical_values: expected max 100.0, got %f", identicalStat.Max)
		}
	} else {
		t.Error("identical_values not found in results")
	}

	// Test negative values case: [-10, -50, -5]
	if negStat, exists := fieldStats["negative_values"]; exists {
		if negStat.Min != -50.0 { // Most negative
			t.Errorf("negative_values: expected min -50.0, got %f", negStat.Min)
		}
		if negStat.Max != -5.0 { // Least negative
			t.Errorf("negative_values: expected max -5.0, got %f", negStat.Max)
		}
	} else {
		t.Error("negative_values not found in results")
	}

	// Test zero values case: [0, 5, -3]
	if zeroStat, exists := fieldStats["zero_values"]; exists {
		if zeroStat.Min != -3.0 {
			t.Errorf("zero_values: expected min -3.0, got %f", zeroStat.Min)
		}
		if zeroStat.Max != 5.0 {
			t.Errorf("zero_values: expected max 5.0, got %f", zeroStat.Max)
		}
	} else {
		t.Error("zero_values not found in results")
	}
}

func TestAggregationEngine_ComprehensiveIntegration_Stories4_1_4_2_4_3(t *testing.T) {
	// Comprehensive integration test for Stories 4.1, 4.2, and 4.3
	config := map[string]benchdriverapi.FieldAggregationConfig{
		"comprehensive_field": {Aggregations: []benchdriverapi.AggregationType{
			benchdriverapi.AggCount, benchdriverapi.AggSum, benchdriverapi.AggAvg,
			benchdriverapi.AggMin, benchdriverapi.AggMax,
		}},
	}
	aggregator := NewAggregationEngine(config)

	// Process a realistic dataset: [5.5, 2.1, 8.7, 3.3, 9.2, 1.8, 6.4]
	testValues := []float64{5.5, 2.1, 8.7, 3.3, 9.2, 1.8, 6.4}
	for _, value := range testValues {
		aggregator.ProcessRecord(map[string]any{"comprehensive_field": value})
	}

	results := aggregator.GetResults()
	if len(results) != 1 {
		t.Errorf("Expected 1 aggregated field, got %d", len(results))
	}

	stat := results[0]
	if stat.FieldName != "comprehensive_field" {
		t.Errorf("Expected field name 'comprehensive_field', got '%s'", stat.FieldName)
	}

	// Verify all calculated values
	expectedCount := int64(7)
	expectedSum := 37.0       // 5.5 + 2.1 + 8.7 + 3.3 + 9.2 + 1.8 + 6.4
	expectedAvg := 37.0 / 7.0 // ≈ 5.286
	expectedMin := 1.8
	expectedMax := 9.2

	if stat.Count != expectedCount {
		t.Errorf("Expected count %d, got %d", expectedCount, stat.Count)
	}
	if stat.Sum != expectedSum {
		t.Errorf("Expected sum %f, got %f", expectedSum, stat.Sum)
	}
	if stat.Avg != expectedAvg {
		t.Errorf("Expected avg %f, got %f", expectedAvg, stat.Avg)
	}
	if stat.Min != expectedMin {
		t.Errorf("Expected min %f, got %f", expectedMin, stat.Min)
	}
	if stat.Max != expectedMax {
		t.Errorf("Expected max %f, got %f", expectedMax, stat.Max)
	}

	// Verify P99 is not calculated (Story 4.4 not implemented yet)
	if stat.P99 != 0 {
		t.Errorf("Expected P99 0 (not implemented), got %f", stat.P99)
	}
}

// =============================================================================
// SYSBENCH INTEGRATION TESTS
// =============================================================================
// TODO: Add real sysbench output samples when provided by user

func TestSysbenchIntegration_OLTP_ReadWrite_JSON(t *testing.T) {
	// TODO: Test with actual sysbench OLTP read/write JSON output
	// This will validate our JSON processor with real sysbench data
	t.Skip("Waiting for actual sysbench OLTP JSON output samples")

	// Expected structure from sysbench --report-json:
	// {"sql_statistics": {"reads": 1234, "writes": 567, "other": 89},
	//  "latency": {"min": 0.12, "avg": 1.23, "max": 45.67, "95th": 2.34}}

	// When implemented, this test should:
	// 1. Use real sysbench JSON output
	// 2. Configure aggregation for key metrics (reads, writes, latency_avg, etc.)
	// 3. Verify aggregations match expected values
	// 4. Test error resilience with partial/malformed JSON
}

func TestSysbenchIntegration_OLTP_ReadWrite_CSV(t *testing.T) {
	// TODO: Test with actual sysbench OLTP read/write CSV output
	// This will validate our CSV processor with real sysbench data
	t.Skip("Waiting for actual sysbench OLTP CSV output samples")

	// Expected CSV structure might be:
	// timestamp,threads,tps,qps,latency_min,latency_avg,latency_max,latency_95th
	// 2024-01-01T00:00:01Z,16,1234.5,4567.8,0.12,1.23,45.67,2.34

	// When implemented, this test should:
	// 1. Use real sysbench CSV output
	// 2. Configure aggregation for TPS, latency metrics
	// 3. Verify CSV header detection works correctly
	// 4. Test with multiple data rows over time
}

func TestSysbenchIntegration_FileIO_JSON(t *testing.T) {
	// TODO: Test with actual sysbench fileio JSON output
	t.Skip("Waiting for actual sysbench fileio JSON output samples")

	// Expected structure might include:
	// {"io_statistics": {"reads": 1000, "writes": 500, "fsyncs": 100},
	//  "throughput": {"read_mb_s": 123.45, "write_mb_s": 67.89}}

	// When implemented, this test should validate:
	// 1. IO operations aggregation (reads, writes, fsyncs)
	// 2. Throughput metrics (MB/s calculations)
	// 3. Different metric types in single output
}

func TestSysbenchIntegration_CPU_JSON(t *testing.T) {
	// TODO: Test with actual sysbench CPU JSON output
	t.Skip("Waiting for actual sysbench CPU JSON output samples")

	// Expected structure might include:
	// {"cpu_statistics": {"events": 100000, "time": 30.0},
	//  "performance": {"events_per_second": 3333.33}}

	// When implemented, this test should validate:
	// 1. Event counting and rate calculations
	// 2. Time-based metrics
	// 3. Performance ratio calculations
}

func TestSysbenchIntegration_Memory_JSON(t *testing.T) {
	// TODO: Test with actual sysbench memory JSON output
	t.Skip("Waiting for actual sysbench memory JSON output samples")

	// Expected structure might include:
	// {"memory_statistics": {"operations": 50000, "transferred_mb": 1024.0},
	//  "performance": {"mb_per_second": 512.5, "operations_per_second": 1666.67}}

	// When implemented, this test should validate:
	// 1. Memory transfer metrics
	// 2. Operation rate calculations
	// 3. Bandwidth measurements
}

func TestSysbenchIntegration_ErrorResilience_JSON(t *testing.T) {
	// TODO: Test error handling with malformed sysbench JSON output
	t.Skip("Waiting for actual sysbench output samples to create error scenarios")

	// This test should verify:
	// 1. Graceful handling of incomplete JSON objects
	// 2. Recovery from parsing errors mid-stream
	// 3. Proper aggregation of successfully parsed records
	// 4. Error logging and stream drainage
}

func TestSysbenchIntegration_ErrorResilience_CSV(t *testing.T) {
	// TODO: Test error handling with malformed sysbench CSV output
	t.Skip("Waiting for actual sysbench output samples to create error scenarios")

	// This test should verify:
	// 1. Handling of inconsistent CSV field counts
	// 2. Recovery from malformed CSV rows
	// 3. Proper aggregation despite some parsing errors
	// 4. CSV header detection with edge cases
}

func TestSysbenchIntegration_LongRunning_MemoryEfficiency(t *testing.T) {
	// TODO: Test memory efficiency with large sysbench output streams
	t.Skip("Waiting for large sysbench output samples")

	// This test should simulate:
	// 1. Processing thousands of sysbench records
	// 2. Verify constant memory usage (no memory leaks)
	// 3. Performance under sustained load
	// 4. Accuracy of aggregations over large datasets

	// This validates our design decision to exclude P99 (memory-intensive)
	// and confirms our aggregations work efficiently at scale
}
