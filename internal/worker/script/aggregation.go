package script

import (
	"pgdistbench/api/benchdriverapi"
	"strconv"
)

// AggregationEngine handles real-time aggregation of field values from parsed data
type AggregationEngine struct {
	config map[string]benchdriverapi.FieldAggregationConfig
	fields map[string]*fieldAggregator
}

// fieldAggregator tracks statistics for a single field
type fieldAggregator struct {
	fieldName string
	config    benchdriverapi.FieldAggregationConfig
	// Statistical tracking
	count int64
	sum   float64 // Story 4.2: Track sum for average calculation
	min   float64 // Story 4.3: Track minimum value
	max   float64 // Story 4.3: Track maximum value
}

// NewAggregationEngine creates a new aggregation engine with the given configuration
func NewAggregationEngine(config map[string]benchdriverapi.FieldAggregationConfig) *AggregationEngine {
	return &AggregationEngine{
		config: config,
		fields: make(map[string]*fieldAggregator),
	}
}

// ProcessRecord processes a complete record and extracts configured fields for aggregation
func (e *AggregationEngine) ProcessRecord(record map[string]any) {
	// Process each configured field from the record
	for fieldName, fieldConfig := range e.config {
		// Extract the field value from the record
		rawValue, exists := record[fieldName]
		if !exists {
			continue
		}

		// Try to convert to numeric value
		if numValue, ok := e.ExtractNumericValue(rawValue); ok {
			e.updateField(fieldName, fieldConfig, numValue)
		}
	}
}

// updateField updates a single field with the given numeric value
func (e *AggregationEngine) updateField(fieldName string, fieldConfig benchdriverapi.FieldAggregationConfig, value float64) {
	// Get or create field aggregator
	aggregator, exists := e.fields[fieldName]
	if !exists {
		aggregator = &fieldAggregator{
			fieldName: fieldName,
			config:    fieldConfig,
			count:     0,
			sum:       0,
			min:       value, // Story 4.3: Initialize min with first value
			max:       value, // Story 4.3: Initialize max with first value
		}
		e.fields[fieldName] = aggregator
	}

	// Update count and sum for all values
	aggregator.count++
	aggregator.sum += value

	// Story 4.3: Update min and max values
	if value < aggregator.min {
		aggregator.min = value
	}
	if value > aggregator.max {
		aggregator.max = value
	}
}

// ExtractNumericValue attempts to convert various types to float64 (moved from JSONProcessor)
func (e *AggregationEngine) ExtractNumericValue(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case string:
		// Try to parse string as number
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f, true
		}
		return 0, false
	default:
		return 0, false
	}
}

// GetResults returns the final aggregated statistics for all configured fields
func (e *AggregationEngine) GetResults() []benchdriverapi.AggregatedFieldStats {
	results := make([]benchdriverapi.AggregatedFieldStats, 0, len(e.fields))

	for fieldName, aggregator := range e.fields {
		stats := benchdriverapi.AggregatedFieldStats{
			FieldName: fieldName,
			Count:     aggregator.count,
		}

		// Calculate aggregations based on configuration
		for _, aggType := range aggregator.config.Aggregations {
			switch aggType {
			case benchdriverapi.AggSum:
				stats.Sum = aggregator.sum
			case benchdriverapi.AggAvg:
				if aggregator.count > 0 {
					stats.Avg = aggregator.sum / float64(aggregator.count)
				}
			case benchdriverapi.AggMin: // Story 4.3: Min aggregation
				stats.Min = aggregator.min
			case benchdriverapi.AggMax: // Story 4.3: Max aggregation
				stats.Max = aggregator.max
				// Note: P99 aggregation intentionally not implemented to avoid memory issues with long-running benchmarks
			}
		}

		results = append(results, stats)
	}

	return results
}
