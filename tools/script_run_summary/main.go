package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"pgdistbench/api/benchdriverapi"
	"pgdistbench/pkg/stats"
	"sort"
	"strconv"
	"strings"
)

// ScriptDocument represents the input JSON structure from script benchmark results
type ScriptDocument struct {
	Runners          []benchdriverapi.ScriptRunStats `json:"runners"`
	AggregatedFields []AggregatedField               `json:"aggregated_fields,omitempty"`
}

type AggregatedField struct {
	FieldName string                 `json:"field_name"`
	Stats     map[string]interface{} `json:"stats"`
}

type Unit string

const (
	UnitNone        Unit = ""
	UnitMillisecond Unit = "ms"
	UnitSecond      Unit = "s"
	UnitRatePerSec  Unit = "rate/s"
	UnitPercent     Unit = "%"
	UnitCount       Unit = "count"
)

type Field struct {
	Name  string
	Unit  Unit
	Value any
}

type FieldStats []Field

// RunSummary represents the summary statistics for one run
type RunSummary struct {
	Fields map[string]FieldStats `json:"fields"`
}

// Configuration for the tool
type Config struct {
	UseAggregates bool
	Fields        []string
	Aggregations  []string
	Units         map[string]string
}

// Default unit mappings
var defaultUnits = map[string]string{
	"time":       "s",
	"tps":        "rate/s",
	"qps":        "rate/s",
	"reconnects": "count",
	"errors":     "count",
	"threads":    "count",
}

// Fields to ignore during auto-detection
var ignoredFields = map[string]bool{
	"time":      true, // sequence field
	"threads":   true, // constant field
	"exit_code": true, // not a stat
}

func main() {
	var config Config
	var fieldsFlag, aggregationsFlag, unitsFlag string

	flag.BoolVar(&config.UseAggregates, "use-aggregates", false, "Use aggregated_stats instead of raw_records")
	flag.StringVar(&fieldsFlag, "fields", "", "Comma-separated list of fields to include")
	flag.StringVar(&aggregationsFlag, "aggregations", "avg,max", "Comma-separated list of aggregations (avg,max,min,sum,count,p99)")
	flag.StringVar(&unitsFlag, "units", "", "Field units as field:unit pairs (e.g., \"tps:rate/s,lat_avg:ms\")")
	flag.Parse()

	if flag.NArg() < 1 {
		log.Fatal("At least one filename is required as a positional argument.")
	}

	// Parse configuration
	config.Aggregations = strings.Split(aggregationsFlag, ",")
	for i := range config.Aggregations {
		config.Aggregations[i] = strings.TrimSpace(config.Aggregations[i])
	}

	config.Units = make(map[string]string)
	// Apply default units
	for k, v := range defaultUnits {
		config.Units[k] = v
	}
	// Apply latency defaults for common latency field patterns
	latencyFields := []string{"lat_avg", "lat_max", "lat_min", "lat_99th", "lat_95th", "lat_50th"}
	for _, field := range latencyFields {
		config.Units[field] = "ms"
	}

	// Parse custom units
	if unitsFlag != "" {
		pairs := strings.Split(unitsFlag, ",")
		for _, pair := range pairs {
			parts := strings.SplitN(strings.TrimSpace(pair), ":", 2)
			if len(parts) == 2 {
				config.Units[parts[0]] = parts[1]
			}
		}
	}

	if fieldsFlag != "" {
		config.Fields = strings.Split(fieldsFlag, ",")
		for i := range config.Fields {
			config.Fields[i] = strings.TrimSpace(config.Fields[i])
		}
	}

	// Process files
	filenames := flag.Args()
	var allSummaries []RunSummary
	var allFields map[string]bool

	for _, filename := range filenames {
		summary, fields, err := fileSummary(filename, config)
		if err != nil {
			log.Fatalf("Failed to get file summary for %s: %v", filename, err)
		}
		allSummaries = append(allSummaries, summary)

		// Collect all available fields
		if allFields == nil {
			allFields = make(map[string]bool)
		}
		for field := range fields {
			allFields[field] = true
		}
	}

	// Determine which fields to display
	displayFields := config.Fields
	if len(displayFields) == 0 {
		// Auto-detect fields
		for field := range allFields {
			displayFields = append(displayFields, field)
		}
		sort.Strings(displayFields)
	}

	// Generate and display the summary table
	displaySummaryTable(allSummaries, displayFields, config)
}

func fileSummary(filename string, config Config) (RunSummary, map[string]bool, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return RunSummary{}, nil, fmt.Errorf("read file: %w", err)
	}

	var doc ScriptDocument
	err = json.Unmarshal(data, &doc)
	if err != nil {
		return RunSummary{}, nil, fmt.Errorf("unmarshal JSON: %w", err)
	}

	summary := RunSummary{Fields: make(map[string]FieldStats)}
	availableFields := make(map[string]bool)

	// Process each runner and aggregate their results
	for _, runner := range doc.Runners {
		var runnerFields map[string]FieldStats
		var runnerAvailableFields map[string]bool

		if !config.UseAggregates && len(runner.RawRecords) > 0 {
			// Calculate stats from raw records
			runnerFields, runnerAvailableFields = calculateFromRawRecords(runner.RawRecords, config.Aggregations)
		} else if len(runner.AggregatedStats) > 0 {
			// Use aggregated stats
			runnerFields, runnerAvailableFields = extractFromAggregatedStats(runner.AggregatedStats, config.Aggregations)
		} else {
			continue // Skip runner with no usable data
		}

		// Merge runner results into summary
		for field, stats := range runnerFields {
			summary.Fields[field] = stats
			availableFields[field] = true
		}

		for field := range runnerAvailableFields {
			availableFields[field] = true
		}
	}

	return summary, availableFields, nil
}

func calculateFromRawRecords(records []map[string]any, aggregations []string) (map[string]FieldStats, map[string]bool) {
	fields := make(map[string]FieldStats)
	availableFields := make(map[string]bool)

	if len(records) == 0 {
		return fields, availableFields
	}

	// Collect all numeric fields and their values
	fieldValues := make(map[string][]float64)

	for _, record := range records {
		for fieldName, value := range record {
			if ignoredFields[fieldName] {
				continue
			}

			// Try to convert to float64
			var floatVal float64
			var err error

			switch v := value.(type) {
			case float64:
				floatVal = v
			case string:
				floatVal, err = strconv.ParseFloat(v, 64)
				if err != nil {
					continue // Skip non-numeric fields
				}
			case int:
				floatVal = float64(v)
			case int64:
				floatVal = float64(v)
			default:
				continue // Skip non-numeric types
			}

			fieldValues[fieldName] = append(fieldValues[fieldName], floatVal)
			availableFields[fieldName] = true
		}
	}

	// Filter out constant fields (all values are the same)
	for fieldName, values := range fieldValues {
		if len(values) > 1 {
			first := values[0]
			isConstant := true
			for _, val := range values[1:] {
				if math.Abs(val-first) > 1e-9 { // Allow small floating point differences
					isConstant = false
					break
				}
			}
			if isConstant {
				delete(fieldValues, fieldName)
				delete(availableFields, fieldName)
				continue
			}
		}

		// Calculate requested aggregations using stats package
		var fieldStats FieldStats

		for _, agg := range aggregations {
			var value float64
			switch strings.ToLower(strings.TrimSpace(agg)) {
			case "avg":
				value = stats.SliceAverage(values)
			case "max":
				value = stats.SliceMax(values)
			case "min":
				value = stats.SliceMin(values)
			case "sum":
				value = stats.SlicesSum(values)
			case "count":
				value = float64(len(values))
			case "p99":
				value = stats.SlicePercentile(values, 99)
			default:
				continue
			}

			fieldStats = append(fieldStats, Field{
				Name:  agg,
				Unit:  UnitNone, // Units will be applied at display time
				Value: value,
			})
		}

		fields[fieldName] = fieldStats
	}

	return fields, availableFields
}

func extractFromAggregatedStats(aggregatedStats []benchdriverapi.AggregatedFieldStats, aggregations []string) (map[string]FieldStats, map[string]bool) {
	fields := make(map[string]FieldStats)
	availableFields := make(map[string]bool)

	for _, stat := range aggregatedStats {
		var fieldStats FieldStats
		availableFields[stat.FieldName] = true

		for _, agg := range aggregations {
			var value float64
			var hasValue bool

			switch strings.ToLower(strings.TrimSpace(agg)) {
			case "avg":
				if stat.Avg != 0 {
					value = stat.Avg
					hasValue = true
				}
			case "max":
				if stat.Max != 0 {
					value = stat.Max
					hasValue = true
				}
			case "min":
				if stat.Min != 0 {
					value = stat.Min
					hasValue = true
				}
			case "sum":
				if stat.Sum != 0 {
					value = stat.Sum
					hasValue = true
				}
			case "count":
				if stat.Count != 0 {
					value = float64(stat.Count)
					hasValue = true
				}
			case "p99":
				if stat.P99 != 0 {
					value = stat.P99
					hasValue = true
				}
			}

			if hasValue {
				fieldStats = append(fieldStats, Field{
					Name:  agg,
					Unit:  UnitNone, // Units will be applied at display time
					Value: value,
				})
			}
		}

		fields[stat.FieldName] = fieldStats
	}

	return fields, availableFields
}

// Helper function to find a field by aggregation name in FieldStats
func findFieldValue(fieldStats FieldStats, aggName string) (float64, bool) {
	for _, field := range fieldStats {
		if field.Name == aggName {
			if val, ok := field.Value.(float64); ok {
				return val, true
			}
		}
	}
	return 0, false
}

func displaySummaryTable(summaries []RunSummary, fields []string, config Config) {
	if len(summaries) == 0 || len(fields) == 0 {
		fmt.Println("No data to display")
		return
	}

	// Build column headers
	var columns []string
	for _, field := range fields {
		for _, agg := range config.Aggregations {
			unit := config.Units[field]
			var header string
			if unit != "" {
				header = fmt.Sprintf("%s(%s)_%s", field, agg, unit)
			} else {
				header = fmt.Sprintf("%s(%s)", field, agg)
			}
			columns = append(columns, header)
		}
	}

	// Print header
	fmt.Println("\nSummary Table:")
	for _, col := range columns {
		fmt.Printf("%-15s ", col)
	}
	fmt.Println()

	// Print separator
	for range columns {
		fmt.Printf("%-15s ", "---------------")
	}
	fmt.Println()

	// Print data rows
	for _, summary := range summaries {
		for _, field := range fields {
			fieldStats, exists := summary.Fields[field]
			if !exists {
				// Print empty values for missing fields
				for range config.Aggregations {
					fmt.Printf("%-15s ", "N/A")
				}
				continue
			}

			for _, agg := range config.Aggregations {
				var value string
				if val, exists := findFieldValue(fieldStats, agg); exists {
					if agg == "count" {
						value = fmt.Sprintf("%d", int(val))
					} else {
						value = fmt.Sprintf("%.2f", val)
					}
				} else {
					value = "N/A"
				}
				fmt.Printf("%-15s ", value)
			}
		}
		fmt.Println()
	}

	// Calculate and print final statistics
	finalStats := calculateFinalStats(summaries, fields, config.Aggregations)

	// Print separator
	for range columns {
		fmt.Printf("%-15s ", "---------------")
	}
	fmt.Println()

	// Print averages
	fmt.Printf("%-15s\n", "Averages")
	printFinalStatsRow(finalStats.Averages, fields, config.Aggregations)

	// Print medians
	fmt.Printf("%-15s\n", "Medians")
	printFinalStatsRow(finalStats.Medians, fields, config.Aggregations)
}

type FinalStats struct {
	Averages map[string]FieldStats
	Medians  map[string]FieldStats
}

func calculateFinalStats(summaries []RunSummary, fields []string, aggregations []string) FinalStats {
	averages := make(map[string]FieldStats)
	medians := make(map[string]FieldStats)

	for _, field := range fields {
		// Collect values for each aggregation type
		aggValues := make(map[string][]float64)

		for _, summary := range summaries {
			if fieldStats, exists := summary.Fields[field]; exists {
				for _, agg := range aggregations {
					if val, exists := findFieldValue(fieldStats, agg); exists {
						aggValues[agg] = append(aggValues[agg], val)
					}
				}
			}
		}

		// Calculate averages and medians using stats package
		var avgStats FieldStats
		var medStats FieldStats

		for _, agg := range aggregations {
			if values, exists := aggValues[agg]; exists && len(values) > 0 {
				avg := stats.SliceAverage(values)
				median := stats.SlicesMedianOf(values, func(v float64) float64 { return v })

				avgStats = append(avgStats, Field{
					Name:  agg,
					Unit:  UnitNone,
					Value: avg,
				})

				medStats = append(medStats, Field{
					Name:  agg,
					Unit:  UnitNone,
					Value: median,
				})
			}
		}

		averages[field] = avgStats
		medians[field] = medStats
	}

	return FinalStats{Averages: averages, Medians: medians}
}

func printFinalStatsRow(statsMap map[string]FieldStats, fields []string, aggregations []string) {
	for _, field := range fields {
		fieldStats, exists := statsMap[field]
		if !exists {
			for range aggregations {
				fmt.Printf("%-15s ", "N/A")
			}
			continue
		}

		for _, agg := range aggregations {
			var value string
			if val, exists := findFieldValue(fieldStats, agg); exists {
				if agg == "count" {
					value = fmt.Sprintf("%d", int(val))
				} else {
					value = fmt.Sprintf("%.2f", val)
				}
			} else {
				value = "N/A"
			}
			fmt.Printf("%-15s ", value)
		}
	}
	fmt.Println()
}
