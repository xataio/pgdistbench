package script

import (
	"bufio"
	"io"
	"log"

	"pgdistbench/api/benchdriverapi"
)

type sysbenchProcessor struct {
	engine     fieldProcessor
	stderr     string
	exitCode   int
	collectRaw bool
	// Keep track of parsing statistics for debugging
	totalRecords int64
	parseErrors  int64
	// If enabled, collect all parsed records
	rawRecords []map[string]any
	// Store final statistics for additional analysis
	finalStats map[string]any
}

func NewSysbenchProcessor() *sysbenchProcessor {
	// Create a default aggregation engine with no specific configuration
	aggregator := NewAggregationEngine(nil)
	return &sysbenchProcessor{
		engine:     aggregator,
		collectRaw: false, // Default to not collecting raw records
		finalStats: make(map[string]any),
	}
}

// NewSysbenchProcessorWithConfig creates a new sysbench processor with custom aggregation configuration
func NewSysbenchProcessorWithConfig(aggregator fieldProcessor, collectRaw bool) *sysbenchProcessor {
	return &sysbenchProcessor{
		engine:     aggregator,
		collectRaw: collectRaw,
		finalStats: make(map[string]any),
	}
}

func (p *sysbenchProcessor) ProcessOutput(reader io.Reader) error {
	scanner := bufio.NewScanner(reader)

	parser := newSysbenchParser()

	for scanner.Scan() {
		line := scanner.Text()

		// Parse line using the new unified interface
		result := parser.ParseLine(line)
		if result == nil {
			continue // No match, ignore line
		}

		// Type switch on the result
		switch parsedResult := result.(type) {
		case *intermediateReportResult:
			p.totalRecords++

			// Collect raw record if enabled
			if p.collectRaw {
				p.rawRecords = append(p.rawRecords, parsedResult.Data)
			}

			// Pass to aggregation engine
			p.engine.ProcessRecord(parsedResult.Data)

		case *ParsedSQLStat:
			p.processSQLStat(parsedResult)

		case *ParsedGeneralStat:
			p.processGeneralStat(parsedResult)

		case *ParsedLatencyStat:
			p.processLatencyStat(parsedResult)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Sysbench: Error reading output: %v", err)
		return err
	}

	log.Printf("Sysbench: Processing completed: %d records processed, %d parse errors",
		p.totalRecords, p.parseErrors)

	return nil
}

// processSQLStat processes a parsed SQL statistics entry
func (p *sysbenchProcessor) processSQLStat(sqlStat *ParsedSQLStat) {
	p.finalStats["sql_"+sqlStat.Name] = sqlStat.Value

	// Also store per-second rate if available
	if sqlStat.HasRate {
		p.finalStats["sql_"+sqlStat.Name+"_per_sec"] = sqlStat.PerSec
	}
}

// processGeneralStat processes a parsed General statistics entry
func (p *sysbenchProcessor) processGeneralStat(generalStat *ParsedGeneralStat) {
	p.finalStats["general_"+generalStat.Name] = generalStat.Value
}

// processLatencyStat processes a parsed Latency statistics entry
func (p *sysbenchProcessor) processLatencyStat(latencyStat *ParsedLatencyStat) {
	p.finalStats["latency_"+latencyStat.Name] = latencyStat.Value
}

func (p *sysbenchProcessor) GetResults() benchdriverapi.ScriptRunStats {
	// Get aggregated stats from intermediate reports
	aggregatedStats := p.engine.GetResults()

	// Create individual aggregated fields for each final statistic
	for statName, statValue := range p.finalStats {
		if floatVal, ok := statValue.(float64); ok {
			finalStatField := benchdriverapi.AggregatedFieldStats{
				FieldName: statName,
				Count:     1,
				Sum:       floatVal,
				Avg:       floatVal,
				Min:       floatVal,
				Max:       floatVal,
			}
			aggregatedStats = append(aggregatedStats, finalStatField)
		}
	}

	return benchdriverapi.ScriptRunStats{
		AggregatedStats: aggregatedStats,
		Stderr:          p.stderr,
		ExitCode:        p.exitCode,
		RawRecords:      p.rawRecords,
		// Note: Stdout is not populated for sysbench format since we process structured data
	}
}

func (p *sysbenchProcessor) GetFormat() benchdriverapi.OutputFormat {
	return benchdriverapi.FormatJSON
}

// SetExecutionResults sets stderr and exit code from command execution
func (p *sysbenchProcessor) SetExecutionResults(stderr string, exitCode int) {
	p.stderr = stderr
	p.exitCode = exitCode
}

// SetCollectRaw enables or disables collection of raw parsed records for testing
func (p *sysbenchProcessor) SetCollectRaw(collect bool) {
	p.collectRaw = collect
}
