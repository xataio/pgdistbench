package script

import (
	"encoding/json"
	"io"
	"log"

	"pgdistbench/api/benchdriverapi"
)

// JSONProcessor implements OutputProcessor for JSON output format
type JSONProcessor struct {
	engine   fieldProcessor
	stderr   string
	exitCode int
	// Keep track of parsing statistics for debugging
	totalRecords int64
	parseErrors  int64
	// If enabled, collect all parsed records
	rawRecords []map[string]any
	collectRaw bool
}

type fieldProcessor interface {
	ProcessRecord(record map[string]any)
	GetResults() []benchdriverapi.AggregatedFieldStats
}

// NewJSONProcessor creates a new JSONProcessor with the given aggregation configuration
func NewJSONProcessor(aggregator fieldProcessor, collectRaw bool) *JSONProcessor {
	return &JSONProcessor{
		engine:     aggregator,
		collectRaw: collectRaw,
	}
}

// ProcessOutput implements OutputProcessor interface - reads JSON objects from stream
func (p *JSONProcessor) ProcessOutput(reader io.Reader) error {
	decoder := json.NewDecoder(reader)

	// Process JSON objects one by one from the stream
	for {
		var jsonObj map[string]interface{}
		err := decoder.Decode(&jsonObj)

		if err == io.EOF {
			// End of stream - this is normal
			break
		}

		if err != nil {
			// Once we get a decoder error, the decoder is in an invalid state
			// Log the error and drain the rest of the stream without processing
			p.parseErrors++
			log.Printf("JSON parsing error, draining remaining stream: %v", err)

			// Drain the rest of the reader to ensure pipes are exhausted
			_, drainErr := io.Copy(io.Discard, reader)
			if drainErr != nil {
				log.Printf("Error draining stream: %v", drainErr)
			}
			break
		}

		// Successfully parsed a JSON object
		p.totalRecords++

		// Collect raw record if enabled
		if p.collectRaw {
			p.rawRecords = append(p.rawRecords, jsonObj)
		}

		// Pass the entire record to the aggregation engine
		p.engine.ProcessRecord(jsonObj)
	}

	log.Printf("JSON processing completed: %d records processed, %d parse errors",
		p.totalRecords, p.parseErrors)

	return nil
}

// GetResults implements OutputProcessor interface
func (p *JSONProcessor) GetResults() benchdriverapi.ScriptRunStats {
	return benchdriverapi.ScriptRunStats{
		AggregatedStats: p.engine.GetResults(),
		Stderr:          p.stderr,
		ExitCode:        p.exitCode,
		RawRecords:      p.rawRecords,
		// Note: Stdout is not populated for JSON format since we process structured data
	}
}

// GetFormat implements OutputProcessor interface
func (p *JSONProcessor) GetFormat() benchdriverapi.OutputFormat {
	return benchdriverapi.FormatJSON
}

// SetExecutionResults sets stderr and exit code from command execution
func (p *JSONProcessor) SetExecutionResults(stderr string, exitCode int) {
	p.stderr = stderr
	p.exitCode = exitCode
}
