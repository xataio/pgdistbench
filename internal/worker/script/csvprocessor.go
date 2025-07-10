package script

import (
	"encoding/csv"
	"io"
	"log"
	"strconv"

	"pgdistbench/api/benchdriverapi"
)

// CSVProcessor implements OutputProcessor for CSV output format
type CSVProcessor struct {
	engine     fieldProcessor
	csvHeaders []string // Configured headers or detected from first line
	stderr     string
	exitCode   int
	// Keep track of parsing statistics for debugging
	totalRecords int64
	parseErrors  int64
	// If enabled, collect all parsed records
	rawRecords []map[string]any
	collectRaw bool
}

// NewCSVProcessor creates a new CSVProcessor with the given aggregation configuration and headers
func NewCSVProcessor(aggregator fieldProcessor, csvHeaders []string, collectRaw bool) *CSVProcessor {
	return &CSVProcessor{
		engine:     aggregator,
		csvHeaders: csvHeaders,
		collectRaw: collectRaw,
	}
}

// ProcessOutput implements OutputProcessor interface - reads CSV records from stream
func (p *CSVProcessor) ProcessOutput(reader io.Reader) error {
	csvReader := csv.NewReader(reader)

	// Allow variable field counts to handle missing fields gracefully
	csvReader.FieldsPerRecord = -1

	var headers []string
	var isFirstLine bool = true

	// Determine headers based on configuration
	if len(p.csvHeaders) > 0 {
		// Use configured headers - treat all lines as data
		headers = p.csvHeaders
		isFirstLine = false
		log.Printf("CSV: Using configured headers: %v", headers)
	}

	// Process CSV records one by one from the stream
	for {
		record, err := csvReader.Read()

		if err == io.EOF {
			// End of stream - this is normal
			break
		}

		if err != nil {
			// Once we get a CSV parsing error, log it and drain the rest of the stream
			p.parseErrors++
			log.Printf("CSV parsing error, draining remaining stream: %v", err)

			// Drain the rest of the reader to ensure pipes are exhausted
			_, drainErr := io.Copy(io.Discard, reader)
			if drainErr != nil {
				log.Printf("Error draining stream: %v", drainErr)
			}
			break
		}

		// Handle header detection from first line if needed
		if isFirstLine && len(p.csvHeaders) == 0 {
			headers = p.detectHeaders(record)
			isFirstLine = false
			log.Printf("CSV: Detected/generated headers: %v", headers)
			continue
		}

		// Skip empty records
		if len(record) == 0 {
			continue
		}

		// Successfully parsed a CSV record
		p.totalRecords++

		// Convert CSV record to map for aggregation engine
		recordMap := p.convertRecordToMap(headers, record)

		// Collect raw record if enabled
		if p.collectRaw {
			p.rawRecords = append(p.rawRecords, recordMap)
		}

		// Pass the record to the aggregation engine
		p.engine.ProcessRecord(recordMap)
	}

	log.Printf("CSV processing completed: %d records processed, %d parse errors",
		p.totalRecords, p.parseErrors)

	return nil
}

// detectHeaders determines headers from the first CSV line or generates default ones
func (p *CSVProcessor) detectHeaders(firstLine []string) []string {
	if len(firstLine) == 0 {
		return []string{}
	}

	// Check if first line looks like headers (non-empty strings that are not all numeric)
	looksLikeHeaders := false
	for _, field := range firstLine {
		if field != "" {
			// If any field is non-numeric, assume this is a header line
			if _, err := strconv.ParseFloat(field, 64); err != nil {
				looksLikeHeaders = true
				break
			}
		}
	}

	if looksLikeHeaders {
		// Use first line as headers
		return firstLine
	}

	// Generate default headers "1", "2", "3", etc.
	headers := make([]string, len(firstLine))
	for i := range headers {
		headers[i] = strconv.Itoa(i + 1)
	}

	// Since we're generating headers, we need to process this first line as data
	p.totalRecords++
	recordMap := p.convertRecordToMap(headers, firstLine)
	p.engine.ProcessRecord(recordMap)

	return headers
}

// convertRecordToMap converts a CSV record ([]string) to a map using the headers
func (p *CSVProcessor) convertRecordToMap(headers []string, record []string) map[string]any {
	recordMap := make(map[string]any)

	// Process each field in the record
	for i, value := range record {
		// Use header name if available, otherwise skip extra fields
		if i < len(headers) {
			fieldName := headers[i]
			if fieldName != "" {
				recordMap[fieldName] = value
			}
		}
	}

	// Handle case where record has fewer fields than headers
	// (missing fields will simply not be present in the map, which is fine)

	return recordMap
}

// GetResults implements OutputProcessor interface
func (p *CSVProcessor) GetResults() benchdriverapi.ScriptRunStats {
	return benchdriverapi.ScriptRunStats{
		AggregatedStats: p.engine.GetResults(),
		Stderr:          p.stderr,
		ExitCode:        p.exitCode,
		RawRecords:      p.rawRecords,
		// Note: Stdout is not populated for CSV format since we process structured data
	}
}

// GetFormat implements OutputProcessor interface
func (p *CSVProcessor) GetFormat() benchdriverapi.OutputFormat {
	return benchdriverapi.FormatCSV
}

// SetExecutionResults sets stderr and exit code from command execution
func (p *CSVProcessor) SetExecutionResults(stderr string, exitCode int) {
	p.stderr = stderr
	p.exitCode = exitCode
}
