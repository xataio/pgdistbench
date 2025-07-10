package script

import (
	"fmt"
	"io"
	"pgdistbench/api/benchdriverapi"
	"strings"
)

// LogProcessor implements OutputProcessor for raw log output (OutputFormat = "log")
type LogProcessor struct {
	buffer   strings.Builder
	stderr   string
	exitCode int
}

// NewLogProcessor creates a new LogProcessor
func NewLogProcessor() *LogProcessor {
	return &LogProcessor{}
}

// ProcessOutput implements OutputProcessor interface - reads entire stream for log format
func (p *LogProcessor) ProcessOutput(reader io.Reader) error {
	// Copy all data from the reader to the buffer
	_, err := io.Copy(&p.buffer, reader)
	if err != nil {
		return fmt.Errorf("failed to read output stream: %w", err)
	}
	return nil
}

// GetResults implements OutputProcessor interface
func (p *LogProcessor) GetResults() benchdriverapi.ScriptRunStats {
	return benchdriverapi.ScriptRunStats{
		Stdout:   p.buffer.String(),
		Stderr:   p.stderr,
		ExitCode: p.exitCode,
	}
}

// GetFormat implements OutputProcessor interface
func (p *LogProcessor) GetFormat() benchdriverapi.OutputFormat {
	return benchdriverapi.FormatLog
}

// SetExecutionResults sets stderr and exit code from command execution
func (p *LogProcessor) SetExecutionResults(stderr string, exitCode int) {
	p.stderr = stderr
	p.exitCode = exitCode
}
