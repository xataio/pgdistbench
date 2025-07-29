package script

import (
	"io"
	"os"
	"pgdistbench/api/benchdriverapi"
)

type stdoutProcessor struct {
	processor OutputProcessor
}

func StdoutProcessor(processor OutputProcessor) OutputProcessor {
	return &stdoutProcessor{processor: processor}
}

func (p *stdoutProcessor) ProcessOutput(reader io.Reader) error {
	reader = io.TeeReader(reader, os.Stdout)
	return p.processor.ProcessOutput(reader)
}

func (p *stdoutProcessor) GetResults() benchdriverapi.ScriptRunStats {
	return p.processor.GetResults()
}

func (p *stdoutProcessor) GetFormat() benchdriverapi.OutputFormat {
	return p.processor.GetFormat()
}
