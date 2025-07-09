package script

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"pgdistbench/api/benchdriverapi"
	"pgdistbench/internal/worker"
)

type Tester struct {
	workerConfig worker.Config
}

type CommandResult struct {
	Stdout   string
	Stderr   string
	ExitCode int
}

func New(cfg worker.Config) (Tester, error) {
	return Tester{
		workerConfig: cfg,
	}, nil
}

func (t *Tester) Close() error {
	// No resources to clean up for now
	return nil
}

func (t *Tester) Prepare(ctx context.Context, cfg benchdriverapi.BenchmarkScriptConfig) (result CommandResult, err error) {
	log.Printf("Script Prepare: starting")
	defer log.Printf("Script Prepare: finished")

	// Execute prepare command if provided (use legacy for prepare/cleanup as they don't need streaming)
	result, err = t.executeCommandSimple(ctx, cfg, cfg.PrepareCmd)
	if err != nil {
		return result, fmt.Errorf("prepare: %w", err)
	}
	return result, err
}

func (t *Tester) Cleanup(ctx context.Context, cfg benchdriverapi.BenchmarkScriptConfig) (result CommandResult, err error) {
	log.Printf("Script Cleanup: starting")
	defer log.Printf("Script Cleanup: finished")

	result, err = t.executeCommandSimple(ctx, cfg, cfg.CleanupCmd)
	if err != nil {
		return result, fmt.Errorf("cleanup: %w", err)
	}
	return result, err
}

func (t *Tester) Run(ctx context.Context, cfg benchdriverapi.BenchmarkScriptConfig) (benchdriverapi.ScriptRunStats, error) {
	log.Printf("Script Run: starting")
	defer log.Printf("Script Run: finished")

	// Execute run command using the new streaming infrastructure
	if cfg.RunCmd != "" {
		stats, err := t.executeCommandAndCollect(ctx, cfg, cfg.RunCmd)
		if err != nil {
			return stats, fmt.Errorf("run command execution failed: %w", err)
		}

		log.Printf("Script Run: exit code %d", stats.ExitCode)
		return stats, nil
	}

	// Return empty stats if no run command
	return benchdriverapi.ScriptRunStats{}, nil
}

// buildEnvironment creates the environment map with DB variables and custom parameters
func (t *Tester) buildEnvironment(cfg benchdriverapi.BenchmarkScriptConfig) map[string]string {
	// Start with current environment
	env := BuildEnvironment()

	optEnv := func(key, value string) {
		if value != "" {
			env[key] = value
		}
	}

	// Add database environment variables from worker.Config
	optEnv("DB_HOST", t.workerConfig.PGHost)
	optEnv("DB_PORT", t.workerConfig.PGPort)
	optEnv("DB_USER", t.workerConfig.PGUser)
	optEnv("DB_PASSWORD", t.workerConfig.PGPass)
	optEnv("DB_NAME", t.workerConfig.PGDatabase)
	optEnv("DB_SSLMODE", t.workerConfig.PGSSLMode)

	// Add custom parameters
	if cfg.Parameters != nil {
		for key, value := range cfg.Parameters {
			if value == "" {
				delete(env, key)
			}
			env[key] = value
		}
	}

	// Add benchmark duration
	duration := "360"
	if cfg.Duration != nil && *cfg.Duration != "" {
		duration = *cfg.Duration
	}
	env["BENCH_DURATION"] = duration

	// Parse duration to seconds for convenience
	if d, err := time.ParseDuration(duration); err == nil {
		env["BENCH_DURATION_SECONDS"] = strconv.Itoa(int(d.Seconds()))
	} else {
		// If parsing fails, assume it's already in seconds or use default
		env["BENCH_DURATION_SECONDS"] = "360"
	}

	return env
}

// createOutputProcessor creates the appropriate output processor based on the configuration
func (t *Tester) createOutputProcessor(cfg benchdriverapi.BenchmarkScriptConfig) OutputProcessor {
	// Determine output format
	format := benchdriverapi.FormatLog // Default format
	if cfg.OutputFormat != nil {
		format = *cfg.OutputFormat
	}

	// Create appropriate processor
	switch format {
	case benchdriverapi.FormatLog:
		return NewLogProcessor()
	case benchdriverapi.FormatJSON:
		// Create JSON processor with aggregation configuration
		aggregationConfig := cfg.AggregationFields
		if aggregationConfig == nil {
			aggregationConfig = make(map[string]benchdriverapi.FieldAggregationConfig)
		}
		aggregator := NewAggregationEngine(aggregationConfig)
		return NewJSONProcessor(aggregator)
	case benchdriverapi.FormatCSV:
		// Create CSV processor with aggregation configuration
		aggregationConfig := cfg.AggregationFields
		if aggregationConfig == nil {
			aggregationConfig = make(map[string]benchdriverapi.FieldAggregationConfig)
		}
		aggregator := NewAggregationEngine(aggregationConfig)

		// Pass CSV headers from configuration
		csvHeaders := cfg.CSVHeaders
		return NewCSVProcessor(aggregator, csvHeaders)
	default:
		log.Printf("Unknown format %s, falling back to log format", format)
		return NewLogProcessor()
	}
}

// executeCommandAndCollect executes a command using the new streaming output processing infrastructure
func (t *Tester) executeCommandAndCollect(ctx context.Context, cfg benchdriverapi.BenchmarkScriptConfig, command string) (benchdriverapi.ScriptRunStats, error) {
	if command == "" {
		return benchdriverapi.ScriptRunStats{}, nil
	}

	// Build environment
	env := t.buildEnvironment(cfg)

	// Determine working directory
	workingDir := ""
	if cfg.ScriptsPath != nil && *cfg.ScriptsPath != "" {
		workingDir = *cfg.ScriptsPath
	}

	// Create output processor
	processor := t.createOutputProcessor(cfg)

	// Create command configuration
	cmdConfig := CommandConfig{
		Command:         command,
		Environment:     env,
		WorkingDir:      workingDir,
		OutputProcessor: processor,
	}

	// Execute command using the command module
	_, err := ExecuteCommand(ctx, cmdConfig)
	if err != nil {
		return benchdriverapi.ScriptRunStats{}, fmt.Errorf("execute command: %w", err)
	}

	// Get final results from processor
	stats := processor.GetResults()

	log.Printf("Script: exit code %d", stats.ExitCode)
	if stats.Stderr != "" {
		log.Printf("Script: stderr: %s", stats.Stderr)
	}
	if len(stats.Stdout) > 0 && len(stats.Stdout) < 1000 {
		log.Printf("Script: stdout: %s", stats.Stdout)
	}

	return stats, nil
}

// executeCommandSimple is a fallback for commands that don't need streaming processing
func (t *Tester) executeCommandSimple(ctx context.Context, cfg benchdriverapi.BenchmarkScriptConfig, command string) (CommandResult, error) {
	if command == "" {
		return CommandResult{}, nil
	}

	// Build environment
	env := t.buildEnvironment(cfg)

	// Determine working directory
	workingDir := ""
	if cfg.ScriptsPath != nil && *cfg.ScriptsPath != "" {
		workingDir = *cfg.ScriptsPath
	}

	// Create command configuration (without processor for legacy mode)
	cmdConfig := CommandConfig{
		Command:     command,
		Environment: env,
		WorkingDir:  workingDir,
	}

	// Execute command using the command module
	result, err := ExecuteCommand(ctx, cmdConfig)
	if err != nil {
		return result, fmt.Errorf("execute command: %w", err)
	}
	log.Printf("Script: exit code %d", result.ExitCode)
	if result.Stderr != "" {
		log.Printf("Script: stderr: %s", result.Stderr)
	}
	if len(result.Stdout) > 0 && len(result.Stdout) < 1000 {
		log.Printf("Script: stdout: %s", result.Stdout)
	}

	return result, err
}
