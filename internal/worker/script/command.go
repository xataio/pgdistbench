package script

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"

	"pgdistbench/api/benchdriverapi"

	"mvdan.cc/sh/v3/expand"
	"mvdan.cc/sh/v3/syntax"
)

// OutputProcessor defines the interface for processing command output streams
type OutputProcessor interface {
	// ProcessOutput processes the entire stdout stream
	ProcessOutput(reader io.Reader) error
	// GetResults returns the final processing results
	GetResults() benchdriverapi.ScriptRunStats
	// GetFormat returns the output format this processor handles
	GetFormat() benchdriverapi.OutputFormat
}

// CommandConfig holds the configuration for command execution
type CommandConfig struct {
	// Command string to execute (e.g., "sysbench --test=oltp_read_write --mysql-host=$DB_HOST run")
	Command string
	// Environment variables as key-value pairs
	Environment map[string]string
	// Working directory for command execution
	WorkingDir string
	// Output processor for handling stdout (optional)
	OutputProcessor OutputProcessor
}

// CommandResult holds the results of command execution
type CommandResult struct {
	Stdout   string
	Stderr   string
	ExitCode int
}

// ExecuteCommand executes a shell command with proper environment variable expansion
func ExecuteCommand(ctx context.Context, config CommandConfig) (CommandResult, error) {
	result := CommandResult{}

	if config.Command == "" {
		log.Printf("No command provided")
		return result, fmt.Errorf("no command provided")
	}

	log.Printf("Executing command: %s", config.Command)

	// Convert environment map to slice format
	envSlice := envMapToSlice(config.Environment)

	// Parse the command string using sh library
	expandedWords, err := parseAndExpandCommand(config.Command, envSlice)
	if err != nil {
		return result, fmt.Errorf("failed to parse and expand command '%s': %w", config.Command, err)
	}

	// Create and configure the command
	cmd := exec.CommandContext(ctx, expandedWords[0], expandedWords[1:]...)
	cmd.Env = envSlice
	if config.WorkingDir != "" {
		cmd.Dir = config.WorkingDir
	}

	// Execute command and capture output
	if config.OutputProcessor != nil {
		return executeWithProcessor(cmd, config.OutputProcessor)
	}
	return executeAndCapture(cmd)
}

// stderrLogger handles stderr with real-time logging and keeps last N lines
type stderrLogger struct {
	lines    []string
	maxLines int
}

func newStderrLogger(maxLines int) *stderrLogger {
	return &stderrLogger{
		lines:    make([]string, 0, maxLines),
		maxLines: maxLines,
	}
}

func (s *stderrLogger) logAndStore(line string) {
	// Log immediately
	log.Printf("Script stderr: %s", line)

	// Store line, maintaining max capacity
	if len(s.lines) >= s.maxLines {
		// Remove oldest line
		copy(s.lines, s.lines[1:])
		s.lines = s.lines[:len(s.lines)-1]
	}
	s.lines = append(s.lines, line)
}

func (s *stderrLogger) getStoredLines() string {
	return strings.Join(s.lines, "\n")
}

// executeWithProcessor executes a command and processes stdout using the provided processor
func executeWithProcessor(cmd *exec.Cmd, processor OutputProcessor) (CommandResult, error) {
	result := CommandResult{}

	// Capture stdout and stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return result, fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return result, fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	// Start command
	if err := cmd.Start(); err != nil {
		return result, fmt.Errorf("failed to start command: %w", err)
	}

	// Process stdout using the processor
	stdoutDone := make(chan error, 1)
	go func() {
		defer close(stdoutDone)
		if err := processor.ProcessOutput(stdout); err != nil {
			stdoutDone <- fmt.Errorf("processor error: %w", err)
			return
		}
		stdoutDone <- nil // Success
	}()

	// Handle stderr with logging and storage
	stderrLogger := newStderrLogger(20) // Keep last 20 lines
	stderrDone := make(chan error, 1)
	go func() {
		defer close(stderrDone)
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			line := scanner.Text()
			stderrLogger.logAndStore(line)
		}
		if err := scanner.Err(); err != nil {
			stderrDone <- fmt.Errorf("stderr scan error: %w", err)
			return
		}
		stderrDone <- nil
	}()

	// Wait for command to complete
	cmdErr := cmd.Wait()

	// Wait for stdout processing to complete, but ignore processor errors for resilient parsing
	processingErr := <-stdoutDone
	if processingErr != nil {
		// Log but don't fail the entire command execution
		// This allows partial data processing as requested
		log.Printf("Output processing error (continuing with partial data): %v", processingErr)
	}

	// Wait for stderr processing
	<-stderrDone

	// Get stored stderr lines
	result.Stderr = stderrLogger.getStoredLines()

	// Get exit code
	if cmdErr != nil {
		if exitError, ok := cmdErr.(*exec.ExitError); ok {
			result.ExitCode = exitError.ExitCode()
		} else {
			// Command failed to run properly (not a script error)
			return result, fmt.Errorf("command execution failed: %w", cmdErr)
		}
	} else {
		result.ExitCode = 0
	}

	// Pass execution results to processors that support it
	type ExecutionResultSetter interface {
		SetExecutionResults(stderr string, exitCode int)
	}

	if resultSetter, ok := processor.(ExecutionResultSetter); ok {
		resultSetter.SetExecutionResults(result.Stderr, result.ExitCode)
	}

	return result, nil
}

// parseAndExpandCommand parses a command string and expands environment variables
func parseAndExpandCommand(cmdStr string, env []string) ([]string, error) {
	// Parse the command string using sh library
	p := syntax.NewParser()
	file, err := p.Parse(strings.NewReader(cmdStr), "")
	if err != nil {
		return nil, fmt.Errorf("failed to parse command: %w", err)
	}

	// Check that we have statements
	if len(file.Stmts) == 0 {
		return nil, fmt.Errorf("no statements found in command")
	}

	// Get the first statement and ensure it's a call expression
	stmt := file.Stmts[0]
	callExpr, ok := stmt.Cmd.(*syntax.CallExpr)
	if !ok {
		return nil, fmt.Errorf("command is not a simple call expression")
	}

	// Expand environment variables using sh library
	expandCfg := &expand.Config{
		Env: expand.ListEnviron(env...),
	}

	var expandedWords []string
	for _, word := range callExpr.Args {
		expanded, err := expand.Fields(expandCfg, word)
		if err != nil {
			return nil, fmt.Errorf("failed to expand word: %w", err)
		}
		expandedWords = append(expandedWords, expanded...)
	}

	if len(expandedWords) == 0 {
		return nil, fmt.Errorf("no words after expansion")
	}

	return expandedWords, nil
}

// executeAndCapture executes a command and captures its output (legacy fallback)
func executeAndCapture(cmd *exec.Cmd) (CommandResult, error) {
	result := CommandResult{}

	// Capture stdout and stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return result, fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return result, fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	// Start command
	if err := cmd.Start(); err != nil {
		return result, fmt.Errorf("failed to start command: %w", err)
	}

	// Read stdout and stderr concurrently
	stdoutCh := make(chan []byte, 1)
	stderrCh := make(chan []byte, 1)

	go func() {
		defer close(stdoutCh)
		if data, err := io.ReadAll(stdout); err == nil {
			stdoutCh <- data
		}
	}()

	go func() {
		defer close(stderrCh)
		if data, err := io.ReadAll(stderr); err == nil {
			stderrCh <- data
		}
	}()

	// Wait for command to complete
	cmdErr := cmd.Wait()

	// Collect results
	if stdoutData := <-stdoutCh; stdoutData != nil {
		result.Stdout = string(stdoutData)
	}
	if stderrData := <-stderrCh; stderrData != nil {
		result.Stderr = string(stderrData)
	}

	// Get exit code
	if cmdErr != nil {
		if exitError, ok := cmdErr.(*exec.ExitError); ok {
			result.ExitCode = exitError.ExitCode()
		} else {
			// Command failed to run properly (not a script error)
			return result, fmt.Errorf("command execution failed: %w", cmdErr)
		}
	} else {
		result.ExitCode = 0
	}

	return result, nil
}

// envMapToSlice converts environment map to slice format expected by exec.Cmd
func envMapToSlice(env map[string]string) []string {
	result := make([]string, 0, len(env))
	for key, value := range env {
		result = append(result, fmt.Sprintf("%s=%s", key, value))
	}
	return result
}

// BuildEnvironment creates an environment map from the current process environment
func BuildEnvironment() map[string]string {
	env := make(map[string]string)

	// Copy current environment
	for _, envVar := range os.Environ() {
		parts := strings.SplitN(envVar, "=", 2)
		if len(parts) == 2 {
			env[parts[0]] = parts[1]
		}
	}

	return env
}
