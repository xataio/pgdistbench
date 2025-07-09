package script

import (
	"context"
	"strings"
	"testing"
	"time"

	"pgdistbench/api/benchdriverapi"
)

func TestExecuteCommand_SimpleCommand(t *testing.T) {
	ctx := context.Background()
	config := CommandConfig{
		Command:     "echo hello world",
		Environment: map[string]string{},
	}

	result, err := ExecuteCommand(ctx, config)
	if err != nil {
		t.Fatalf("ExecuteCommand failed: %v", err)
	}

	if result.ExitCode != 0 {
		t.Errorf("Expected exit code 0, got %d", result.ExitCode)
	}

	expected := "hello world"
	if strings.TrimSpace(result.Stdout) != expected {
		t.Errorf("Expected stdout '%s', got '%s'", expected, result.Stdout)
	}
}

func TestExecuteCommand_EnvironmentVariableExpansion(t *testing.T) {
	ctx := context.Background()
	config := CommandConfig{
		Command: "echo $TEST_VAR",
		Environment: map[string]string{
			"TEST_VAR": "test_value",
		},
	}

	result, err := ExecuteCommand(ctx, config)
	if err != nil {
		t.Fatalf("ExecuteCommand failed: %v", err)
	}

	expected := "test_value"
	if strings.TrimSpace(result.Stdout) != expected {
		t.Errorf("Expected stdout '%s', got '%s'", expected, result.Stdout)
	}
}

func TestExecuteCommand_NonZeroExitCode(t *testing.T) {
	ctx := context.Background()
	config := CommandConfig{
		Command:     "sh -c 'exit 42'",
		Environment: map[string]string{},
	}

	result, err := ExecuteCommand(ctx, config)
	if err != nil {
		t.Fatalf("ExecuteCommand failed: %v", err)
	}

	if result.ExitCode != 42 {
		t.Errorf("Expected exit code 42, got %d", result.ExitCode)
	}
}

func TestExecuteCommand_StderrCapture(t *testing.T) {
	ctx := context.Background()
	config := CommandConfig{
		Command:     "sh -c 'echo error_message >&2'",
		Environment: map[string]string{},
	}

	result, err := ExecuteCommand(ctx, config)
	if err != nil {
		t.Fatalf("ExecuteCommand failed: %v", err)
	}

	expected := "error_message"
	if strings.TrimSpace(result.Stderr) != expected {
		t.Errorf("Expected stderr '%s', got '%s'", expected, result.Stderr)
	}
}

func TestExecuteCommand_EmptyCommand(t *testing.T) {
	ctx := context.Background()
	config := CommandConfig{
		Command:     "",
		Environment: map[string]string{},
	}

	_, err := ExecuteCommand(ctx, config)
	if err == nil {
		t.Error("Expected error for empty command, got nil")
	}
}

func TestExecuteCommand_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	config := CommandConfig{
		Command:     "sleep 10",
		Environment: map[string]string{},
	}

	start := time.Now()
	ExecuteCommand(ctx, config)
	duration := time.Since(start)
	// Should be cancelled quickly, not take the full 10 seconds
	if duration > 5*time.Second {
		t.Errorf("Command took too long to cancel: %v", duration)
	}
}

// Test OutputProcessor functionality (Story 0 & 1)

func TestExecuteCommand_WithLogProcessor(t *testing.T) {
	ctx := context.Background()
	processor := NewLogProcessor()

	// Use a simpler command that outputs multiple lines
	config := CommandConfig{
		Command:         "sh -c 'echo line1; echo line2; echo line3'",
		Environment:     map[string]string{},
		OutputProcessor: processor,
	}

	result, err := ExecuteCommand(ctx, config)
	if err != nil {
		t.Fatalf("ExecuteCommand with processor failed: %v", err)
	}

	if result.ExitCode != 0 {
		t.Errorf("Expected exit code 0, got %d", result.ExitCode)
	}

	// Check that processor accumulated the output
	stats := processor.GetResults()
	expectedOutput := "line1\nline2\nline3"

	if strings.TrimSpace(stats.Stdout) != expectedOutput {
		t.Errorf("Expected processed output '%s', got '%s'", expectedOutput, stats.Stdout)
	}
}

func TestLogProcessor_ProcessOutput(t *testing.T) {
	processor := NewLogProcessor()

	// Test reading from a reader
	input := "first line\nsecond line\nthird line"
	reader := strings.NewReader(input)

	err := processor.ProcessOutput(reader)
	if err != nil {
		t.Fatalf("ProcessOutput failed: %v", err)
	}

	// Test getting results
	stats := processor.GetResults()
	expected := "first line\nsecond line\nthird line"

	if stats.Stdout != expected {
		t.Errorf("Expected output '%s', got '%s'", expected, stats.Stdout)
	}

	if stats.ExitCode != 0 {
		t.Errorf("Expected exit code 0, got %d", stats.ExitCode)
	}
}

func TestLogProcessor_SetExecutionResults(t *testing.T) {
	processor := NewLogProcessor()

	// Process some output first using the new interface
	input := "some output"
	reader := strings.NewReader(input)
	err := processor.ProcessOutput(reader)
	if err != nil {
		t.Fatalf("ProcessOutput failed: %v", err)
	}

	processor.SetExecutionResults("error message", 1)

	stats := processor.GetResults()

	if stats.Stdout != "some output" {
		t.Errorf("Expected stdout 'some output', got '%s'", stats.Stdout)
	}

	if stats.Stderr != "error message" {
		t.Errorf("Expected stderr 'error message', got '%s'", stats.Stderr)
	}

	if stats.ExitCode != 1 {
		t.Errorf("Expected exit code 1, got %d", stats.ExitCode)
	}
}

func TestLogProcessor_GetFormat(t *testing.T) {
	processor := NewLogProcessor()

	if processor.GetFormat() != benchdriverapi.FormatLog {
		t.Errorf("Expected format 'log', got '%s'", processor.GetFormat())
	}
}

func TestParseAndExpandCommand_SimpleExpansion(t *testing.T) {
	env := []string{"VAR1=value1", "VAR2=value2"}
	cmd := "echo $VAR1 $VAR2"

	result, err := parseAndExpandCommand(cmd, env)
	if err != nil {
		t.Fatalf("parseAndExpandCommand failed: %v", err)
	}

	expected := []string{"echo", "value1", "value2"}
	if len(result) != len(expected) {
		t.Fatalf("Expected %d words, got %d", len(expected), len(result))
	}

	for i, word := range result {
		if word != expected[i] {
			t.Errorf("Expected word %d to be '%s', got '%s'", i, expected[i], word)
		}
	}
}

func TestParseAndExpandCommand_QuotedArguments(t *testing.T) {
	env := []string{"MESSAGE=hello world"}
	cmd := `echo "$MESSAGE"`

	result, err := parseAndExpandCommand(cmd, env)
	if err != nil {
		t.Fatalf("parseAndExpandCommand failed: %v", err)
	}

	expected := []string{"echo", "hello world"}
	if len(result) != len(expected) {
		t.Fatalf("Expected %d words, got %d", len(expected), len(result))
	}

	for i, word := range result {
		if word != expected[i] {
			t.Errorf("Expected word %d to be '%s', got '%s'", i, expected[i], word)
		}
	}
}

func TestBuildEnvironment(t *testing.T) {
	env := BuildEnvironment()

	if len(env) == 0 {
		t.Error("Expected non-empty environment map")
	}

	// Should contain PATH (available on most systems)
	if _, exists := env["PATH"]; !exists {
		t.Error("Expected PATH to be in environment")
	}
}

func TestEnvMapToSlice(t *testing.T) {
	envMap := map[string]string{
		"VAR1": "value1",
		"VAR2": "value2",
	}

	result := envMapToSlice(envMap)

	if len(result) != 2 {
		t.Fatalf("Expected 2 environment variables, got %d", len(result))
	}

	// Check that both variables are present (order might vary)
	found := make(map[string]bool)
	for _, env := range result {
		if env == "VAR1=value1" {
			found["VAR1"] = true
		} else if env == "VAR2=value2" {
			found["VAR2"] = true
		}
	}

	if !found["VAR1"] || !found["VAR2"] {
		t.Errorf("Missing expected environment variables in result: %v", result)
	}
}

func TestExecuteCommand_StderrLogging(t *testing.T) {
	ctx := context.Background()
	processor := NewLogProcessor()

	// Command that produces both stdout and stderr
	config := CommandConfig{
		Command:         "sh -c 'echo stdout_line; echo stderr_line >&2; echo more_stdout'",
		Environment:     map[string]string{},
		OutputProcessor: processor,
	}

	result, err := ExecuteCommand(ctx, config)
	if err != nil {
		t.Fatalf("ExecuteCommand failed: %v", err)
	}

	if result.ExitCode != 0 {
		t.Errorf("Expected exit code 0, got %d", result.ExitCode)
	}

	// Check that stderr was captured (last 20 lines)
	if !strings.Contains(result.Stderr, "stderr_line") {
		t.Errorf("Expected stderr to contain 'stderr_line', got '%s'", result.Stderr)
	}

	// Check that processor got the stdout
	stats := processor.GetResults()
	expectedStdout := "stdout_line\nmore_stdout"
	if strings.TrimSpace(stats.Stdout) != expectedStdout {
		t.Errorf("Expected stdout '%s', got '%s'", expectedStdout, stats.Stdout)
	}

	// Verify stderr is also in the processor results
	if !strings.Contains(stats.Stderr, "stderr_line") {
		t.Errorf("Expected processor stderr to contain 'stderr_line', got '%s'", stats.Stderr)
	}
}
