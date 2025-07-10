package main

import (
	"errors"
	"fmt"
	"os"

	"pgdistbench/cmd/k8runner/commands"

	// Load systems plugins
	// CNPG support
	_ "pgdistbench/pkg/client/systems/syscnpg"
)

// ScriptExitError interface to check for script exit errors
type ScriptExitError interface {
	error
	ExitCode() int
}

func main() {
	if err := commands.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)

		// Check if this is a ScriptExitError and exit with the appropriate code
		var scriptErr ScriptExitError
		if errors.As(err, &scriptErr) {
			os.Exit(scriptErr.ExitCode())
		}

		os.Exit(1)
	}
}
