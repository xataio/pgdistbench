package main

import (
	"fmt"
	"os"

	"pgdistbench/cmd/k8runner/commands"

	// Load systems plugins
	// CNPG support
	_ "pgdistbench/pkg/client/systems/syscnpg"
)

func main() {
	if err := commands.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}
