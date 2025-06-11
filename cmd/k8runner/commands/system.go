package commands

import (
	"context"
	"fmt"
	"pgdistbench/pkg/client/systems"

	"github.com/spf13/cobra"
)

func systemsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "systems",
		Short: "Systems under Test management commands",
	}

	cmd.AddCommand(systemsApplyCmd())
	cmd.AddCommand(systemsDeleteCmd())
	cmd.AddCommand(systemsListCmd())
	cmd.AddCommand(systemsStatusCmd())

	return cmd
}

func systemsDeleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete a resource",
		RunE: func(cmd *cobra.Command, args []string) error {
			br, err := NewBRun()
			if err != nil {
				return err
			}
			defer br.Close()

			cfg, err := readSystemsConfig(args)
			if err != nil {
				return fmt.Errorf("read systems config: %w", err)
			}

			return br.Systems().Delete(context.Background(), cfg)
		},
	}

	return cmd
}

func systemsApplyCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a resource",
		RunE: func(cmd *cobra.Command, args []string) error {
			br, err := NewBRun()
			if err != nil {
				return err
			}
			defer br.Close()

			cfg, err := readSystemsConfig(args)
			if err != nil {
				return fmt.Errorf("read systems config: %w", err)
			}

			list, err := br.Systems().Apply(context.Background(), cfg)
			if err != nil {
				return fmt.Errorf("list resources: %w", err)
			}

			fmt.Printf("Resources: (%d/%d)\n", len(list), cfg.Count)
			for _, sys := range list {
				fmt.Printf("%s: %s\n", sys.Info.Name, sys.Info.Status)
			}

			return nil
		},
	}

	return cmd
}

func systemsListCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List resources",
		RunE: func(cmd *cobra.Command, args []string) error {
			br, err := NewBRun()
			if err != nil {
				return err
			}
			defer br.Close()

			cfg, err := readSystemsConfig(args)
			if err != nil {
				return fmt.Errorf("read systems config: %w", err)
			}

			list, err := br.Systems().List(context.Background(), cfg)
			if err != nil {
				return fmt.Errorf("list resources: %w", err)
			}

			fmt.Printf("Resources: (%d/%d)\n", len(list), cfg.Count)
			for _, sys := range list {
				fmt.Printf("%s: %s\n", sys.Info.Name, sys.Info.Status)
			}

			return nil
		},
	}

	return cmd
}

func systemsStatusCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Get readiness status",
		RunE: func(cmd *cobra.Command, args []string) error {
			br, err := NewBRun()
			if err != nil {
				return err
			}
			defer br.Close()

			cfg, err := readSystemsConfig(args)
			if err != nil {
				return fmt.Errorf("read systems config: %w", err)
			}

			list, err := br.Systems().List(context.Background(), cfg)
			if err != nil {
				return fmt.Errorf("list resources: %w", err)
			}

			if len(list) < cfg.Count {
				return fmt.Errorf("System count mismatch. Run `create` command to create missing instances")
			}

			for _, sys := range list {
				if !sys.Info.Ready {
					return fmt.Errorf("System %s is not ready", sys.Info.Name)
				}
			}

			fmt.Println("All systems are ready")
			return nil
		},
	}

	return cmd
}

func readSystemsConfig(args []string) (systems.SystemsConfig, error) {
	system := "default"
	if len(args) > 0 {
		system = args[0]
	}

	return readConfigFile[systems.SystemsConfig]("systems." + system)
}
