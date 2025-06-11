package commands

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/goccy/go-yaml"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"kcl-lang.io/kcl-go"
	"kcl-lang.io/kcl-go/pkg/utils"

	brun "pgdistbench/pkg/client"
)

var rootCmd = &cobra.Command{
	Use:          "benchrun",
	SilenceUsage: true,
}

var (
	k8sctx     = ""
	workdir    = "." // root of `main.k` file to load configurations from
	mainConfig = ""
)

func init() {
	viper.SetEnvPrefix("BENCHRUN")
	viper.AutomaticEnv()

	rootCmd.PersistentFlags().StringVarP(&workdir, "workdir", "w", ".", "Root directory to load configuration files from")
	rootCmd.PersistentFlags().StringVarP(&mainConfig, "main", "m", "", "Path to the main configuration file (defaults to main.yaml, main.k, or main.kcl)")
	rootCmd.PersistentFlags().StringVarP(&k8sctx, "context", "c", "", "Kubernetes context to use")
}

func Execute() error {
	rootCmd.AddCommand(systemsCmd())
	rootCmd.AddCommand(testCmd())
	rootCmd.AddCommand(runnerCmd())
	rootCmd.AddCommand(execCmd())
	return rootCmd.Execute()
}

func NewBRun() (*brun.Client, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	configOverrides := &clientcmd.ConfigOverrides{}
	if k8sctx != "" {
		configOverrides.CurrentContext = k8sctx
	}
	kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides)
	config, err := kubeConfig.ClientConfig()
	if err != nil {
		return nil, err
	}

	return brun.New(
		config,
	), nil
}

func testCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "test",
		Short: "Test commands",
		RunE: func(cmd *cobra.Command, args []string) error {
			br, err := NewBRun()
			if err != nil {
				return err
			}
			defer br.Close()

			config := br.RestConfig()
			config.ContentType = "application/json"
			config.APIPath = "api/v1/namespaces/default/pods/pgdistbench/proxy/"
			config.NegotiatedSerializer = serializer.NewCodecFactory(scheme.Scheme)
			config.UserAgent = rest.DefaultKubernetesUserAgent()
			rc, err := rest.UnversionedRESTClientFor(&config)
			if err != nil {
				return err
			}

			requ := rc.Get()
			fmt.Println(requ.URL().String())

			contents, err := requ.Do(context.Background()).Raw()
			if err != nil {
				return err
			}
			fmt.Println(string(contents))

			return nil
		},
	}

	return cmd
}

func readConfigFile[T any](selector string) (cfg T, err error) {
	if mainConfig == "" {
		rootDir := workdir
		if rootDir == "" {
			rootDir = "."
		}

		for _, file := range []string{"main.yaml", "main.yml", "main.k", "main.kcl"} {
			fullPath := filepath.Join(rootDir, file)
			if _, err := os.Stat(fullPath); err == nil {
				mainConfig = fullPath
				break
			}
		}
	}

	if strings.HasSuffix(mainConfig, ".k") || strings.HasSuffix(mainConfig, ".kcl") {
		return readKCLConfig[T](selector)
	}
	return readYamlConfig[T](selector)
}

func readYamlConfig[T any](selector string) (cfg T, err error) {
	var in *os.File
	if mainConfig == "" || mainConfig == "-" {
		in = os.Stdin
	} else {
		in, err = os.Open(mainConfig)
		if err != nil {
			return cfg, fmt.Errorf("open config file: %w", err)
		}
		defer in.Close()
	}

	if selector != "" {
		var path *yaml.Path
		path, err = yaml.PathString(fmt.Sprintf("$.%s", selector))
		if err != nil {
			panic(err)
		}

		err = path.Read(in, &cfg)
	} else {
		err = yaml.NewDecoder(in).Decode(&cfg)
	}

	if err != nil {
		return cfg, fmt.Errorf("decode yaml config file: %w", err)
	}
	return cfg, nil
}

type kclMod struct {
	Dependencies map[string]kclDependency `toml:"dependencies"`
}

type kclDependency struct {
	Path    string `toml:"path"`
	Version string `toml:"version"`
}

var noKCLMod = errors.New("no kcl.mod set")

func tryKclMod(workdir string) (mod kclMod, rootDir string, err error) {
	rootDir, err = utils.FindPkgRoot(workdir)
	if err != nil {
		return mod, rootDir, noKCLMod
	}

	modFile := filepath.Join(rootDir, "kcl.mod")
	_, err = toml.DecodeFile(modFile, &mod)
	if err != nil {
		return mod, rootDir, fmt.Errorf("decode kcl.mod file: %w", err)
	}

	return mod, rootDir, nil
}

func readKCLConfig[T any](selector string) (cfg T, err error) {
	var res *kcl.KCLResultList

	var files []string
	isStdin := mainConfig == "-"
	if !isStdin {
		files = append(files, mainConfig)
	}

	if workdir == "" {
		workdir, err = os.Getwd()
		if err != nil {
			return cfg, fmt.Errorf("get current working directory: %w", err)
		}
	}

	var opts []kcl.Option
	opts = append(opts, kcl.WithWorkDir(workdir))

	if isStdin {
		body, err := io.ReadAll(os.Stdin)
		if err != nil {
			return cfg, err
		}

		opts = append(opts, kcl.WithCode(string(body)))
	}
	if selector != "" {
		opts = append(opts, kcl.WithSelectors(selector))
	}

	opts = append(opts, kcl.WithLogger(os.Stdout))

	{
		mod, rootDir, err := tryKclMod(workdir)
		if err != nil && !errors.Is(err, noKCLMod) {
			return cfg, err
		}

		for k, dep := range mod.Dependencies {
			if dep.Path == "" {
				continue
			}

			depPath := filepath.Clean(filepath.Join(rootDir, dep.Path))
			if _, err := os.Stat(depPath); err != nil {
				return cfg, fmt.Errorf("dependency %s not found: %w", k, err)
			}

			opts = append(opts, kcl.WithExternalPkgAndPath(k, depPath))
		}
	}

	if len(files) > 0 {
		res, err = kcl.RunFiles(files, opts...)
	} else {
		res, err = kcl.Run("<stdin>", opts...)
	}
	if err != nil {
		return cfg, err
	}

	yamlContents := res.GetRawYamlResult()
	in := strings.NewReader(yamlContents)
	err = yaml.NewDecoder(in).Decode(&cfg)
	return cfg, err
}
