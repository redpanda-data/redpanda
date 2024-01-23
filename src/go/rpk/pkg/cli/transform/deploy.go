/*
* Copyright 2023 Redpanda Data, Inc.
*
* Use of this software is governed by the Business Source License
* included in the file licenses/BSL.md
*
* As of the Change Date specified in that file, in accordance with
* the Business Source License, use of this software will be governed
* by the Apache License, Version 2.0
 */

package transform

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/transform/project"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/httpapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newDeployCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var fc deployFlagConfig
	var file string

	cmd := &cobra.Command{
		Use:   "deploy [WASM]",
		Short: "Deploy a transform",
		Long: `Deploy a transform.

When run in the same directory as a transform.yaml, this reads the configuration
file, then looks for a .wasm file with the same name as your project. If the
input and output topics are specified in the configuration file, those are used.
Otherwise, the topics can be specified on the command line using the 
--input-topic and --output-topic flags.

To deploy Wasm files directly without a transform.yaml file:

  rpk transform deploy --file transform.wasm --name myTransform \
    --input-topic my-topic-1 \
    --output-topic my-topic-2

Environment variables can be specified for the transform using the --var flag, these
are separated by an equals for example: --var=KEY=VALUE

The --var flag can be repeated to specify multiple variables like so:

  rpk transform deploy --var FOO=BAR --var FIZZ=BUZZ
`,
		Args: cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			api, err := adminapi.NewClient(fs, p)
			out.MaybeDie(err, "unable to initialize admin api client: %v", err)

			cfg := fc.ToProjectConfig()

			fileConfig, err := project.LoadCfg(fs)
			// We allow users to deploy if they aren't in a directory with transform.yaml
			// in that case all config needs to be specified on the command line.
			if err == nil {
				cfg = mergeProjectConfigs(fileConfig, cfg)
			}
			err = validateProjectConfig(cfg, err)
			out.MaybeDieErr(err)

			if cfg.InputTopic == "" {
				cfg.InputTopic, err = out.Prompt("Select an input topic:")
				out.MaybeDie(err, "no input topic: %v", err)
				if cfg.InputTopic == "" {
					out.Die("missing input topic")
				}
			}
			if cfg.OutputTopic == "" {
				cfg.OutputTopic, err = out.Prompt("Select an output topic:")
				out.MaybeDie(err, "no output topic: %v", err)
				if cfg.OutputTopic == "" {
					out.Die("missing output topic")
				}
			}

			if file == "" {
				file = fmt.Sprintf("%s.wasm", cfg.Name)
			}
			var wasm []byte
			if strings.HasPrefix(file, "https://") || strings.HasPrefix(file, "http://") {
				wasm, err = loadWasmFromNetwork(cmd.Context(), file)
			} else {
				wasm, err = loadWasmFromDisk(fs, file)
			}
			out.MaybeDieErr(err)

			t := adminapi.TransformMetadata{
				InputTopic:   cfg.InputTopic,
				OutputTopics: []string{cfg.OutputTopic},
				Name:         cfg.Name,
				Status:       nil,
				Environment:  mapToEnvVars(cfg.Env),
			}
			err = api.DeployWasmTransform(cmd.Context(), t, wasm)
			if he := (*adminapi.HTTPResponseError)(nil); errors.As(err, &he) {
				if he.Response.StatusCode == 400 {
					body, bodyErr := he.DecodeGenericErrorBody()
					if bodyErr == nil {
						out.Die("unable to deploy transform %s: %s", cfg.Name, body.Message)
					}
				}
			}
			out.MaybeDie(err, "unable to deploy transform %s: %v", cfg.Name, err)

			fmt.Printf("transform %q deployed.\n", cfg.Name)
		},
	}
	cmd.Flags().StringVar(&file, "file", "", "The WebAssembly module to deploy")

	cmd.Flags().StringVarP(&fc.inputTopic, "input-topic", "i", "", "The input topic to apply the transform to")
	cmd.Flags().StringVarP(&fc.outputTopic, "output-topic", "o", "", "The output topic to write the transform results to")
	cmd.Flags().StringVar(&fc.functionName, "name", "", "The name of the transform")
	cmd.Flags().Var(&fc.env, "var", "Specify an environment variable in the form of KEY=VALUE")
	return cmd
}

type environment struct {
	vars map[string]string
}

func (e *environment) Set(s string) error {
	i := strings.IndexByte(s, '=')
	if i == -1 {
		return errors.New("missing value")
	}
	k := s[:i]
	if k == "" {
		return errors.New("missing key")
	}
	v := s[i+1:]
	if v == "" {
		return errors.New("missing value")
	}
	if e.vars == nil {
		e.vars = make(map[string]string)
	}
	e.vars[k] = v
	return nil
}

func (*environment) Type() string {
	return "environmentVariable"
}

func (e *environment) String() string {
	if e.vars == nil {
		return ""
	}
	vars := make([]string, 0)
	for k, v := range e.vars {
		vars = append(vars, k+"="+v)
	}
	return strings.Join(vars, ", ")
}

type deployFlagConfig struct {
	inputTopic   string
	outputTopic  string
	functionName string
	env          environment
}

// ToProjectConfig creates a project.Config from the specified command line flags.
func (fc deployFlagConfig) ToProjectConfig() (out project.Config) {
	out.Name = fc.functionName
	out.InputTopic = fc.inputTopic
	out.OutputTopic = fc.outputTopic
	out.Env = fc.env.vars
	return out
}

// mergeProjectConfigs overlays the rhs configuration (if specified) over lhs and returns a new config.
func mergeProjectConfigs(lhs project.Config, rhs project.Config) (out project.Config) {
	out = lhs
	if rhs.Name != "" {
		out.Name = rhs.Name
	}
	// for environment variables we merge the maps with the command line taking
	// precedence over the config file.
	m := map[string]string{}
	if lhs.Env != nil {
		for k, v := range lhs.Env {
			m[k] = v
		}
	}
	if rhs.Env != nil {
		for k, v := range rhs.Env {
			m[k] = v
		}
	}
	out.Env = m
	if rhs.InputTopic != "" {
		out.InputTopic = rhs.InputTopic
	}
	if rhs.OutputTopic != "" {
		out.OutputTopic = rhs.OutputTopic
	}
	return out
}

// isEmptyProjectConfig checks if a project config is completely empty.
func isEmptyProjectConfig(cfg project.Config) bool {
	return cfg.Name == "" && cfg.InputTopic == "" && cfg.OutputTopic == "" && (cfg.Env == nil || len(cfg.Env) == 0)
}

// validateProjectConfig validates the merged command line and file configurations.
func validateProjectConfig(cfg project.Config, fileConfigErr error) error {
	// If the user just typed `rpk transform deploy` then we assume they expected to take the configuration values from
	// the file, so print out that error.
	if isEmptyProjectConfig(cfg) && fileConfigErr != nil {
		return fmt.Errorf("unable to find %q: %v", project.ConfigFileName, fileConfigErr)
	}
	if cfg.Name == "" {
		return errors.New("missing name")
	}
	return nil
}

// loadWasmFromDisk loads the wasm file and ensures the magic bytes are correct.
func loadWasmFromDisk(fs afero.Fs, path string) ([]byte, error) {
	contents, err := afero.ReadFile(fs, path)
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("missing %q did you run `rpk transform build`", path)
	}
	if err != nil {
		return nil, fmt.Errorf("unable to read %q: %v", path, err)
	}
	return contents, nil
}

// loadWasmFromDisk downloads the wasm file and ensures the magic bytes are correct.
func loadWasmFromNetwork(ctx context.Context, url string) ([]byte, error) {
	client := httpapi.NewClient(httpapi.ReqTimeout(120 * time.Second))
	var contents []byte
	if err := client.Get(ctx, url, nil, &contents); err != nil {
		return nil, fmt.Errorf("unable to fetch wasm file: %v", err)
	}
	return contents, nil
}

// mapToEnvVars converts a map to the adminapi environment variable type.
func mapToEnvVars(env map[string]string) (vars []adminapi.EnvironmentVariable) {
	if env == nil {
		return
	}
	for k, v := range env {
		vars = append(vars, adminapi.EnvironmentVariable{
			Key:   k,
			Value: v,
		})
	}
	return
}
