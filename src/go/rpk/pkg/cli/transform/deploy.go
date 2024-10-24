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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redpanda-data/common-go/rpadmin"

	dataplanev1alpha1 "buf.build/gen/go/redpandadata/dataplane/protocolbuffers/go/redpanda/api/dataplane/v1alpha1"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/transform/project"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/httpapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newDeployCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var fc deployFlagConfig
	var file string

	cmd := &cobra.Command{
		Use:   "deploy",
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
    --output-topic my-topic-3

Environment variables can be specified for the transform using the --var flag, these
are separated by an equals for example: --var=KEY=VALUE

The --var flag can be repeated to specify multiple variables like so:

  rpk transform deploy --var FOO=BAR --var FIZZ=BUZZ

The --from-offset flag can be used to specify where on the input topic the transform
should begin processing. Expressed as:

  - @T - Begin reading records with committed timestamp >= T (UNIX time, ms from epoch)
  - +N - Begin reading N records from the start of each input partition
  - -N - Begin reading N records prior to the end of each input partition

Note that the broker will only respect from-offset on the first deploy for a given
transform. Re-deploying the transform will cause processing to pick up at the last
committed offset. Recall that this state is maintained until the transform is deleted.
`,
		Args: cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitServerlessAdmin(p)

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
			if len(cfg.OutputTopics) == 0 {
				ot, err := out.Prompt("Select an output topic:")
				out.MaybeDie(err, "no output topic: %v", err)
				if ot == "" {
					out.Die("missing output topic")
				}
				cfg.OutputTopics = []string{ot}
			}
			if cfg.Compression == "" {
				cfg.Compression = "none"
			}

			if file == "" {
				file = fmt.Sprintf("%s.wasm", cfg.Name)
			}
			var wasm io.Reader
			if strings.HasPrefix(file, "https://") || strings.HasPrefix(file, "http://") {
				wasm, err = loadWasmFromNetwork(cmd.Context(), file)
			} else {
				wasm, err = loadWasmFromDisk(fs, file)
			}
			out.MaybeDieErr(err)

			offset, err := parseOffset(cfg.FromOffset)
			out.MaybeDieErr(err)

			t := rpadmin.TransformMetadata{
				InputTopic:      cfg.InputTopic,
				OutputTopics:    cfg.OutputTopics,
				Name:            cfg.Name,
				Status:          nil,
				Environment:     mapToEnvVars(cfg.Env),
				CompressionMode: cfg.Compression,
				FromOffset:      offset,
			}
			if p.FromCloud && !p.CloudCluster.IsServerless() {
				url, err := p.CloudCluster.CheckClusterURL()
				out.MaybeDie(err, "unable to get cluster information: %v", err)

				cl, err := publicapi.NewDataPlaneClientSet(url, p.CurrentAuth().AuthToken)
				out.MaybeDie(err, "unable to initialize cloud client: %v", err)

				err = cl.Transform.DeployTransform(cmd.Context(), publicapi.DeployTransformRequest{
					Metadata:   adminAPIToDataplaneMetadata(t),
					WasmBinary: wasm,
				})
				out.MaybeDie(err, "unable to deploy transform to Cloud Cluster: %v", err)
			} else {
				api, err := adminapi.NewClient(cmd.Context(), fs, p)
				out.MaybeDie(err, "unable to initialize admin api client: %v", err)

				err = api.DeployWasmTransform(cmd.Context(), t, wasm)
				if he := (*rpadmin.HTTPResponseError)(nil); errors.As(err, &he) {
					if he.Response.StatusCode == 400 {
						body, bodyErr := he.DecodeGenericErrorBody()
						if bodyErr == nil {
							out.Die("unable to deploy transform %s: %s", cfg.Name, body.Message)
						}
					}
				}
				out.MaybeDie(err, "unable to deploy transform %s: %v", cfg.Name, err)
			}

			fmt.Printf("transform %q deployed.\n", cfg.Name)
		},
	}
	cmd.Flags().StringVar(&file, "file", "", "The WebAssembly module to deploy")

	cmd.Flags().StringVarP(&fc.inputTopic, "input-topic", "i", "", "The input topic to apply the transform to")
	cmd.Flags().StringSliceVarP(&fc.outputTopics, "output-topic", "o", []string{}, "The output topic to write the transform results to (repeatable)")
	cmd.Flags().StringVar(&fc.functionName, "name", "", "The name of the transform")
	cmd.Flags().Var(&fc.env, "var", "Specify an environment variable in the form of KEY=VALUE")
	cmd.Flags().StringVar(&fc.compression, "compression", "", "Output batch compression type")
	cmd.Flags().StringVar(&fc.fromOffset, "from-offset", "", "Process an input topic partition from a relative offset; check help text for more information")
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
	outputTopics []string
	functionName string
	env          environment
	compression  string
	fromOffset   string
}

// ToProjectConfig creates a project.Config from the specified command line flags.
func (fc deployFlagConfig) ToProjectConfig() (out project.Config) {
	out.Name = fc.functionName
	out.InputTopic = fc.inputTopic
	out.OutputTopics = fc.outputTopics
	out.Env = fc.env.vars
	out.Compression = fc.compression
	out.FromOffset = fc.fromOffset
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
	if len(rhs.OutputTopics) > 0 {
		out.OutputTopics = rhs.OutputTopics
	}
	if rhs.Compression != "" {
		out.Compression = rhs.Compression
	}

	if rhs.FromOffset != "" {
		out.FromOffset = rhs.FromOffset
	}

	return out
}

// isEmptyProjectConfig checks if a project config is completely empty.
func isEmptyProjectConfig(cfg project.Config) bool {
	return cfg.Name == "" && cfg.InputTopic == "" && len(cfg.OutputTopics) == 0 && len(cfg.Env) == 0 && cfg.Compression == ""
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

// verifyWasm checks that a wasm file has the correct magic bytes.
func verifyWasm(binary []byte) error {
	// Check the file is a .wasm file (needs the magic \0asm prefix)
	if !bytes.HasPrefix(binary, []byte{0x00, 0x61, 0x73, 0x6d}) {
		return fmt.Errorf("invalid wasm binary")
	}
	return nil
}

// loadWasmFromDisk loads the wasm file and ensures the magic bytes are correct.
func loadWasmFromDisk(fs afero.Fs, path string) (io.Reader, error) {
	contents, err := afero.ReadFile(fs, path)
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("missing %q did you run `rpk transform build`", path)
	}
	if err != nil {
		return nil, fmt.Errorf("unable to read %q: %v", path, err)
	}
	return bytes.NewReader(contents), verifyWasm(contents)
}

// loadWasmFromDisk downloads the wasm file and ensures the magic bytes are correct.
func loadWasmFromNetwork(ctx context.Context, url string) (io.Reader, error) {
	client := httpapi.NewClient(httpapi.ReqTimeout(120 * time.Second))
	var contents []byte
	if err := client.Get(ctx, url, nil, &contents); err != nil {
		return nil, fmt.Errorf("unable to fetch wasm file: %v", err)
	}
	return bytes.NewReader(contents), verifyWasm(contents)
}

// mapToEnvVars converts a map to the adminapi environment variable type.
func mapToEnvVars(env map[string]string) (vars []rpadmin.EnvironmentVariable) {
	if env == nil {
		return
	}
	for k, v := range env {
		vars = append(vars, rpadmin.EnvironmentVariable{
			Key:   k,
			Value: v,
		})
	}
	return
}

// parseOffset converts a string formatted offset to the adminapi offset type.
func parseOffset(formattedOffset string) (*rpadmin.Offset, error) {
	if formattedOffset == "" {
		return nil, nil
	}
	var format string
	switch pfx := formattedOffset[0:1]; pfx {
	case "@":
		format = "timestamp"
	case "+":
		format = "from_start"
	case "-":
		format = "from_end"
	default:
		return nil, fmt.Errorf("bad prefix: expected one of ['@','+','-'], got: %q", pfx)
	}

	val, err := strconv.ParseInt(formattedOffset[1:], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("bad offset: parse error '%v'", err)
	}

	return &rpadmin.Offset{Format: format, Value: val}, nil
}

func adminAPIToDataplaneMetadata(m rpadmin.TransformMetadata) *dataplanev1alpha1.DeployTransformRequest {
	var envs []*dataplanev1alpha1.TransformMetadata_EnvironmentVariable
	for _, e := range m.Environment {
		envs = append(envs, &dataplanev1alpha1.TransformMetadata_EnvironmentVariable{
			Key:   e.Key,
			Value: e.Value,
		})
	}
	return &dataplanev1alpha1.DeployTransformRequest{
		Name:                 m.Name,
		InputTopicName:       m.InputTopic,
		OutputTopicNames:     m.OutputTopics,
		EnvironmentVariables: envs,
	}
}
