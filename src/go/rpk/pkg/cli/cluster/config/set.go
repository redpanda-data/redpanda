// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package config

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"golang.org/x/term"
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	controlplanev1 "buf.build/gen/go/redpandadata/cloud/protocolbuffers/go/redpanda/api/controlplane/v1"
	"connectrpc.com/connect"
	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/structpb"
	"gopkg.in/yaml.v3"
)

// anySlice represents a slice of any value type.
type anySlice []any

// A custom unmarshal is needed because go-yaml parse "YYYY-MM-DD" as a full
// timestamp, writing YYYY-MM-DD HH:MM:SS +0000 UTC when encoding, so we are
// going to treat timestamps as strings.
// See: https://github.com/go-yaml/yaml/issues/770

func (s *anySlice) UnmarshalYAML(n *yaml.Node) error {
	replaceTimestamp(n)

	var a []any
	err := n.Decode(&a)
	if err != nil {
		return err
	}
	*s = a
	return nil
}

func parseArgs(args []string) ([]string, error) {
	if len(args) == 2 && !strings.Contains(args[0], "=") {
		args = []string{args[0] + "=" + args[1]}
	}
	for _, arg := range args {
		if !strings.Contains(arg, "=") {
			return nil, fmt.Errorf("invalid arguments: %v, please use one of 'rpk cluster config set <key> <value>' or 'rpk cluster config set <key>=<value>', for empty values use 'rpk cluster config set <key>=\"\"' ", args)
		}
	}
	return args, nil
}

func newSetCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		noConfirm bool
		timeout   time.Duration
	)
	cmd := &cobra.Command{
		Use:   "set [KEY] [VALUE]",
		Short: "Set a single cluster configuration property",
		Long: `Set a single cluster configuration property.

This command is provided for use in scripts.  For interactive editing, or bulk
changes, use the 'edit' and 'import' commands respectively.

You may also use <key>=<value> notation for setting configuration properties:

  rpk cluster config set log_retention_ms=-1

If an empty string is given as the value, the property is reset to its default.
Use the flag '--no-confirm' to avoid the confirmation prompt.`,

		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			configs, err := parseArgs(args)
			out.MaybeDieErr(err)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			vp := cfg.VirtualProfile()

			if vp.CheckFromCloud() {
				if vp.CloudCluster.IsServerless() {
					out.Die("rpk cluster config set is not supported on Redpanda serverless clusters")
				}
				out.MaybeDie(err, "rpk unable to load config: %v", err)
				operation, err := setCloudConfig(cmd.Context(), cfg, vp, configs)
				out.MaybeDieErr(err)

				operationID := operation.GetOperation().GetId()
				fmt.Print("Processing configuration...")

				// Check if stdout is a terminal for progress indication
				isTerminal := term.IsTerminal(int(os.Stdout.Fd()))
				cloudClient := publicapi.NewCloudClientSet(cfg.DevOverrides().PublicAPIURL, vp.CurrentAuth().AuthToken)
				pollCtx, cancel := context.WithTimeout(cmd.Context(), timeout)
				defer cancel()
				finalOp, completedInTime, err := pollOperationStatusWithConfig(pollCtx, cloudClient, operationID, isTerminal, nil)
				out.MaybeDieErr(err)

				// Clear the progress line
				if isTerminal {
					fmt.Print("\r\033[K")
				} else {
					fmt.Println() // Add newline for non-terminal output
				}

				// Handle the result based on state
				state := finalOp.GetState()
				if completedInTime {
					switch state {
					case controlplanev1.Operation_STATE_COMPLETED:
						fmt.Printf("Configuration update completed successfully. Operation ID: %s\n", operationID)
					case controlplanev1.Operation_STATE_FAILED:
						fmt.Printf("Configuration update failed. Operation ID: %s\n", operationID)
					}
				} else {
					fmt.Printf("Configuration update is in progress and may take up to 10 minutes. We've waited for %s. To check the status, run 'rpk cluster config status' with the operation ID below.\n\n", timeout)
					fmt.Printf("Operation ID: %s\n", operationID)
				}
			} else {
				client, err := adminapi.NewClient(cmd.Context(), fs, vp)
				out.MaybeDie(err, "unable to initialize admin client: %v", err)

				schema, err := client.ClusterConfigSchema(cmd.Context())
				out.MaybeDie(err, "unable to query config schema: %v", err)

				upsert, remove, err := validateConfigSelfHosted(schema, configs, noConfirm)
				out.MaybeDieErr(err)

				result, err := client.PatchClusterConfig(cmd.Context(), upsert, remove)

				if he := (*rpadmin.HTTPResponseError)(nil); errors.As(err, &he) {
					// Special case 400 (validation) errors with friendly output
					// about which configuration properties were invalid.
					if he.Response.StatusCode == 400 {
						ve, err := formatValidationError(err, he)
						out.MaybeDie(err, "error setting config: %v", err)
						out.Die("no changes were made: %v", ve)
					}
				}

				out.MaybeDie(err, "error setting property: %v", err)
				fmt.Printf("Successfully updated configuration. New configuration version is %d.\n", result.ConfigVersion)

				status, err := client.ClusterConfigStatus(cmd.Context(), true)
				out.MaybeDie(err, "unable to check if the cluster needs to be restarted: %v; check the status with 'rpk cluster config status'", err)
				for _, value := range status {
					if value.Restart {
						fmt.Print("\nCluster needs to be restarted. See more details with 'rpk cluster config status'.\n")
						break
					}
				}
			}
		},
	}

	cmd.Flags().BoolVar(&noConfirm, "no-confirm", false, "Disable confirmation prompt")
	cmd.Flags().DurationVar(&timeout, "timeout", 10*time.Second, "Maximum time to poll for operation completion before displaying operation ID for manual status checking (e.g. 300ms, 1.5s, 30s)")
	return cmd
}

func validateConfigSelfHosted(schema rpadmin.ConfigSchema, args []string, noConfirm bool) (map[string]any, []string, error) {
	upsert := make(map[string]any)
	remove := make([]string, 0)

	for _, arg := range args {
		split := strings.SplitN(arg, "=", 2)
		key, value := split[0], split[1]

		// Disabling Tiered Storage requires a confirmation from the user because it may lead to data loss.
		if key == "cloud_storage_enable_remote_write" && value == "false" {
			if !noConfirm {
				confirmed, err := out.Confirm("Warning: disabling Tiered Storage may lead to data loss. If you only want to pause Tiered Storage temporarily, use the 'cloud_storage_enable_segment_uploads' option. Abort?")
				out.MaybeDie(err, "unable to read user input: %v", err)
				if confirmed {
					out.Die("aborted by user")
				}
			}
		}

		meta, ok := schema[key]

		if !ok {
			// loop over schema, try to find key in the Aliases,
			for _, v := range schema {
				if slices.Contains(v.Aliases, key) {
					meta, ok = v, true
					break
				}
			}
			if !ok {
				return nil, nil, fmt.Errorf("unknown property %q", key)
			}
		}

		// - For scalars, pass string values through to the REST
		// API -- it will give more informative errors than we can
		// about validation.  Special case strings for nullable
		// properties ('null') and for resetting to default ('')
		// - For arrays, make an effort: otherwise the REST API
		// may interpret a scalar string as a list of length 1
		// (via one_or_many_property).

		if meta.Nullable && value == "null" {
			// Nullable types may be explicitly set to null
			upsert[key] = nil
		} else if meta.Type != "string" && (value == "") {
			// Non-string types that receive an empty string
			// are reset to default
			remove = append(remove, key)
		} else if meta.Type == "array" {
			var a anySlice
			err := yaml.Unmarshal([]byte(value), &a)
			if err != nil {
				return nil, nil, fmt.Errorf("invalid list syntax: %v", err)
			}
			upsert[key] = a
		} else {
			upsert[key] = value
		}
	}

	return upsert, remove, nil
}

func setCloudConfig(ctx context.Context, cfg *config.Config, p *config.RpkProfile, configs []string) (*controlplanev1.UpdateClusterOperation, error) {
	cloudClient := publicapi.NewCloudClientSet(cfg.DevOverrides().PublicAPIURL, p.CurrentAuth().AuthToken)

	redpandaConfigs := make(map[string]any)
	paths := make([]string, 0)

	for _, c := range configs {
		split := strings.SplitN(c, "=", 2)
		key, value := split[0], split[1]
		redpandaConfigs[key] = value
		paths = append(paths, fmt.Sprintf("cluster_configuration.custom_properties.%s", key))
	}
	customerProperties, err := structpb.NewStruct(redpandaConfigs)
	if err != nil {
		return nil, fmt.Errorf("internal error while converting config to redpanda cloud configs: %v", err)
	}
	req := &controlplanev1.UpdateClusterRequest{
		Cluster: &controlplanev1.ClusterUpdate{
			Id: p.CloudCluster.ClusterID,
			ClusterConfiguration: &controlplanev1.ClusterUpdate_ClusterConfiguration{
				CustomProperties: customerProperties,
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: paths},
	}

	operation, err := cloudClient.Cluster.UpdateCluster(ctx, connect.NewRequest(req))
	if err != nil {
		var ce *connect.Error
		if errors.As(err, &ce) {
			if ce.Code() == connect.CodePermissionDenied {
				return nil, fmt.Errorf("this user does not have permission to update the cluster configuration, please check your role or contact admin")
			}
			if ce.Code() == connect.CodeNotFound {
				return nil, fmt.Errorf("cluster not found. Please ensure the cluster exists in the cloud")
			}
			if ce.Code() == connect.CodeInvalidArgument {
				var errs []string
				for _, detail := range ce.Details() {
					c, _ := detail.Value()
					switch d := c.(type) {
					case *errdetails.BadRequest:
						for _, violation := range d.FieldViolations {
							errs = append(
								errs,
								fmt.Sprintf("Field violation, description: %s\n", violation.Description),
							)
						}
					default:
						// do nothing
					}
				}
				if len(errs) > 0 {
					return nil, fmt.Errorf("invalid arguments: %s", strings.Join(errs, "\n"))
				}
			}
		}
		return nil, fmt.Errorf("internal error while updating redpanda cloud configs: %v", err)
	}
	return operation.Msg, nil
}

// pollConfig contains timing configuration for operation polling.
type pollConfig struct {
	initialDelay      time.Duration
	fastPollInterval  time.Duration
	slowPollInterval  time.Duration
	fastPollThreshold time.Duration
}

// pollOperationStatusWithConfig is the same as pollOperationStatus but allows overriding timing configuration.
// This is primarily useful for testing with shorter intervals.
func pollOperationStatusWithConfig(ctx context.Context, cloudClient *publicapi.CloudClientSet, operationID string, isTerminal bool, cfg *pollConfig) (*controlplanev1.Operation, bool, error) {
	// Use default production timing if no config provided
	if cfg == nil {
		cfg = &pollConfig{
			initialDelay:      2 * time.Second,
			fastPollInterval:  500 * time.Millisecond,
			slowPollInterval:  1 * time.Second,
			fastPollThreshold: 5 * time.Second,
		}
	}

	startTime := time.Now()
	initialDelay := cfg.initialDelay
	fastPollInterval := cfg.fastPollInterval
	slowPollInterval := cfg.slowPollInterval
	fastPollThreshold := cfg.fastPollThreshold

	// For exponential backoff in slow polling phase
	currentSlowInterval := slowPollInterval
	maxSlowInterval := 32 * time.Second

	// Cap initial delay at half the timeout to ensure time for at least one poll
	if deadline, ok := ctx.Deadline(); ok {
		timeRemaining := time.Until(deadline)
		maxInitialDelay := timeRemaining / 2
		if initialDelay > maxInitialDelay {
			initialDelay = maxInitialDelay
		}
	}

	// Wait for initial delay before first poll
	select {
	case <-time.After(initialDelay):
		// Print initial progress after initial delay
		if isTerminal {
			fmt.Printf("\rProcessing configuration... (%ds elapsed)", int(time.Since(startTime).Seconds()))
		}
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			// Timeout during initial delay - get final status
			return getFinalOperationStatus(ctx, cloudClient, operationID)
		}
		return nil, false, fmt.Errorf("context cancelled while waiting for initial delay: %w", ctx.Err())
	}

	for {
		resp, err := cloudClient.Operations.GetOperation(ctx, connect.NewRequest(&controlplanev1.GetOperationRequest{
			Id: operationID,
		}))
		if err != nil {
			// Check if timeout was reached
			if ctx.Err() == context.DeadlineExceeded {
				return getFinalOperationStatus(ctx, cloudClient, operationID)
			}
			return nil, false, fmt.Errorf("failed to get operation status: %v", err)
		}

		op := resp.Msg.Operation
		state := op.GetState()

		if state == controlplanev1.Operation_STATE_COMPLETED || state == controlplanev1.Operation_STATE_FAILED {
			return op, true, nil
		}

		// Print progress with elapsed time
		elapsed := time.Since(startTime)
		if isTerminal {
			fmt.Printf("\rProcessing configuration... (%ds elapsed)", int(elapsed.Seconds()))
		}

		// Adaptive polling with exponential backoff:
		// - Fast polling (500ms) for first 5 seconds
		// - Exponential backoff after 5 seconds: 1s, 2s, 4s, 8s, 16s, 32s (max)
		var pollInterval time.Duration
		if elapsed < fastPollThreshold {
			pollInterval = fastPollInterval
		} else {
			pollInterval = currentSlowInterval
			nextInterval := min(currentSlowInterval*2, maxSlowInterval)
			currentSlowInterval = nextInterval
		}

		// Wait for next poll or context cancellation
		select {
		case <-time.After(pollInterval):
			// Continue to next iteration
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				return getFinalOperationStatus(ctx, cloudClient, operationID)
			}
			return nil, false, fmt.Errorf("context cancelled while polling operation status: %w", ctx.Err())
		}
	}
}

// getFinalOperationStatus retrieves the operation status one final time after timeout.
// Uses context.Background() to ensure this call isn't affected by the timeout.
// The ctx parameter is accepted to satisfy linters but is intentionally not used.
func getFinalOperationStatus(ctx context.Context, cloudClient *publicapi.CloudClientSet, operationID string) (*controlplanev1.Operation, bool, error) {
	_ = ctx // Explicitly mark as intentionally unused to satisfy linters
	// Use context.Background() to ensure this call succeeds even though the original context timed out
	//nolint:contextcheck // Intentionally using context.Background() since ctx has already timed out
	resp, err := cloudClient.Operations.GetOperation(context.Background(), connect.NewRequest(&controlplanev1.GetOperationRequest{
		Id: operationID,
	}))
	if err != nil {
		return nil, false, fmt.Errorf("failed to get final operation status: %v", err)
	}
	return resp.Msg.Operation, false, nil
}
