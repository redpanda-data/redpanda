// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package topic

import (
	"context"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/zap"
)

func newAlterConfigCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		sets      []string // key=val
		deletions []string // key only
		appends   []string // key=val
		subtracts []string // key=val

		dry bool
	)

	cmd := &cobra.Command{
		Use:   "alter-config [TOPICS...] --set key=value --delete key2,key3",
		Short: `Set, delete, add, and remove key/value configs for a topic`,
		Long: `Set, delete, add, and remove key/value configs for a topic.

This command allows you to incrementally alter the configuration for multiple
topics at a time.

Incremental altering supports four operations:

  1) Setting a key=value pair
  2) Deleting a key's value
  3) Appending a new value to a list-of-values key
  4) Subtracting (removing) an existing value from a list-of-values key

The --dry option will validate whether the requested configuration change is
valid, but does not apply it.
`,
		Args: cobra.MinimumNArgs(1),
		Run: func(_ *cobra.Command, topics []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			cl, err := kafka.NewFranzClient(fs, p)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer cl.Close()

			if len(topics) == 0 {
				out.Exit("No topics specified.")
			}

			// Sets, appends, and subtracts are key=value pairs;
			// deletions are just keys.
			setKVs, err := parseKVs(sets)
			out.MaybeDie(err, "unable to parse --set: %v", err)
			appendKVs, err := parseKVs(appends)
			out.MaybeDie(err, "unable to parse --append: %v", err)
			subtractKVs, err := parseKVs(subtracts)
			out.MaybeDie(err, "unable to parse --subtract: %v", err)

			// Redpanda fails to set both remote.read and write when passed
			// at the same time, so we issue first the set request for write,
			// then the rest of the requests.
			// See https://github.com/redpanda-data/redpanda/issues/9191
			// TODO: Remove this once v24.2 is EOL.
			// See https://github.com/redpanda-data/redpanda/pull/23545.
			_, isRRR := setKVs["redpanda.remote.read"]
			wv, isRRW := setKVs["redpanda.remote.write"]
			rrwErrors := make(map[string]int16)
			if isRRR && isRRW {
				req := kmsg.NewPtrIncrementalAlterConfigsRequest()
				req.ValidateOnly = dry
				cfg := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
				cfg.Name = "redpanda.remote.write"
				cfg.Op = kmsg.IncrementalAlterConfigOpSet
				cfg.Value = kmsg.StringPtr(wv)

				for _, topic := range topics {
					reqTopic := kmsg.NewIncrementalAlterConfigsRequestResource()
					reqTopic.ResourceType = kmsg.ConfigResourceTypeTopic
					reqTopic.ResourceName = topic
					reqTopic.Configs = []kmsg.IncrementalAlterConfigsRequestResourceConfig{cfg}
					req.Resources = append(req.Resources, reqTopic)
				}
				resp, err := req.RequestWith(context.Background(), cl)
				out.MaybeDie(err, "unable to incrementally update configs: %v", err)
				for _, resource := range resp.Resources {
					rrwErrors[resource.ResourceName] = resource.ErrorCode
				}

				delete(setKVs, "redpanda.remote.write")
			}

			req := kmsg.NewPtrIncrementalAlterConfigsRequest()
			req.ValidateOnly = dry

			var configs []kmsg.IncrementalAlterConfigsRequestResourceConfig
			for _, pair := range []struct {
				kvs map[string]string
				op  kmsg.IncrementalAlterConfigOp
			}{
				{setKVs, kmsg.IncrementalAlterConfigOpSet},           // 0 == set
				{appendKVs, kmsg.IncrementalAlterConfigOpAppend},     // 2 == append
				{subtractKVs, kmsg.IncrementalAlterConfigOpSubtract}, // 3 == subtract
			} {
				for k, v := range pair.kvs {
					config := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
					config.Name = k
					config.Op = pair.op
					config.Value = kmsg.StringPtr(v)
					configs = append(configs, config)
				}
			}
			for _, del := range deletions {
				config := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
				config.Name = del
				config.Op = kmsg.IncrementalAlterConfigOpDelete // 1 == delete
				configs = append(configs, config)
			}

			if len(configs) == 0 {
				out.Exit("No incremental configuration changes were requested!")
			}

			for _, topic := range topics {
				reqTopic := kmsg.NewIncrementalAlterConfigsRequestResource()
				reqTopic.ResourceType = kmsg.ConfigResourceTypeTopic
				reqTopic.ResourceName = topic
				reqTopic.Configs = configs
				req.Resources = append(req.Resources, reqTopic)
			}

			resp, err := req.RequestWith(context.Background(), cl)
			out.MaybeDie(err, "unable to incrementally update configs: %v", err)

			tw := out.NewTable("TOPIC", "STATUS")
			defer tw.Flush()

			for _, resource := range resp.Resources {
				msg := "OK"
				if rrwErrorCode := rrwErrors[resource.ResourceName]; resource.ErrorCode != 0 || rrwErrorCode != 0 {
					if err := kerr.TypedErrorForCode(resource.ErrorCode); err != nil {
						msg = err.Message
					} else if err := kerr.TypedErrorForCode(rrwErrorCode); err != nil {
						msg = err.Message
					}
					if resource.ErrorMessage != nil {
						zap.L().Sugar().Debugf("redpanda returned error message: %v", *resource.ErrorMessage)
						msg += ": " + *resource.ErrorMessage
					}
				}
				tw.Print(resource.ResourceName, msg)
			}
		},
	}

	cmd.Flags().StringArrayVarP(&sets, "set", "s", nil, "key=value; Pair to set (repeatable)")
	cmd.Flags().StringArrayVarP(&deletions, "delete", "d", nil, "Key to delete (repeatable)")
	cmd.Flags().StringArrayVar(&appends, "append", nil, "key=value; Value to append to a list-of-values key (repeatable)")
	cmd.Flags().StringArrayVar(&subtracts, "subtract", nil, "key=value; Value to remove from list-of-values key (repeatable)")

	cmd.Flags().BoolVar(&dry, "dry", false, "Dry run: validate the alter request, but do not apply")

	return cmd
}
