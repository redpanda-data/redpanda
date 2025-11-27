// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package shadow

import (
	"fmt"
	"strings"
	"time"

	adminv2 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/admin/v2"
	corecommonv1 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/common/v1"
	dataplanev1alpha3 "buf.build/gen/go/redpandadata/dataplane/protocolbuffers/go/redpanda/api/dataplane/v1alpha3"
	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newDescribeCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var opts slDescribeOptions
	cmd := &cobra.Command{
		Use:   "describe [LINK_NAME]",
		Args:  cobra.ExactArgs(1),
		Short: "Describe a Redpanda Shadow Link",
		Long: `Describe a Redpanda Shadow Link.

This command shows the Shadow Link configuration, including connection settings,
synchronization options, and filters. Use the flags to display specific sections
or all sections of the configuration.

By default, the command displays the overview and client configuration sections.
Use the flags to display additional sections such as topic synchronization,
consumer offset synchronization, and security synchronization settings.
`,
		Example: `
Describe a Shadow Link with default sections (overview and client):
  rpk shadow describe my-shadow-link

Display all configuration sections:
  rpk shadow describe my-shadow-link --print-all

Display specific sections:
  rpk shadow describe my-shadow-link --print-overview --print-topic

Display only the client configuration:
  rpk shadow describe my-shadow-link -c
`,
		Run: func(cmd *cobra.Command, args []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "unable to load rpk config: %v", err)
			config.CheckExitCloudAdmin(p)

			opts.defaultOrAll()

			cl, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			linkName := args[0]
			link, err := cl.ShadowLinkService().GetShadowLink(cmd.Context(), connect.NewRequest(&adminv2.GetShadowLinkRequest{
				Name: linkName,
			}))
			out.MaybeDie(err, "unable to get Redpanda Shadow Link %q: %v", linkName, handleConnectError(err, "get", linkName))

			printShadowLinkDescription(link.Msg.GetShadowLink(), opts)
		},
	}
	cmd.Flags().BoolVarP(&opts.overview, "print-overview", "o", false, "Print the overview section")
	cmd.Flags().BoolVarP(&opts.client, "print-client", "c", false, "Print the client configuration section")
	cmd.Flags().BoolVarP(&opts.topic, "print-topic", "t", false, "Print the detailed topic configuration section")
	cmd.Flags().BoolVarP(&opts.co, "print-consumer", "r", false, "Print the detailed consumer offset configuration section")
	cmd.Flags().BoolVarP(&opts.sec, "print-security", "s", false, "Print the detailed security configuration section")
	cmd.Flags().BoolVarP(&opts.all, "print-all", "a", false, "Print all sections")
	return cmd
}

type slDescribeOptions struct {
	all      bool
	overview bool
	client   bool
	topic    bool
	co       bool // consumer offset
	sec      bool // security
}

// If no flags are set, default to overview and client sections.
func (o *slDescribeOptions) defaultOrAll() {
	if !o.all && !o.overview && !o.client && !o.topic && !o.co && !o.sec {
		o.overview, o.client = true, true
	}

	if o.all {
		o.overview, o.client, o.topic, o.co, o.sec = true, true, true, true, true
	}
}

func printShadowLinkDescription(link *adminv2.ShadowLink, opts slDescribeOptions) {
	const (
		secOverview       = "Overview"
		secClient         = "Client"
		secTopicSync      = "Topic Sync"
		secConsumerOffset = "Consumer Offset Sync"
		secSecurity       = "Security Sync"
	)

	sections := out.NewSections(
		out.ConditionalSectionHeaders(map[string]bool{
			secOverview:       opts.overview,
			secClient:         opts.client,
			secTopicSync:      opts.topic,
			secConsumerOffset: opts.co,
			secSecurity:       opts.sec,
		})...,
	)

	cfg := link.GetConfigurations()

	sections.Add(secOverview, func() {
		printOverview(link)
	})

	sections.Add(secClient, func() {
		printClient(cfg.GetClientOptions())
	})

	sections.Add(secTopicSync, func() {
		printTopicSync(cfg.GetTopicMetadataSyncOptions())
	})

	sections.Add(secConsumerOffset, func() {
		printConsumerOffsetSync(cfg.GetConsumerOffsetSyncOptions())
	})

	sections.Add(secSecurity, func() {
		printSecuritySync(cfg.GetSecuritySyncOptions())
	})
}

func printOverview(link *adminv2.ShadowLink) {
	tw := out.NewTabWriter()
	defer tw.Flush()
	tw.Print("NAME", link.GetName())
	tw.Print("UID", link.GetUid())
	if status := link.GetStatus(); status != nil {
		tw.Print("STATE", strings.TrimPrefix(status.GetState().String(), "SHADOW_LINK_STATE_"))
	}
}

func printCloudOverview(link *dataplanev1alpha3.ShadowLink) {
	tw := out.NewTabWriter()
	defer tw.Flush()
	tw.Print("NAME", link.GetName())
	tw.Print("UID", link.GetUid())
	tw.Print("STATE", strings.TrimPrefix(link.GetState().String(), "SHADOW_LINK_STATE_"))
}

func printClient(opts *adminv2.ShadowLinkClientOptions) {
	tw := out.NewTabWriter()
	defer tw.Flush()
	if opts == nil {
		tw.Print("No client configuration")
		return
	}

	// This is a full section with 2 columns, we have to print each row with
	// two arguments to ensure proper alignment.

	tw.Print("CLIENT ID", opts.GetClientId())
	tw.Print("SOURCE CLUSTER ID", opts.GetSourceClusterId())
	tw.Print("BOOTSTRAP SERVERS:", "")
	for _, server := range opts.GetBootstrapServers() {
		tw.Print("", fmt.Sprintf("- %s", server))
	}

	// TLS section
	if tls := opts.GetTlsSettings(); tls != nil {
		tw.Print("TLS:", "")
		tw.Print("----", "")
		// TLS settings can be either file-based or PEM-based.
		if fileSettings := tls.GetTlsFileSettings(); fileSettings != nil {
			// CA is required, key and cert are optional.
			tw.Print("CA", fileSettings.GetCaPath())
			if keyPath := fileSettings.GetKeyPath(); keyPath != "" {
				tw.Print("KEY", keyPath)
			}
			if certPath := fileSettings.GetCertPath(); certPath != "" {
				tw.Print("CERT", certPath)
			}
		} else if pemSettings := tls.GetTlsPemSettings(); pemSettings != nil {
			tw.Print("CA", pemSettings.GetCa())
			if key := pemSettings.GetKeyFingerprint(); key != "" {
				tw.Print("KEY FINGERPRINT", key)
			}
			if cert := pemSettings.GetCert(); cert != "" {
				tw.Print("CERT", cert)
			}
		}
	}

	// SASL section
	if auth := opts.GetAuthenticationConfiguration(); auth != nil {
		if scram := auth.GetScramConfiguration(); scram != nil {
			tw.Print("", "")
			tw.Print("SASL:", "")
			tw.Print("-----", "")
			tw.Print("USERNAME", scram.GetUsername())
			tw.Print("MECHANISM", formatScramMechanism(scram.GetScramMechanism()))
			if scram.GetPasswordSet() {
				tw.Print("PASSWORD SET AT", scram.GetPasswordSetAt().AsTime().Format(time.RFC3339))
			}
		}
	}

	tw.Print("", "")
	tw.Print("CLIENT CONFIGURATION:", "")
	tw.Print(strings.Repeat("-", 21), "")

	// Print client config table
	tw.Print("metadata_max_age_ms", opts.GetEffectiveMetadataMaxAgeMs())
	tw.Print("connection_timeout_ms", opts.GetEffectiveConnectionTimeoutMs())
	tw.Print("retry_backoff_ms", opts.GetEffectiveRetryBackoffMs())
	tw.Print("fetch_wait_max_ms", opts.GetEffectiveFetchWaitMaxMs())
	tw.Print("fetch_min_bytes", opts.GetEffectiveFetchMinBytes())
	tw.Print("fetch_max_bytes", opts.GetEffectiveFetchMaxBytes())
	tw.Print("fetch_partition_max_bytes", opts.GetEffectiveFetchPartitionMaxBytes())
}

func printTopicSync(opts *adminv2.TopicMetadataSyncOptions) {
	tw := out.NewTabWriter()
	defer tw.Flush()
	if opts == nil {
		tw.Print("No topic sync configuration")
		return
	}

	tw.Print("INTERVAL", opts.GetEffectiveInterval().AsDuration().String())
	tw.Print("PAUSED", opts.GetPaused())
	if opts.HasStartOffset() {
		var startOffset string
		if opts.GetStartAtEarliest() != nil {
			startOffset = "EARLIEST"
		}
		if opts.GetStartAtLatest() != nil {
			startOffset = "LATEST"
		}
		if opts.GetStartAtTimestamp() != nil {
			startOffset = opts.GetStartAtTimestamp().AsTime().String()
		}
		tw.Print("START OFFSET", startOffset)
	}
	if len(opts.GetAutoCreateShadowTopicFilters()) > 0 {
		tw.Print("FILTERS:", "")
		for _, filter := range opts.GetAutoCreateShadowTopicFilters() {
			tw.Print("", fmt.Sprintf("- %s %s %q", formatFilterType(filter.GetFilterType()), formatPatternType(filter.GetPatternType()), filter.GetName()))
		}
	}

	if props := opts.GetSyncedShadowTopicProperties(); len(props) > 0 {
		tw.Print("PROPERTIES:", "")
		for _, prop := range props {
			tw.Print("", fmt.Sprintf("- %s", prop))
		}
	}
}

func printConsumerOffsetSync(opts *adminv2.ConsumerOffsetSyncOptions) {
	tw := out.NewTabWriter()
	defer tw.Flush()
	if opts == nil {
		tw.Print("No consumer offset sync configuration")
		return
	}

	tw.Print("PAUSED", opts.GetPaused())
	tw.Print("INTERVAL", opts.GetEffectiveInterval().AsDuration().String())

	if len(opts.GetGroupFilters()) > 0 {
		tw.Print("GROUP FILTERS:", "")
		for _, filter := range opts.GetGroupFilters() {
			tw.Print("", fmt.Sprintf("- %s %s %q", formatFilterType(filter.GetFilterType()), formatPatternType(filter.GetPatternType()), filter.GetName()))
		}
	}
}

func printSecuritySync(opts *adminv2.SecuritySettingsSyncOptions) {
	tw := out.NewTabWriter()
	defer tw.Flush()
	if opts == nil {
		tw.Print("No security sync configuration")
		return
	}

	tw.Print("PAUSED", opts.GetPaused())
	tw.Print("INTERVAL", opts.GetEffectiveInterval().AsDuration().String())

	if len(opts.GetAclFilters()) > 0 {
		tw.Print("ACL FILTERS:")
		tw.Flush()
		aclTw := out.NewTable("", "RESOURCE", "PATTERN", "NAME", "OPERATION", "PERMISSION")
		defer aclTw.Flush()
		for _, filter := range opts.GetAclFilters() {
			resource := filter.GetResourceFilter()
			access := filter.GetAccessFilter()
			aclTw.Print(
				"",
				formatACLResource(resource.GetResourceType()),
				formatACLPattern(resource.GetPatternType()),
				resource.GetName(),
				formatACLOperation(access.GetOperation()),
				formatACLPermissionType(access.GetPermissionType()),
			)
		}
	}
}

func formatScramMechanism(m adminv2.ScramMechanism) string {
	switch m {
	case adminv2.ScramMechanism_SCRAM_MECHANISM_SCRAM_SHA_256:
		return "SCRAM-SHA-256"
	case adminv2.ScramMechanism_SCRAM_MECHANISM_SCRAM_SHA_512:
		return "SCRAM-SHA-512"
	default:
		return "UNSPECIFIED"
	}
}

func formatFilterType(ft adminv2.FilterType) string {
	switch ft {
	case adminv2.FilterType_FILTER_TYPE_INCLUDE:
		return "include"
	case adminv2.FilterType_FILTER_TYPE_EXCLUDE:
		return "exclude"
	default:
		return "unspecified"
	}
}

func formatPatternType(pt adminv2.PatternType) string {
	switch pt {
	case adminv2.PatternType_PATTERN_TYPE_LITERAL:
		return "literal"
	case adminv2.PatternType_PATTERN_TYPE_PREFIX:
		return "prefix"
	default:
		return "unspecified"
	}
}

func formatACLResource(r corecommonv1.ACLResource) string {
	return strings.ToUpper(strings.TrimPrefix(r.String(), "ACL_RESOURCE_"))
}

func formatACLPattern(p corecommonv1.ACLPattern) string {
	return strings.ToUpper(strings.TrimPrefix(p.String(), "ACL_PATTERN_"))
}

func formatACLOperation(o corecommonv1.ACLOperation) string {
	return strings.ToUpper(strings.TrimPrefix(o.String(), "ACL_OPERATION_"))
}

func formatACLPermissionType(p corecommonv1.ACLPermissionType) string {
	return strings.ToUpper(strings.TrimPrefix(p.String(), "ACL_PERMISSION_TYPE_"))
}
