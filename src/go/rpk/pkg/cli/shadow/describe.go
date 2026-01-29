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

	controlplanev1 "buf.build/gen/go/redpandadata/cloud/protocolbuffers/go/redpanda/api/controlplane/v1"
	adminv2 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/admin/v2"
	corecommonv1 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/common/v1"
	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth/providers/auth0"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

// Section header constants for describe output.
const (
	secOverview       = "Overview"
	secClient         = "Client"
	secTopicSync      = "Topic Sync"
	secConsumerOffset = "Consumer Offset Sync"
	secSecurity       = "Security Sync"
	secSchemaRegistry = "Schema Registry Sync"
)

// shadowLinkDescription is the unified output for both cloud and self-hosted.
type shadowLinkDescription struct {
	// Overview fields (flattened to top level)
	Name             string `json:"name" yaml:"name"`
	ID               string `json:"id" yaml:"id"`
	State            string `json:"state,omitempty" yaml:"state,omitempty"`
	Reason           string `json:"reason,omitempty" yaml:"reason,omitempty"`
	ShadowRedpandaID string `json:"shadow_redpanda_id,omitempty" yaml:"shadow_redpanda_id,omitempty"`
	CreatedAt        string `json:"created_at,omitempty" yaml:"created_at,omitempty"`
	UpdatedAt        string `json:"updated_at,omitempty" yaml:"updated_at,omitempty"`

	// Configuration sections
	ClientOptions             *describeClientOptions             `json:"client_options,omitempty" yaml:"client_options,omitempty"`
	TopicMetadataSyncOptions  *describeTopicMetadataSyncOptions  `json:"topic_metadata_sync_options,omitempty" yaml:"topic_metadata_sync_options,omitempty"`
	ConsumerOffsetSyncOptions *describeConsumerOffsetSyncOptions `json:"consumer_offset_sync_options,omitempty" yaml:"consumer_offset_sync_options,omitempty"`
	SecuritySyncOptions       *describeSecuritySyncOptions       `json:"security_sync_options,omitempty" yaml:"security_sync_options,omitempty"`
	SchemaRegistrySyncOptions *describeSchemaRegistrySyncOptions `json:"schema_registry_sync_options,omitempty" yaml:"schema_registry_sync_options,omitempty"`
}

// describeClientOptions uses effective values and auth metadata (not password).
type describeClientOptions struct {
	ClientID               string                        `json:"client_id,omitempty" yaml:"client_id,omitempty"`
	SourceClusterID        string                        `json:"source_cluster_id,omitempty" yaml:"source_cluster_id,omitempty"`
	BootstrapServers       []string                      `json:"bootstrap_servers" yaml:"bootstrap_servers"`
	TLSSettings            *TLSSettings                  `json:"tls_settings,omitempty" yaml:"tls_settings,omitempty"`
	AuthenticationConfig   *describeAuthenticationConfig `json:"authentication_configuration,omitempty" yaml:"authentication_configuration,omitempty"`
	MetadataMaxAgeMs       int32                         `json:"metadata_max_age_ms" yaml:"metadata_max_age_ms"`
	ConnectionTimeoutMs    int32                         `json:"connection_timeout_ms" yaml:"connection_timeout_ms"`
	RetryBackoffMs         int32                         `json:"retry_backoff_ms" yaml:"retry_backoff_ms"`
	FetchWaitMaxMs         int32                         `json:"fetch_wait_max_ms" yaml:"fetch_wait_max_ms"`
	FetchMinBytes          int32                         `json:"fetch_min_bytes" yaml:"fetch_min_bytes"`
	FetchMaxBytes          int32                         `json:"fetch_max_bytes" yaml:"fetch_max_bytes"`
	FetchPartitionMaxBytes int32                         `json:"fetch_partition_max_bytes" yaml:"fetch_partition_max_bytes"`
}

// describeAuthenticationConfig shows metadata, not the actual password.
type describeAuthenticationConfig struct {
	Username      string `json:"username,omitempty" yaml:"username,omitempty"`
	Mechanism     string `json:"mechanism,omitempty" yaml:"mechanism,omitempty"`
	PasswordSet   bool   `json:"password_set,omitempty" yaml:"password_set,omitempty"`
	PasswordSetAt string `json:"password_set_at,omitempty" yaml:"password_set_at,omitempty"`
}

// describeTopicMetadataSyncOptions uses string intervals (not time.Duration).
type describeTopicMetadataSyncOptions struct {
	Interval                     string        `json:"interval" yaml:"interval"`
	Paused                       bool          `json:"paused" yaml:"paused"`
	StartOffset                  string        `json:"start_offset,omitempty" yaml:"start_offset,omitempty"`
	AutoCreateShadowTopicFilters []*NameFilter `json:"auto_create_shadow_topic_filters,omitempty" yaml:"auto_create_shadow_topic_filters,omitempty"`
	SyncedShadowTopicProperties  []string      `json:"synced_shadow_topic_properties,omitempty" yaml:"synced_shadow_topic_properties,omitempty"`
}

type describeConsumerOffsetSyncOptions struct {
	Interval     string        `json:"interval" yaml:"interval"`
	Paused       bool          `json:"paused" yaml:"paused"`
	GroupFilters []*NameFilter `json:"group_filters,omitempty" yaml:"group_filters,omitempty"`
}

type describeSecuritySyncOptions struct {
	Interval   string       `json:"interval" yaml:"interval"`
	Paused     bool         `json:"paused" yaml:"paused"`
	ACLFilters []*ACLFilter `json:"acl_filters,omitempty" yaml:"acl_filters,omitempty"`
}

type describeSchemaRegistrySyncOptions struct {
	ShadowingMode string `json:"shadowing_mode" yaml:"shadowing_mode"`
}

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

For Redpanda Cloud, rpk will use the Redpanda ID of the cluster you are 
currently logged into. If you wish to use a different one either login and 
create a profile for it, or use the --redpanda-id flag to specify it directly.

Using the --format flag with JSON or YAML will output the full configuration in 
the specified format, ignoring section flags.
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

Display output as JSON:
  rpk shadow describe my-shadow-link --format json
`,
		Run: func(cmd *cobra.Command, args []string) {
			f := p.Formatter
			if h, ok := f.Help(shadowLinkDescription{}); ok {
				out.Exit(h)
			}

			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load rpk config: %v", err)
			prof := cfg.VirtualProfile()
			config.CheckExitServerlessAdmin(prof)

			opts.defaultOrAll()

			linkName := args[0]

			if prof.CheckFromCloud() {
				cloudClient, err := publicapi.NewValidatedCloudClientSet(
					cfg.DevOverrides().PublicAPIURL,
					prof.CurrentAuth().AuthToken,
					auth0.NewClient(cfg.DevOverrides()).Audience(),
					[]string{prof.CurrentAuth().ClientID},
				)
				out.MaybeDieErr(err)

				link, err := cloudClient.ShadowLinkByNameAndRPID(cmd.Context(), linkName, prof.CloudCluster.ClusterID)
				out.MaybeDie(err, "unable to find Shadow Link %q", linkName)

				printCloudShadowLinkDescription(f, link, opts)
				return
			}
			cl, err := adminapi.NewClient(cmd.Context(), fs, prof)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)
			link, err := cl.ShadowLinkService().GetShadowLink(cmd.Context(), connect.NewRequest(&adminv2.GetShadowLinkRequest{
				Name: linkName,
			}))
			out.MaybeDie(err, "unable to get Redpanda Shadow Link %q: %v", linkName, handleConnectError(err, "get", linkName))

			printShadowLinkDescription(f, link.Msg.GetShadowLink(), opts)
		},
	}
	cmd.Flags().BoolVarP(&opts.overview, "print-overview", "o", false, "Print the overview section")
	cmd.Flags().BoolVarP(&opts.client, "print-client", "c", false, "Print the client configuration section")
	cmd.Flags().BoolVarP(&opts.topic, "print-topic", "t", false, "Print the detailed topic configuration section")
	cmd.Flags().BoolVarP(&opts.co, "print-consumer", "r", false, "Print the detailed consumer offset configuration section")
	cmd.Flags().BoolVarP(&opts.sec, "print-security", "s", false, "Print the detailed security configuration section")
	cmd.Flags().BoolVarP(&opts.sr, "print-registry", "y", false, "Print the detailed schema registry configuration section")
	cmd.Flags().BoolVarP(&opts.all, "print-all", "a", false, "Print all sections")
	p.InstallFormatFlag(cmd)
	return cmd
}

type slDescribeOptions struct {
	all      bool
	overview bool
	client   bool
	topic    bool
	co       bool // consumer offset
	sec      bool // security
	sr       bool // schema registry
}

// If no flags are set, default to overview and client sections.
func (o *slDescribeOptions) defaultOrAll() {
	if !o.all && !o.overview && !o.client && !o.topic && !o.co && !o.sec && !o.sr {
		o.overview, o.client = true, true
	}

	if o.all {
		o.overview, o.client, o.topic, o.co, o.sec, o.sr = true, true, true, true, true, true
	}
}

func printShadowLinkDescription(f config.OutFormatter, link *adminv2.ShadowLink, opts slDescribeOptions) {
	if isText, _, s, err := f.Format(fromAdminV2ShadowLinkDescription(link)); !isText {
		out.MaybeDie(err, "unable to print in the required format %q: %v", f.Kind, err)
		fmt.Println(s)
		return
	}

	sections := out.NewSections(
		out.ConditionalSectionHeaders(map[string]bool{
			secOverview:       opts.overview,
			secClient:         opts.client,
			secTopicSync:      opts.topic,
			secConsumerOffset: opts.co,
			secSecurity:       opts.sec,
			secSchemaRegistry: opts.sr,
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

	sections.Add(secSchemaRegistry, func() {
		printSchemaRegistrySync(cfg.GetSchemaRegistrySyncOptions())
	})
}

func printCloudShadowLinkDescription(f config.OutFormatter, link *controlplanev1.ShadowLink, opts slDescribeOptions) {
	if isText, _, s, err := f.Format(fromCloudShadowLinkDescription(link)); !isText {
		out.MaybeDie(err, "unable to print in the required format %q: %v", f.Kind, err)
		fmt.Println(s)
		return
	}

	sections := out.NewSections(
		out.ConditionalSectionHeaders(map[string]bool{
			secOverview:       opts.overview,
			secClient:         opts.client,
			secTopicSync:      opts.topic,
			secConsumerOffset: opts.co,
			secSecurity:       opts.sec,
		})...,
	)

	sections.Add(secOverview, func() {
		printCloudOverview(link)
	})

	sections.Add(secClient, func() {
		printCloudClient(link.GetClientOptions())
	})

	sections.Add(secTopicSync, func() {
		printTopicSync(link.GetTopicMetadataSyncOptions())
	})

	sections.Add(secConsumerOffset, func() {
		printConsumerOffsetSync(link.GetConsumerOffsetSyncOptions())
	})

	sections.Add(secSecurity, func() {
		printSecuritySync(link.GetSecuritySyncOptions())
	})

	sections.Add(secSchemaRegistry, func() {
		printSchemaRegistrySync(link.GetSchemaRegistrySyncOptions())
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

func printCloudOverview(link *controlplanev1.ShadowLink) {
	tw := out.NewTabWriter()
	defer tw.Flush()
	tw.Print("NAME", link.GetName())
	tw.Print("ID", link.GetId())
	tw.Print("STATE", strings.TrimPrefix(link.GetState().String(), "STATE_"))
	if link.GetReason() != "" {
		tw.Print("REASON", link.GetReason())
	}
	tw.Print("SHADOW REDPANDA ID", link.GetShadowRedpandaId())
	if createdAt := link.GetCreatedAt(); createdAt != nil {
		tw.Print("CREATED AT", createdAt.AsTime().Format(time.RFC3339))
	}
	if updatedAt := link.GetUpdatedAt(); updatedAt != nil {
		tw.Print("UPDATED AT", updatedAt.AsTime().Format(time.RFC3339))
	}
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

func printCloudClient(opts *controlplanev1.ShadowLinkClientOptions) {
	tw := out.NewTabWriter()
	defer tw.Flush()
	if opts == nil {
		tw.Print("No client configuration")
		return
	}

	if opts.GetClientId() != "" {
		tw.Print("CLIENT ID", opts.GetClientId())
	}
	if opts.GetSourceClusterId() != "" {
		tw.Print("SOURCE CLUSTER ID", opts.GetSourceClusterId())
	}
	tw.Print("BOOTSTRAP SERVERS:", "")
	for _, server := range opts.GetBootstrapServers() {
		tw.Print("", fmt.Sprintf("- %s", server))
	}

	// TLS section - Cloud only supports PEM content
	if tls := opts.GetTlsSettings(); tls != nil {
		tw.Print("TLS:", "")
		tw.Print("----", "")
		tw.Print("ENABLED", tls.GetEnabled())
		if ca := tls.GetCa(); ca != "" {
			tw.Print("CA", ca)
		}
		if key := tls.GetKey(); key != "" {
			tw.Print("KEY", key)
		}
		if cert := tls.GetCert(); cert != "" {
			tw.Print("CERT", cert)
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

func printSchemaRegistrySync(opts *adminv2.SchemaRegistrySyncOptions) {
	tw := out.NewTabWriter()
	defer tw.Flush()
	if opts == nil {
		tw.Print("No schema registry sync configuration")
		return
	}
	tw.Print("SHADOWING MODE", strings.ReplaceAll(opts.WhichSchemaRegistryShadowingMode().String(), "_", " "))
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

// fromAdminV2ShadowLinkDescription converts adminv2.ShadowLink to shadowLinkDescription.
func fromAdminV2ShadowLinkDescription(link *adminv2.ShadowLink) shadowLinkDescription {
	cfg := link.GetConfigurations()
	var state string
	if status := link.GetStatus(); status != nil {
		state = strings.TrimPrefix(status.GetState().String(), "SHADOW_LINK_STATE_")
	}
	return shadowLinkDescription{
		Name:                      link.GetName(),
		ID:                        link.GetUid(),
		State:                     state,
		ClientOptions:             buildDescribeClientOptions(cfg.GetClientOptions()),
		TopicMetadataSyncOptions:  buildDescribeTopicSyncOptions(cfg.GetTopicMetadataSyncOptions()),
		ConsumerOffsetSyncOptions: buildDescribeConsumerOffsetOptions(cfg.GetConsumerOffsetSyncOptions()),
		SecuritySyncOptions:       buildDescribeSecurityOptions(cfg.GetSecuritySyncOptions()),
		SchemaRegistrySyncOptions: buildDescribeSchemaRegistryOptions(cfg.GetSchemaRegistrySyncOptions()),
	}
}

// fromCloudShadowLinkDescription converts controlplanev1.ShadowLink to shadowLinkDescription.
func fromCloudShadowLinkDescription(link *controlplanev1.ShadowLink) shadowLinkDescription {
	var createdAt, updatedAt string
	if t := link.GetCreatedAt(); t != nil {
		createdAt = t.AsTime().Format(time.RFC3339)
	}
	if t := link.GetUpdatedAt(); t != nil {
		updatedAt = t.AsTime().Format(time.RFC3339)
	}
	return shadowLinkDescription{
		Name:                      link.GetName(),
		ID:                        link.GetId(),
		State:                     strings.TrimPrefix(link.GetState().String(), "STATE_"),
		Reason:                    link.GetReason(),
		ShadowRedpandaID:          link.GetShadowRedpandaId(),
		CreatedAt:                 createdAt,
		UpdatedAt:                 updatedAt,
		ClientOptions:             buildDescribeCloudClientOptions(link.GetClientOptions()),
		TopicMetadataSyncOptions:  buildDescribeTopicSyncOptions(link.GetTopicMetadataSyncOptions()),
		ConsumerOffsetSyncOptions: buildDescribeConsumerOffsetOptions(link.GetConsumerOffsetSyncOptions()),
		SecuritySyncOptions:       buildDescribeSecurityOptions(link.GetSecuritySyncOptions()),
		SchemaRegistrySyncOptions: buildDescribeSchemaRegistryOptions(link.GetSchemaRegistrySyncOptions()),
	}
}

func buildDescribeClientOptions(opts *adminv2.ShadowLinkClientOptions) *describeClientOptions {
	if opts == nil {
		return nil
	}
	return &describeClientOptions{
		ClientID:               opts.GetClientId(),
		SourceClusterID:        opts.GetSourceClusterId(),
		BootstrapServers:       opts.GetBootstrapServers(),
		TLSSettings:            adminTLSToCfg(opts.GetTlsSettings()),
		AuthenticationConfig:   buildDescribeAuthConfig(opts.GetAuthenticationConfiguration()),
		MetadataMaxAgeMs:       opts.GetEffectiveMetadataMaxAgeMs(),
		ConnectionTimeoutMs:    opts.GetEffectiveConnectionTimeoutMs(),
		RetryBackoffMs:         opts.GetEffectiveRetryBackoffMs(),
		FetchWaitMaxMs:         opts.GetEffectiveFetchWaitMaxMs(),
		FetchMinBytes:          opts.GetEffectiveFetchMinBytes(),
		FetchMaxBytes:          opts.GetEffectiveFetchMaxBytes(),
		FetchPartitionMaxBytes: opts.GetEffectiveFetchPartitionMaxBytes(),
	}
}

func buildDescribeCloudClientOptions(opts *controlplanev1.ShadowLinkClientOptions) *describeClientOptions {
	if opts == nil {
		return nil
	}
	return &describeClientOptions{
		ClientID:               opts.GetClientId(),
		SourceClusterID:        opts.GetSourceClusterId(),
		BootstrapServers:       opts.GetBootstrapServers(),
		TLSSettings:            cloudTLSToCfg(opts.GetTlsSettings()),
		AuthenticationConfig:   buildDescribeAuthConfig(opts.GetAuthenticationConfiguration()),
		MetadataMaxAgeMs:       opts.GetEffectiveMetadataMaxAgeMs(),
		ConnectionTimeoutMs:    opts.GetEffectiveConnectionTimeoutMs(),
		RetryBackoffMs:         opts.GetEffectiveRetryBackoffMs(),
		FetchWaitMaxMs:         opts.GetEffectiveFetchWaitMaxMs(),
		FetchMinBytes:          opts.GetEffectiveFetchMinBytes(),
		FetchMaxBytes:          opts.GetEffectiveFetchMaxBytes(),
		FetchPartitionMaxBytes: opts.GetEffectiveFetchPartitionMaxBytes(),
	}
}

func buildDescribeAuthConfig(auth *adminv2.AuthenticationConfiguration) *describeAuthenticationConfig {
	if auth == nil {
		return nil
	}
	if scram := auth.GetScramConfiguration(); scram != nil {
		var passwordSetAt string
		if scram.GetPasswordSet() && scram.GetPasswordSetAt() != nil {
			passwordSetAt = scram.GetPasswordSetAt().AsTime().Format(time.RFC3339)
		}
		return &describeAuthenticationConfig{
			Username:      scram.GetUsername(),
			Mechanism:     formatScramMechanism(scram.GetScramMechanism()),
			PasswordSet:   scram.GetPasswordSet(),
			PasswordSetAt: passwordSetAt,
		}
	}
	if plain := auth.GetPlainConfiguration(); plain != nil {
		var passwordSetAt string
		if plain.GetPasswordSet() && plain.GetPasswordSetAt() != nil {
			passwordSetAt = plain.GetPasswordSetAt().AsTime().Format(time.RFC3339)
		}
		return &describeAuthenticationConfig{
			Username:      plain.GetUsername(),
			Mechanism:     "PLAIN",
			PasswordSet:   plain.GetPasswordSet(),
			PasswordSetAt: passwordSetAt,
		}
	}
	return nil
}

func buildDescribeTopicSyncOptions(opts *adminv2.TopicMetadataSyncOptions) *describeTopicMetadataSyncOptions {
	if opts == nil {
		return nil
	}
	var startOffset string
	if opts.HasStartOffset() {
		if opts.GetStartAtEarliest() != nil {
			startOffset = "EARLIEST"
		} else if opts.GetStartAtLatest() != nil {
			startOffset = "LATEST"
		} else if ts := opts.GetStartAtTimestamp(); ts != nil {
			startOffset = ts.AsTime().Format(time.RFC3339)
		}
	}
	var filters []*NameFilter
	for _, f := range opts.GetAutoCreateShadowTopicFilters() {
		filters = append(filters, adminMapFilterToCfg(f))
	}
	return &describeTopicMetadataSyncOptions{
		Interval:                     opts.GetEffectiveInterval().AsDuration().String(),
		Paused:                       opts.GetPaused(),
		StartOffset:                  startOffset,
		AutoCreateShadowTopicFilters: filters,
		SyncedShadowTopicProperties:  opts.GetSyncedShadowTopicProperties(),
	}
}

func buildDescribeConsumerOffsetOptions(opts *adminv2.ConsumerOffsetSyncOptions) *describeConsumerOffsetSyncOptions {
	if opts == nil {
		return nil
	}
	var filters []*NameFilter
	for _, f := range opts.GetGroupFilters() {
		filters = append(filters, adminMapFilterToCfg(f))
	}
	return &describeConsumerOffsetSyncOptions{
		Interval:     opts.GetEffectiveInterval().AsDuration().String(),
		Paused:       opts.GetPaused(),
		GroupFilters: filters,
	}
}

func buildDescribeSecurityOptions(opts *adminv2.SecuritySettingsSyncOptions) *describeSecuritySyncOptions {
	if opts == nil {
		return nil
	}
	var filters []*ACLFilter
	for _, f := range opts.GetAclFilters() {
		filters = append(filters, adminACLFilterToCfg(f))
	}
	return &describeSecuritySyncOptions{
		Interval:   opts.GetEffectiveInterval().AsDuration().String(),
		Paused:     opts.GetPaused(),
		ACLFilters: filters,
	}
}

func buildDescribeSchemaRegistryOptions(opts *adminv2.SchemaRegistrySyncOptions) *describeSchemaRegistrySyncOptions {
	if opts == nil {
		return nil
	}
	return &describeSchemaRegistrySyncOptions{
		ShadowingMode: strings.ReplaceAll(opts.WhichSchemaRegistryShadowingMode().String(), "_", " "),
	}
}
