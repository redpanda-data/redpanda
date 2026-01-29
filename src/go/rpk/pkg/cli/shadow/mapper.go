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
	controlplanev1 "buf.build/gen/go/redpandadata/cloud/protocolbuffers/go/redpanda/api/controlplane/v1"
	adminv2 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/admin/v2"
	corecommonv1 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/common/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func shadowLinkConfigToProto(slCfg *ShadowLinkConfig) *adminv2.ShadowLink {
	if slCfg == nil {
		return nil
	}

	shadowLink := &adminv2.ShadowLink{
		Name: slCfg.Name,
		// uid and status are output-only fields, not included in create request
	}

	shadowLink.Configurations = &adminv2.ShadowLinkConfigurations{
		ClientOptions:             mapClientOptions(slCfg.ClientOptions),
		TopicMetadataSyncOptions:  mapTopicMetadataSyncOptions(slCfg.TopicMetadataSyncOptions),
		ConsumerOffsetSyncOptions: mapConsumerOffsetSyncOptions(slCfg.ConsumerOffsetSyncOptions),
		SecuritySyncOptions:       mapSecuritySyncOptions(slCfg.SecuritySyncOptions),
		SchemaRegistrySyncOptions: mapSchemaRegistrySyncOptions(slCfg.SchemaRegistrySyncOptions),
	}
	return shadowLink
}

func shadowLinkConfigToCloudCreate(slCfg *ShadowLinkConfig) *controlplanev1.ShadowLinkCreate {
	if slCfg == nil {
		return nil
	}
	cloudSl := &controlplanev1.ShadowLinkCreate{
		Name: slCfg.Name,
	}
	if slc := slCfg.CloudOptions; slc != nil {
		cloudSl.ShadowRedpandaId = slc.ShadowRedpandaID
		cloudSl.SourceRedpandaId = slc.SourceRedpandaID
	}

	cloudSl.ClientOptions = mapCloudClientOptions(slCfg.ClientOptions)
	cloudSl.TopicMetadataSyncOptions = mapTopicMetadataSyncOptions(slCfg.TopicMetadataSyncOptions)
	cloudSl.ConsumerOffsetSyncOptions = mapConsumerOffsetSyncOptions(slCfg.ConsumerOffsetSyncOptions)
	cloudSl.SecuritySyncOptions = mapSecuritySyncOptions(slCfg.SecuritySyncOptions)
	cloudSl.SchemaRegistrySyncOptions = mapSchemaRegistrySyncOptions(slCfg.SchemaRegistrySyncOptions)
	return cloudSl
}

func mapClientOptions(opts *ShadowLinkClientOptions) *adminv2.ShadowLinkClientOptions {
	if opts == nil {
		return nil
	}

	pbOpts := &adminv2.ShadowLinkClientOptions{
		BootstrapServers:       opts.BootstrapServers,
		SourceClusterId:        opts.SourceClusterID,
		MetadataMaxAgeMs:       opts.MetadataMaxAgeMs,
		ConnectionTimeoutMs:    opts.ConnectionTimeoutMs,
		RetryBackoffMs:         opts.RetryBackoffMs,
		FetchWaitMaxMs:         opts.FetchWaitMaxMs,
		FetchMinBytes:          opts.FetchMinBytes,
		FetchMaxBytes:          opts.FetchMaxBytes,
		FetchPartitionMaxBytes: opts.FetchPartitionMaxBytes,
		// client_id is output-only in the protobuf, so we don't set it.
	}

	if opts.TLSSettings != nil {
		pbOpts.TlsSettings = mapTLSSettings(opts.TLSSettings)
	}

	if opts.AuthenticationConfiguration != nil {
		pbOpts.AuthenticationConfiguration = mapAuthenticationConfiguration(opts.AuthenticationConfiguration)
	}

	return pbOpts
}

func mapCloudClientOptions(opts *ShadowLinkClientOptions) *controlplanev1.ShadowLinkClientOptions {
	if opts == nil {
		return nil
	}

	pbOpts := &controlplanev1.ShadowLinkClientOptions{
		BootstrapServers:       opts.BootstrapServers,
		SourceClusterId:        opts.SourceClusterID,
		MetadataMaxAgeMs:       opts.MetadataMaxAgeMs,
		ConnectionTimeoutMs:    opts.ConnectionTimeoutMs,
		RetryBackoffMs:         opts.RetryBackoffMs,
		FetchWaitMaxMs:         opts.FetchWaitMaxMs,
		FetchMinBytes:          opts.FetchMinBytes,
		FetchMaxBytes:          opts.FetchMaxBytes,
		FetchPartitionMaxBytes: opts.FetchPartitionMaxBytes,
		// ClientId is intentionally left empty; It's output only.
	}

	if opts.TLSSettings != nil {
		pbOpts.TlsSettings = mapCloudTLSSettings(opts.TLSSettings)
	}

	if opts.AuthenticationConfiguration != nil {
		pbOpts.AuthenticationConfiguration = mapAuthenticationConfiguration(opts.AuthenticationConfiguration)
	}

	return pbOpts
}

func mapCloudTLSSettings(tls *TLSSettings) *controlplanev1.TLSSettings {
	if tls == nil {
		return nil
	}

	cloudTLS := &controlplanev1.TLSSettings{
		Enabled:             tls.Enabled,
		DoNotSetSniHostname: tls.DoNotSetSniHostname,
	}

	// Cloud proto only supports inline PEM content, not file paths
	if tls.TLSPEMSettings != nil {
		cloudTLS.Ca = tls.TLSPEMSettings.CA
		cloudTLS.Key = tls.TLSPEMSettings.Key
		cloudTLS.Cert = tls.TLSPEMSettings.Cert
	}
	// Note: TLSFileSettings cannot be mapped directly to cloud proto
	// since the cloud API requires inline PEM content, not file paths.
	// If only TLSFileSettings is present (and TLSPEMSettings is nil),
	// certificate information will be omitted from the returned proto.
	// The caller is responsible for providing inline PEM content.
	return cloudTLS
}

func mapTLSSettings(tls *TLSSettings) *corecommonv1.TLSSettings {
	if tls == nil {
		return nil
	}

	pbTLS := &corecommonv1.TLSSettings{
		Enabled:             tls.Enabled,
		DoNotSetSniHostname: tls.DoNotSetSniHostname,
	}
	switch {
	case tls.TLSFileSettings != nil:
		t := tls.TLSFileSettings
		pbTLS.TlsSettings = &corecommonv1.TLSSettings_TlsFileSettings{
			TlsFileSettings: &corecommonv1.TLSFileSettings{
				CaPath:   t.CAPath,
				KeyPath:  t.KeyPath,
				CertPath: t.CertPath,
			},
		}
	case tls.TLSPEMSettings != nil:
		t := tls.TLSPEMSettings
		pbTLS.TlsSettings = &corecommonv1.TLSSettings_TlsPemSettings{
			TlsPemSettings: &corecommonv1.TLSPEMSettings{
				Ca:   t.CA,
				Key:  t.Key,
				Cert: t.Cert,
				// key_fingerprint is output-only
			},
		}
	}

	return pbTLS
}

func mapAuthenticationConfiguration(auth *AuthenticationConfiguration) *adminv2.AuthenticationConfiguration {
	if auth == nil {
		return nil
	}

	pbAuth := &adminv2.AuthenticationConfiguration{}

	switch {
	case auth.ScramConfiguration != nil:
		a := auth.ScramConfiguration
		pbAuth.Authentication = &adminv2.AuthenticationConfiguration_ScramConfiguration{
			ScramConfiguration: &adminv2.ScramConfig{
				Username:       a.Username,
				Password:       a.Password,
				ScramMechanism: mapScramMechanism(a.ScramMechanism),
				// password_set and password_set_at are output-only
			},
		}
	case auth.PlainConfiguration != nil:
		a := auth.PlainConfiguration
		pbAuth.Authentication = &adminv2.AuthenticationConfiguration_PlainConfiguration{
			PlainConfiguration: &adminv2.PlainConfig{
				Username: a.Username,
				Password: a.Password,
				// password_set and password_set_at are output-only
			},
		}
	}

	return pbAuth
}

func mapScramMechanism(m ScramMechanism) adminv2.ScramMechanism {
	switch m {
	case ScramMechanismScramSha256:
		return adminv2.ScramMechanism_SCRAM_MECHANISM_SCRAM_SHA_256
	case ScramMechanismScramSha512:
		return adminv2.ScramMechanism_SCRAM_MECHANISM_SCRAM_SHA_512
	default:
		return adminv2.ScramMechanism_SCRAM_MECHANISM_UNSPECIFIED
	}
}

func mapTopicMetadataSyncOptions(opts *TopicMetadataSyncOptions) *adminv2.TopicMetadataSyncOptions {
	if opts == nil {
		return nil
	}

	pbOpts := &adminv2.TopicMetadataSyncOptions{
		SyncedShadowTopicProperties: opts.SyncedShadowTopicProperties,
		ExcludeDefault:              opts.ExcludeDefault,
		Paused:                      opts.Paused,
	}

	if opts.Interval > 0 {
		pbOpts.Interval = durationpb.New(opts.Interval)
	}

	for _, filter := range opts.AutoCreateShadowTopicFilters {
		pbOpts.AutoCreateShadowTopicFilters = append(pbOpts.AutoCreateShadowTopicFilters, mapNameFilter(filter))
	}

	// Handle start_offset oneof - only one can be set
	if opts.StartAtEarliest != nil {
		pbOpts.StartOffset = &adminv2.TopicMetadataSyncOptions_StartAtEarliest{
			StartAtEarliest: &adminv2.TopicMetadataSyncOptions_EarliestOffset{},
		}
	} else if opts.StartAtLatest != nil {
		pbOpts.StartOffset = &adminv2.TopicMetadataSyncOptions_StartAtLatest{
			StartAtLatest: &adminv2.TopicMetadataSyncOptions_LatestOffset{},
		}
	} else if opts.StartAtTimestamp != nil {
		pbOpts.StartOffset = &adminv2.TopicMetadataSyncOptions_StartAtTimestamp{
			StartAtTimestamp: timestamppb.New(*opts.StartAtTimestamp),
		}
	}

	return pbOpts
}

func mapConsumerOffsetSyncOptions(opts *ConsumerOffsetSyncOptions) *adminv2.ConsumerOffsetSyncOptions {
	if opts == nil {
		return nil
	}

	pbOpts := &adminv2.ConsumerOffsetSyncOptions{
		Paused: opts.Paused,
	}

	if opts.Interval > 0 {
		pbOpts.Interval = durationpb.New(opts.Interval)
	}

	for _, filter := range opts.GroupFilters {
		pbOpts.GroupFilters = append(pbOpts.GroupFilters, mapNameFilter(filter))
	}

	return pbOpts
}

func mapSecuritySyncOptions(opts *SecuritySettingsSyncOptions) *adminv2.SecuritySettingsSyncOptions {
	if opts == nil {
		return nil
	}

	pbOpts := &adminv2.SecuritySettingsSyncOptions{
		Paused: opts.Paused,
	}

	if opts.Interval > 0 {
		pbOpts.Interval = durationpb.New(opts.Interval)
	}

	for _, filter := range opts.ACLFilters {
		pbOpts.AclFilters = append(pbOpts.AclFilters, mapACLFilter(filter))
	}

	return pbOpts
}

func mapSchemaRegistrySyncOptions(opts *SchemaRegistrySyncOptions) *adminv2.SchemaRegistrySyncOptions {
	if opts == nil {
		return nil
	}

	pbOpts := &adminv2.SchemaRegistrySyncOptions{}

	// Handle schema_registry_shadowing_mode oneof
	// If the struct has the ShadowSchemaRegistryTopic field populated,
	// we set the oneof
	if opts.ShadowSchemaRegistryTopic != nil {
		pbOpts.SchemaRegistryShadowingMode = &adminv2.SchemaRegistrySyncOptions_ShadowSchemaRegistryTopic_{
			ShadowSchemaRegistryTopic: &adminv2.SchemaRegistrySyncOptions_ShadowSchemaRegistryTopic{},
		}
	}

	return pbOpts
}

func mapNameFilter(filter *NameFilter) *adminv2.NameFilter {
	if filter == nil {
		return nil
	}

	return &adminv2.NameFilter{
		PatternType: mapPatternType(filter.PatternType),
		FilterType:  mapFilterType(filter.FilterType),
		Name:        filter.Name,
	}
}

func mapACLFilter(filter *ACLFilter) *adminv2.ACLFilter {
	if filter == nil {
		return nil
	}

	return &adminv2.ACLFilter{
		ResourceFilter: mapACLResourceFilter(filter.ResourceFilter),
		AccessFilter:   mapACLAccessFilter(filter.AccessFilter),
	}
}

func mapACLResourceFilter(filter *ACLResourceFilter) *adminv2.ACLResourceFilter {
	if filter == nil {
		return nil
	}

	return &adminv2.ACLResourceFilter{
		ResourceType: mapACLResource(filter.ResourceType),
		PatternType:  mapACLPattern(filter.PatternType),
		Name:         filter.Name,
	}
}

func mapACLAccessFilter(filter *ACLAccessFilter) *adminv2.ACLAccessFilter {
	if filter == nil {
		return nil
	}

	return &adminv2.ACLAccessFilter{
		Principal:      filter.Principal,
		Operation:      mapACLOperation(filter.Operation),
		PermissionType: mapACLPermissionType(filter.PermissionType),
		Host:           filter.Host,
	}
}

func mapPatternType(pt PatternType) adminv2.PatternType {
	switch pt {
	case PatternTypeLiteral:
		return adminv2.PatternType_PATTERN_TYPE_LITERAL
	case PatternTypePrefix:
		return adminv2.PatternType_PATTERN_TYPE_PREFIX
	default:
		return adminv2.PatternType_PATTERN_TYPE_UNSPECIFIED
	}
}

func mapFilterType(ft FilterType) adminv2.FilterType {
	switch ft {
	case FilterTypeInclude:
		return adminv2.FilterType_FILTER_TYPE_INCLUDE
	case FilterTypeExclude:
		return adminv2.FilterType_FILTER_TYPE_EXCLUDE
	default:
		return adminv2.FilterType_FILTER_TYPE_UNSPECIFIED
	}
}

func mapACLResource(resource ACLResource) corecommonv1.ACLResource {
	switch resource {
	case ACLResourceAny:
		return corecommonv1.ACLResource_ACL_RESOURCE_ANY
	case ACLResourceCluster:
		return corecommonv1.ACLResource_ACL_RESOURCE_CLUSTER
	case ACLResourceGroup:
		return corecommonv1.ACLResource_ACL_RESOURCE_GROUP
	case ACLResourceTopic:
		return corecommonv1.ACLResource_ACL_RESOURCE_TOPIC
	case ACLResourceTXNID:
		return corecommonv1.ACLResource_ACL_RESOURCE_TXN_ID
	case ACLResourceSRSubject:
		return corecommonv1.ACLResource_ACL_RESOURCE_SR_SUBJECT
	case ACLResourceSRRegistry:
		return corecommonv1.ACLResource_ACL_RESOURCE_SR_REGISTRY
	case ACLResourceSRAny:
		return corecommonv1.ACLResource_ACL_RESOURCE_SR_ANY
	default:
		return corecommonv1.ACLResource_ACL_RESOURCE_UNSPECIFIED
	}
}

func mapACLPattern(pattern ACLPattern) corecommonv1.ACLPattern {
	switch pattern {
	case ACLPatternAny:
		return corecommonv1.ACLPattern_ACL_PATTERN_ANY
	case ACLPatternLiteral:
		return corecommonv1.ACLPattern_ACL_PATTERN_LITERAL
	case ACLPatternPrefixed:
		return corecommonv1.ACLPattern_ACL_PATTERN_PREFIXED
	case ACLPatternMatch:
		return corecommonv1.ACLPattern_ACL_PATTERN_MATCH
	default:
		return corecommonv1.ACLPattern_ACL_PATTERN_UNSPECIFIED
	}
}

func mapACLOperation(operation ACLOperation) corecommonv1.ACLOperation {
	switch operation {
	case ACLOperationAny:
		return corecommonv1.ACLOperation_ACL_OPERATION_ANY
	case ACLOperationRead:
		return corecommonv1.ACLOperation_ACL_OPERATION_READ
	case ACLOperationWrite:
		return corecommonv1.ACLOperation_ACL_OPERATION_WRITE
	case ACLOperationCreate:
		return corecommonv1.ACLOperation_ACL_OPERATION_CREATE
	case ACLOperationRemove:
		return corecommonv1.ACLOperation_ACL_OPERATION_REMOVE
	case ACLOperationAlter:
		return corecommonv1.ACLOperation_ACL_OPERATION_ALTER
	case ACLOperationDescribe:
		return corecommonv1.ACLOperation_ACL_OPERATION_DESCRIBE
	case ACLOperationClusterAction:
		return corecommonv1.ACLOperation_ACL_OPERATION_CLUSTER_ACTION
	case ACLOperationDescribeConfigs:
		return corecommonv1.ACLOperation_ACL_OPERATION_DESCRIBE_CONFIGS
	case ACLOperationAlterConfigs:
		return corecommonv1.ACLOperation_ACL_OPERATION_ALTER_CONFIGS
	case ACLOperationIdempotentWrite:
		return corecommonv1.ACLOperation_ACL_OPERATION_IDEMPOTENT_WRITE
	default:
		return corecommonv1.ACLOperation_ACL_OPERATION_UNSPECIFIED
	}
}

func mapACLPermissionType(permType ACLPermissionType) corecommonv1.ACLPermissionType {
	switch permType {
	case ACLPermissionTypeAny:
		return corecommonv1.ACLPermissionType_ACL_PERMISSION_TYPE_ANY
	case ACLPermissionTypeAllow:
		return corecommonv1.ACLPermissionType_ACL_PERMISSION_TYPE_ALLOW
	case ACLPermissionTypeDeny:
		return corecommonv1.ACLPermissionType_ACL_PERMISSION_TYPE_DENY
	default:
		return corecommonv1.ACLPermissionType_ACL_PERMISSION_TYPE_UNSPECIFIED
	}
}

func shadowLinkToConfig(sl *adminv2.ShadowLink) *ShadowLinkConfig {
	if sl == nil {
		return nil
	}

	cfg := &ShadowLinkConfig{
		Name: sl.Name,
		// uid and status are output-only fields, not included in config
	}

	if sl.GetConfigurations() != nil {
		cfg.ClientOptions = adminClientOptsToCfg(sl.GetConfigurations().GetClientOptions())
		cfg.TopicMetadataSyncOptions = adminTopicMetadataSyncToCfg(sl.GetConfigurations().GetTopicMetadataSyncOptions())
		cfg.ConsumerOffsetSyncOptions = adminConsumerOffsetSyncToCfg(sl.GetConfigurations().GetConsumerOffsetSyncOptions())
		cfg.SecuritySyncOptions = adminSecuritySyncToCfg(sl.GetConfigurations().GetSecuritySyncOptions())
		cfg.SchemaRegistrySyncOptions = adminSchemaRegistrySyncToCfg(sl.GetConfigurations().GetSchemaRegistrySyncOptions())
	}

	return cfg
}

func adminClientOptsToCfg(opts *adminv2.ShadowLinkClientOptions) *ShadowLinkClientOptions {
	if opts == nil {
		return nil
	}

	cfg := &ShadowLinkClientOptions{
		BootstrapServers:       opts.GetBootstrapServers(),
		SourceClusterID:        opts.GetSourceClusterId(),
		MetadataMaxAgeMs:       opts.GetMetadataMaxAgeMs(),
		ConnectionTimeoutMs:    opts.GetConnectionTimeoutMs(),
		RetryBackoffMs:         opts.GetRetryBackoffMs(),
		FetchWaitMaxMs:         opts.GetFetchWaitMaxMs(),
		FetchMinBytes:          opts.GetFetchMinBytes(),
		FetchMaxBytes:          opts.GetFetchMaxBytes(),
		FetchPartitionMaxBytes: opts.GetFetchPartitionMaxBytes(),
	}

	if opts.GetTlsSettings() != nil {
		cfg.TLSSettings = adminTLSToCfg(opts.GetTlsSettings())
	}

	if opts.GetAuthenticationConfiguration() != nil {
		cfg.AuthenticationConfiguration = adminAuthToCfg(opts.GetAuthenticationConfiguration())
	}

	return cfg
}

func adminTLSToCfg(tls *corecommonv1.TLSSettings) *TLSSettings {
	if tls == nil {
		return nil
	}
	tlsSettings := &TLSSettings{
		Enabled:             tls.GetEnabled(),
		DoNotSetSniHostname: tls.GetDoNotSetSniHostname(),
	}

	switch t := tls.GetTlsSettings().(type) {
	case *corecommonv1.TLSSettings_TlsFileSettings:
		if t.TlsFileSettings == nil {
			return tlsSettings
		}
		tlsSettings.TLSFileSettings = &TLSFileSettings{
			CAPath:   t.TlsFileSettings.GetCaPath(),
			KeyPath:  t.TlsFileSettings.GetKeyPath(),
			CertPath: t.TlsFileSettings.GetCertPath(),
		}
	case *corecommonv1.TLSSettings_TlsPemSettings:
		if t.TlsPemSettings == nil {
			return tlsSettings
		}
		tlsSettings.TLSPEMSettings = &TLSPEMSettings{
			CA:   t.TlsPemSettings.GetCa(),
			Key:  t.TlsPemSettings.GetKey(),
			Cert: t.TlsPemSettings.GetCert(),
		}
	}

	return tlsSettings
}

func adminAuthToCfg(auth *adminv2.AuthenticationConfiguration) *AuthenticationConfiguration {
	if auth == nil {
		return nil
	}

	switch a := auth.GetAuthentication().(type) {
	case *adminv2.AuthenticationConfiguration_ScramConfiguration:
		if a.ScramConfiguration == nil {
			return nil
		}
		return &AuthenticationConfiguration{
			ScramConfiguration: &ScramConfiguration{
				Username:       a.ScramConfiguration.GetUsername(),
				Password:       a.ScramConfiguration.GetPassword(),
				ScramMechanism: adminScramMechanismToCfg(a.ScramConfiguration.GetScramMechanism()),
			},
		}
	case *adminv2.AuthenticationConfiguration_PlainConfiguration:
		if a.PlainConfiguration == nil {
			return nil
		}
		return &AuthenticationConfiguration{
			PlainConfiguration: &PlainConfiguration{
				Username: a.PlainConfiguration.GetUsername(),
				Password: a.PlainConfiguration.GetPassword(),
			},
		}
	}

	return nil
}

func adminScramMechanismToCfg(m adminv2.ScramMechanism) ScramMechanism {
	switch m {
	case adminv2.ScramMechanism_SCRAM_MECHANISM_SCRAM_SHA_256:
		return ScramMechanismScramSha256
	case adminv2.ScramMechanism_SCRAM_MECHANISM_SCRAM_SHA_512:
		return ScramMechanismScramSha512
	default:
		return ""
	}
}

func adminTopicMetadataSyncToCfg(opts *adminv2.TopicMetadataSyncOptions) *TopicMetadataSyncOptions {
	if opts == nil {
		return nil
	}

	cfg := &TopicMetadataSyncOptions{
		SyncedShadowTopicProperties: opts.GetSyncedShadowTopicProperties(),
		ExcludeDefault:              opts.GetExcludeDefault(),
		Paused:                      opts.GetPaused(),
	}

	if opts.GetInterval() != nil {
		cfg.Interval = opts.GetInterval().AsDuration()
	}

	for _, filter := range opts.GetAutoCreateShadowTopicFilters() {
		cfg.AutoCreateShadowTopicFilters = append(cfg.AutoCreateShadowTopicFilters, adminMapFilterToCfg(filter))
	}

	// Handle start_offset oneof
	switch startOffset := opts.GetStartOffset().(type) {
	case *adminv2.TopicMetadataSyncOptions_StartAtEarliest:
		cfg.StartAtEarliest = &StartAtEarliest{}
	case *adminv2.TopicMetadataSyncOptions_StartAtLatest:
		cfg.StartAtLatest = &StartAtLatest{}
	case *adminv2.TopicMetadataSyncOptions_StartAtTimestamp:
		if startOffset.StartAtTimestamp != nil {
			t := startOffset.StartAtTimestamp.AsTime()
			cfg.StartAtTimestamp = &t
		}
	}

	return cfg
}

func adminConsumerOffsetSyncToCfg(opts *adminv2.ConsumerOffsetSyncOptions) *ConsumerOffsetSyncOptions {
	if opts == nil {
		return nil
	}

	cfg := &ConsumerOffsetSyncOptions{
		Paused: opts.GetPaused(),
	}

	if opts.GetInterval() != nil {
		cfg.Interval = opts.GetInterval().AsDuration()
	}

	for _, filter := range opts.GetGroupFilters() {
		cfg.GroupFilters = append(cfg.GroupFilters, adminMapFilterToCfg(filter))
	}

	return cfg
}

func adminSecuritySyncToCfg(opts *adminv2.SecuritySettingsSyncOptions) *SecuritySettingsSyncOptions {
	if opts == nil {
		return nil
	}

	cfg := &SecuritySettingsSyncOptions{
		Paused: opts.GetPaused(),
	}

	if opts.GetInterval() != nil {
		cfg.Interval = opts.GetInterval().AsDuration()
	}

	for _, filter := range opts.GetAclFilters() {
		cfg.ACLFilters = append(cfg.ACLFilters, adminACLFilterToCfg(filter))
	}

	return cfg
}

func adminSchemaRegistrySyncToCfg(opts *adminv2.SchemaRegistrySyncOptions) *SchemaRegistrySyncOptions {
	if opts == nil {
		return nil
	}

	cfg := &SchemaRegistrySyncOptions{}

	// Handle schema_registry_shadowing_mode oneof
	if _, ok := opts.GetSchemaRegistryShadowingMode().(*adminv2.SchemaRegistrySyncOptions_ShadowSchemaRegistryTopic_); ok {
		cfg.ShadowSchemaRegistryTopic = &ShadowSchemaRegistryTopic{}
	}

	return cfg
}

func adminMapFilterToCfg(filter *adminv2.NameFilter) *NameFilter {
	if filter == nil {
		return nil
	}

	return &NameFilter{
		PatternType: adminPatternTypeToCfg(filter.GetPatternType()),
		FilterType:  adminFilterTypeToCfg(filter.GetFilterType()),
		Name:        filter.GetName(),
	}
}

func adminACLFilterToCfg(filter *adminv2.ACLFilter) *ACLFilter {
	if filter == nil {
		return nil
	}

	return &ACLFilter{
		ResourceFilter: adminACLResourceFilterToCfg(filter.GetResourceFilter()),
		AccessFilter:   adminACLAccessFilterToCfg(filter.GetAccessFilter()),
	}
}

func adminACLResourceFilterToCfg(filter *adminv2.ACLResourceFilter) *ACLResourceFilter {
	if filter == nil {
		return nil
	}

	return &ACLResourceFilter{
		ResourceType: adminACLResourceToCfg(filter.GetResourceType()),
		PatternType:  adminACLPatternToCfg(filter.GetPatternType()),
		Name:         filter.GetName(),
	}
}

func adminACLAccessFilterToCfg(filter *adminv2.ACLAccessFilter) *ACLAccessFilter {
	if filter == nil {
		return nil
	}

	return &ACLAccessFilter{
		Principal:      filter.GetPrincipal(),
		Operation:      adminACLOperationToCfg(filter.GetOperation()),
		PermissionType: adminPermissionTypeToCfg(filter.GetPermissionType()),
		Host:           filter.GetHost(),
	}
}

func adminPatternTypeToCfg(pt adminv2.PatternType) PatternType {
	switch pt {
	case adminv2.PatternType_PATTERN_TYPE_LITERAL:
		return PatternTypeLiteral
	case adminv2.PatternType_PATTERN_TYPE_PREFIX:
		return PatternTypePrefix
	default:
		return ""
	}
}

func adminFilterTypeToCfg(ft adminv2.FilterType) FilterType {
	switch ft {
	case adminv2.FilterType_FILTER_TYPE_INCLUDE:
		return FilterTypeInclude
	case adminv2.FilterType_FILTER_TYPE_EXCLUDE:
		return FilterTypeExclude
	default:
		return ""
	}
}

func adminACLResourceToCfg(resource corecommonv1.ACLResource) ACLResource {
	switch resource {
	case corecommonv1.ACLResource_ACL_RESOURCE_ANY:
		return ACLResourceAny
	case corecommonv1.ACLResource_ACL_RESOURCE_CLUSTER:
		return ACLResourceCluster
	case corecommonv1.ACLResource_ACL_RESOURCE_GROUP:
		return ACLResourceGroup
	case corecommonv1.ACLResource_ACL_RESOURCE_TOPIC:
		return ACLResourceTopic
	case corecommonv1.ACLResource_ACL_RESOURCE_TXN_ID:
		return ACLResourceTXNID
	case corecommonv1.ACLResource_ACL_RESOURCE_SR_SUBJECT:
		return ACLResourceSRSubject
	case corecommonv1.ACLResource_ACL_RESOURCE_SR_REGISTRY:
		return ACLResourceSRRegistry
	case corecommonv1.ACLResource_ACL_RESOURCE_SR_ANY:
		return ACLResourceSRAny
	default:
		return ""
	}
}

func adminACLPatternToCfg(pattern corecommonv1.ACLPattern) ACLPattern {
	switch pattern {
	case corecommonv1.ACLPattern_ACL_PATTERN_ANY:
		return ACLPatternAny
	case corecommonv1.ACLPattern_ACL_PATTERN_LITERAL:
		return ACLPatternLiteral
	case corecommonv1.ACLPattern_ACL_PATTERN_PREFIXED:
		return ACLPatternPrefixed
	case corecommonv1.ACLPattern_ACL_PATTERN_MATCH:
		return ACLPatternMatch
	default:
		return ""
	}
}

func adminACLOperationToCfg(operation corecommonv1.ACLOperation) ACLOperation {
	switch operation {
	case corecommonv1.ACLOperation_ACL_OPERATION_ANY:
		return ACLOperationAny
	case corecommonv1.ACLOperation_ACL_OPERATION_READ:
		return ACLOperationRead
	case corecommonv1.ACLOperation_ACL_OPERATION_WRITE:
		return ACLOperationWrite
	case corecommonv1.ACLOperation_ACL_OPERATION_CREATE:
		return ACLOperationCreate
	case corecommonv1.ACLOperation_ACL_OPERATION_REMOVE:
		return ACLOperationRemove
	case corecommonv1.ACLOperation_ACL_OPERATION_ALTER:
		return ACLOperationAlter
	case corecommonv1.ACLOperation_ACL_OPERATION_DESCRIBE:
		return ACLOperationDescribe
	case corecommonv1.ACLOperation_ACL_OPERATION_CLUSTER_ACTION:
		return ACLOperationClusterAction
	case corecommonv1.ACLOperation_ACL_OPERATION_DESCRIBE_CONFIGS:
		return ACLOperationDescribeConfigs
	case corecommonv1.ACLOperation_ACL_OPERATION_ALTER_CONFIGS:
		return ACLOperationAlterConfigs
	case corecommonv1.ACLOperation_ACL_OPERATION_IDEMPOTENT_WRITE:
		return ACLOperationIdempotentWrite
	default:
		return ""
	}
}

func adminPermissionTypeToCfg(permType corecommonv1.ACLPermissionType) ACLPermissionType {
	switch permType {
	case corecommonv1.ACLPermissionType_ACL_PERMISSION_TYPE_ANY:
		return ACLPermissionTypeAny
	case corecommonv1.ACLPermissionType_ACL_PERMISSION_TYPE_ALLOW:
		return ACLPermissionTypeAllow
	case corecommonv1.ACLPermissionType_ACL_PERMISSION_TYPE_DENY:
		return ACLPermissionTypeDeny
	default:
		return ""
	}
}

func cloudShadowLinkToConfig(sl *controlplanev1.ShadowLink) *ShadowLinkConfig {
	if sl == nil {
		return nil
	}

	cfg := &ShadowLinkConfig{
		Name: sl.GetName(),
		CloudOptions: &CloudShadowLinkOptions{
			ShadowRedpandaID: sl.GetShadowRedpandaId(),
		},
	}

	cfg.ClientOptions = cloudClientOptsToCfg(sl.GetClientOptions())
	// Sync options use the same adminv2 types, so we can reuse the existing functions
	cfg.TopicMetadataSyncOptions = adminTopicMetadataSyncToCfg(sl.GetTopicMetadataSyncOptions())
	cfg.ConsumerOffsetSyncOptions = adminConsumerOffsetSyncToCfg(sl.GetConsumerOffsetSyncOptions())
	cfg.SecuritySyncOptions = adminSecuritySyncToCfg(sl.GetSecuritySyncOptions())
	cfg.SchemaRegistrySyncOptions = adminSchemaRegistrySyncToCfg(sl.GetSchemaRegistrySyncOptions())

	return cfg
}

func cloudClientOptsToCfg(opts *controlplanev1.ShadowLinkClientOptions) *ShadowLinkClientOptions {
	if opts == nil {
		return nil
	}

	cfg := &ShadowLinkClientOptions{
		BootstrapServers:       opts.GetBootstrapServers(),
		SourceClusterID:        opts.GetSourceClusterId(),
		MetadataMaxAgeMs:       opts.GetMetadataMaxAgeMs(),
		ConnectionTimeoutMs:    opts.GetConnectionTimeoutMs(),
		RetryBackoffMs:         opts.GetRetryBackoffMs(),
		FetchWaitMaxMs:         opts.GetFetchWaitMaxMs(),
		FetchMinBytes:          opts.GetFetchMinBytes(),
		FetchMaxBytes:          opts.GetFetchMaxBytes(),
		FetchPartitionMaxBytes: opts.GetFetchPartitionMaxBytes(),
	}

	if tls := opts.GetTlsSettings(); tls != nil {
		cfg.TLSSettings = cloudTLSToCfg(tls)
	}

	if opts.GetAuthenticationConfiguration() != nil {
		cfg.AuthenticationConfiguration = adminAuthToCfg(opts.GetAuthenticationConfiguration())
	}

	return cfg
}

func cloudTLSToCfg(tls *controlplanev1.TLSSettings) *TLSSettings {
	if tls == nil {
		return nil
	}
	tlsSettings := &TLSSettings{
		Enabled:             tls.GetEnabled(),
		DoNotSetSniHostname: tls.GetDoNotSetSniHostname(),
	}

	// Cloud only supports PEM content, not file paths
	if tls.GetCa() != "" || tls.GetKey() != "" || tls.GetCert() != "" {
		tlsSettings.TLSPEMSettings = &TLSPEMSettings{
			CA:   tls.GetCa(),
			Key:  tls.GetKey(),
			Cert: tls.GetCert(),
		}
	}

	return tlsSettings
}

func shadowLinkConfigToCloudUpdate(slCfg *ShadowLinkConfig, id string) *controlplanev1.ShadowLinkUpdate {
	if slCfg == nil {
		return nil
	}

	return &controlplanev1.ShadowLinkUpdate{
		Id:                        id,
		ClientOptions:             mapCloudClientOptions(slCfg.ClientOptions),
		TopicMetadataSyncOptions:  mapTopicMetadataSyncOptions(slCfg.TopicMetadataSyncOptions),
		ConsumerOffsetSyncOptions: mapConsumerOffsetSyncOptions(slCfg.ConsumerOffsetSyncOptions),
		SecuritySyncOptions:       mapSecuritySyncOptions(slCfg.SecuritySyncOptions),
		SchemaRegistrySyncOptions: mapSchemaRegistrySyncOptions(slCfg.SchemaRegistrySyncOptions),
	}
}
