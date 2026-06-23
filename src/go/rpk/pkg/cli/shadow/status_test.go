// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package shadow

import (
	"testing"
	"time"

	adminv2 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/admin/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestFromAdminV2ShadowLinkSchemaRegistry(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		link *adminv2.ShadowLink
		want *schemaRegistrySyncStatus
	}{
		{
			name: "no status",
			link: &adminv2.ShadowLink{Name: "no-status"},
			want: nil,
		},
		{
			name: "status without schema registry sync",
			link: &adminv2.ShadowLink{
				Name:   "topic-mode",
				Status: &adminv2.ShadowLinkStatus{},
			},
			want: nil,
		},
		{
			name: "full schema registry sync status",
			link: &adminv2.ShadowLink{
				Name: "sr-link",
				Uid:  "uid-123",
				Status: &adminv2.ShadowLinkStatus{
					SchemaRegistrySyncStatus: &adminv2.SchemaRegistrySyncStatus{
						Inventory: &adminv2.SchemaRegistryInventory{
							SelectedSourceSubjects:        10,
							SelectedSourceSubjectVersions: 25,
							DestinationSubjects:           8,
							DestinationSubjectVersions:    20,
						},
						CurrentSync: &adminv2.SchemaRegistryCurrentSync{
							SyncType: adminv2.SchemaRegistrySyncType_SCHEMA_REGISTRY_SYNC_TYPE_FULL,
							Summary: &adminv2.SchemaRegistrySyncSummary{
								StartTime:              timestamppb.New(start),
								SubjectVersionsChanged: 5,
								Errors:                 1,
							},
						},
						LastFullSync: &adminv2.SchemaRegistrySyncSummary{
							StartTime:                   timestamppb.New(start),
							FinishTime:                  timestamppb.New(start.Add(time.Minute)),
							SubjectVersionsChanged:      25,
							CompatibilityConfigsChanged: 3,
							ModesChanged:                1,
							UnsupportedFeaturesRemoved:  2,
						},
						TotalsSinceTaskStart: &adminv2.SchemaRegistrySyncSummary{
							SubjectVersionsChanged: 100,
						},
						LastErrorMessage: "transient source error",
					},
				},
			},
			want: &schemaRegistrySyncStatus{
				Inventory: &schemaRegistryInventory{
					SelectedSourceSubjects:        10,
					SelectedSourceSubjectVersions: 25,
					DestinationSubjects:           8,
					DestinationSubjectVersions:    20,
				},
				CurrentSync: &schemaRegistryCurrentSync{
					SyncType: "FULL",
					Summary: &schemaRegistrySyncSummary{
						StartTime:              "2024-01-01T00:00:00Z",
						SubjectVersionsChanged: 5,
						Errors:                 1,
					},
				},
				LastFullSync: &schemaRegistrySyncSummary{
					StartTime:                   "2024-01-01T00:00:00Z",
					FinishTime:                  "2024-01-01T00:01:00Z",
					SubjectVersionsChanged:      25,
					CompatibilityConfigsChanged: 3,
					ModesChanged:                1,
					UnsupportedFeaturesRemoved:  2,
				},
				TotalsSinceTaskStart: &schemaRegistrySyncSummary{
					SubjectVersionsChanged: 100,
				},
				LastErrorMessage: "transient source error",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := fromAdminV2ShadowLink(tt.link)
			require.Equal(t, tt.want, got.SchemaRegistry)
		})
	}
}
