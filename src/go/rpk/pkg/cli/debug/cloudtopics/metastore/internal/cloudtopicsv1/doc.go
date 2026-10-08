// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package cloudtopicsv1 holds Go bindings generated from the INTERNAL admin
// API proto redpanda/core/admin/internal/cloud_topics/v1/metastore.proto.
//
// This is deliberately NOT sourced from the published buf.build/redpandadata
// core module: that proto path is excluded from the registry and from
// buf-breaking checks, so the API carries no compatibility guarantees. These
// bindings are regenerated locally (see metastore/gen) and live under an
// internal/ package so nothing outside the metastore command can depend on
// them. Do not promote this to a public rpadmin accessor.
package cloudtopicsv1
