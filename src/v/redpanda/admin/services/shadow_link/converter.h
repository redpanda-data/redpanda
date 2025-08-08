/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster_link/model/types.h"
#include "proto/redpanda/core/admin/shadow_link.proto.h"

namespace admin {

/// \brief Converts a create cluster link request into a cluster link metadata
/// object
///
/// \throws std::invalid_argument if the request contains invalid data
cluster_link::model::metadata
convert_create_to_metadata(proto::admin::create_shadow_link_request req);

/// \brief Converts a cluster link metadata object into a shadow link resource
proto::admin::shadow_link
metadata_to_shadow_link(cluster_link::model::metadata md);
} // namespace admin
