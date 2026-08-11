/**
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/dev/licenses/rcl.md
 *
 */

#pragma once

#include "cluster_link/model/types.h"
#include "proto/redpanda/core/admin/v2/shadow_link.proto.h"

namespace admin {

/// \brief Sets the client ID for the cluster link
///
/// The client is is the format:
/// `cluster-link-{cluster-link-name}-{cluster-link-uuid}`
void set_client_id(cluster_link::model::metadata& md);

/// \brief Converts a create cluster link request into a cluster link metadata
/// object
///
/// \throws serde::pb::rpc::invalid_argument_exception if the request contains
/// invalid data
cluster_link::model::metadata
convert_create_to_metadata(proto::admin::create_shadow_link_request req);

/// \brief Converts a cluster link metadata object into a shadow link resource
proto::admin::shadow_link metadata_to_shadow_link(
  cluster_link::model::metadata_ptr md,
  cluster_link::model::shadow_link_status_report status_report);

/// \brief Converts an update shadow link request in to the appropriate model
/// type
///
/// \throws serde::pb::rpc::invalid_argument_exception if the request contains
/// invalid data
cluster_link::model::update_cluster_link_configuration_cmd
create_update_cluster_link_config_cmd(
  proto::admin::update_shadow_link_request req,
  cluster_link::model::metadata_ptr current_metadata);

/// \brief Converts model data into a ShadowTopic resource
proto::admin::shadow_topic model_to_shadow_topic(
  ::model::topic_view,
  const cluster_link::model::mirror_topic_metadata& md,
  const cluster_link::model::shadow_link_status_report&);
} // namespace admin
