/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "cluster/types.h"
#include "compat/check.h"
#include "compat/cluster_generator.h"
#include "compat/cluster_json.h"
#include "compat/json.h"

namespace compat {

GEN_COMPAT_CHECK(
  cluster::config_status,
  {
      json_write(node);
      json_write(version);
      json_write(restart);
      json_write(unknown);
      json_write(invalid);
  },
  {
      json_read(node);
      json_read(version);
      json_read(restart);
      json_read(unknown);
      json_read(invalid);
  });

GEN_COMPAT_CHECK(
  cluster::cluster_property_kv,
  {
      json_write(key);
      json_write(value);
  },
  {
      json_read(key);
      json_read(value);
  });

GEN_COMPAT_CHECK(
  cluster::config_update_request,
  {
      json_write(upsert);
      json_write(remove);
  },
  {
      json_read(upsert);
      json_read(remove);
  });

GEN_COMPAT_CHECK(
  cluster::config_update_reply,
  {
      json_write(error);
      json_write(latest_version);
  },
  {
      json_read(error);
      json_read(latest_version);
  });

GEN_COMPAT_CHECK(
  cluster::hello_request,
  {
      json_write(peer);
      json_write(start_time);
  },
  {
      json_read(peer);
      json_read(start_time);
  });

GEN_COMPAT_CHECK(
  cluster::hello_reply, { json_write(error); }, { json_read(error); });

GEN_COMPAT_CHECK(
  cluster::feature_update_action,
  {
      json_write(feature_name);
      json_write(action);
  },
  {
      json_read(feature_name);
      json_read(action);
  });

GEN_COMPAT_CHECK(
  cluster::feature_action_request,
  { json_write(action); },
  { json_read(action); });

GEN_COMPAT_CHECK(
  cluster::feature_action_response,
  { json_write(error); },
  { json_read(error); });

GEN_COMPAT_CHECK(
  cluster::feature_barrier_request,
  {
      json_write(tag);
      json_write(peer);
      json_write(entered);
  },
  {
      json_read(tag);
      json_read(peer);
      json_read(entered);
  });

GEN_COMPAT_CHECK(
  cluster::feature_barrier_response,
  {
      json_write(entered);
      json_write(complete);
  },
  {
      json_read(entered);
      json_read(complete);
  });

GEN_COMPAT_CHECK(
  cluster::join_request, { json_write(node); }, { json_read(node); });

GEN_COMPAT_CHECK(
  cluster::join_reply, { json_write(success); }, { json_read(success); });

GEN_COMPAT_CHECK(
  cluster::join_node_request,
  {
      json_write(logical_version);
      json_write(node_uuid);
      json_write(node);
  },
  {
      json_read(logical_version);
      json_read(node_uuid);
      json_read(node);
  });

GEN_COMPAT_CHECK(
  cluster::join_node_reply,
  {
      json_write(success);
      json_write(id);
  },
  {
      json_read(success);
      json_read(id);
  })

} // namespace compat
