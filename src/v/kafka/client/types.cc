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

#include "kafka/client/types.h"

#include "utils/to_string.h"

namespace kafka::client {
fmt::iterator metadata_update::broker::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{node_id: {}, host: {}, port: {}, rack: {}}}",
      node_id,
      host,
      port,
      rack);
}

fmt::iterator metadata_update::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{brokers: {}, cluster_id: {}, controller_id: {}, topics: {}, "
      "cluster_authorized_operations: {}}}",
      brokers,
      cluster_id,
      controller_id,
      topics,
      cluster_authorized_operations);
}

} // namespace kafka::client
