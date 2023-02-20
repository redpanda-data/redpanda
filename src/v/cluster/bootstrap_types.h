// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "cluster/types.h"
#include "model/fundamental.h"
#include "serde/serde.h"

#include <fmt/core.h>

#include <vector>

namespace cluster {

struct cluster_bootstrap_info_request
  : serde::envelope<
      cluster_bootstrap_info_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    friend std::ostream&
    operator<<(std::ostream& o, const cluster_bootstrap_info_request&) {
        fmt::print(o, "{{}}");
        return o;
    }

    auto serde_fields() { return std::tie(); }
};

struct cluster_bootstrap_info_reply
  : serde::envelope<
      cluster_bootstrap_info_reply,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    model::broker broker;
    cluster_version version;
    std::vector<net::unresolved_address> seed_servers;
    bool empty_seed_starts_cluster;
    std::optional<model::cluster_uuid> cluster_uuid;
    model::node_uuid node_uuid;

    auto serde_fields() {
        return std::tie(
          broker,
          version,
          seed_servers,
          empty_seed_starts_cluster,
          cluster_uuid,
          node_uuid);
    }

    friend std::ostream&
    operator<<(std::ostream& o, const cluster_bootstrap_info_reply& v) {
        fmt::print(
          o,
          "{{broker: {}, version: {}, seed_servers: {}, "
          "empty_seed_starts_cluster: {}, cluster_uuid: {}, node_uuid: {}}}",
          v.broker,
          v.version,
          v.seed_servers,
          v.empty_seed_starts_cluster,
          v.cluster_uuid,
          v.node_uuid);
        return o;
    }
};

} // namespace cluster
