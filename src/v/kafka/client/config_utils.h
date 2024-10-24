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

#include "base/seastarx.h"
#include "cluster/fwd.h"
#include "config/fwd.h"
#include "kafka/client/fwd.h"
#include "model/compression.h"
#include "security/acl.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include <vector>

namespace kafka::client {

ss::future<std::unique_ptr<kafka::client::configuration>>
create_client_credentials(
  cluster::controller& controller,
  const config::configuration& cluster_cfg,
  const kafka::client::configuration& client_cfg,
  security::acl_principal principal);

void set_client_credentials(
  const kafka::client::configuration& client_cfg,
  kafka::client::client& client);

ss::future<> set_client_credentials(
  const kafka::client::configuration& client_cfg,
  ss::sharded<kafka::client::client>& client);

model::compression compression_from_str(std::string_view v);

} // namespace kafka::client
