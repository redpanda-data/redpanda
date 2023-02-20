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

#include "cluster/fwd.h"
#include "config/fwd.h"
#include "kafka/client/fwd.h"
#include "seastarx.h"
#include "security/acl.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include <vector>

namespace pandaproxy {

ss::future<std::unique_ptr<kafka::client::configuration>>
create_client_credentials(
  cluster::controller& controller,
  config::configuration const& cluster_cfg,
  kafka::client::configuration const& client_cfg,
  security::acl_principal principal);

ss::future<> set_client_credentials(
  kafka::client::configuration const& client_cfg,
  ss::sharded<kafka::client::client>& client);

} // namespace pandaproxy
