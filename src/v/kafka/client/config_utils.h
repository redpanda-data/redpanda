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
#include "kafka/client/configuration.h"
#include "security/acl.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

namespace kafka::client {

bool is_scram_configured(const configuration& client_cfg);

ss::future<std::optional<kafka::client::sasl_configuration>>
create_client_credentials(
  ::cluster::controller& controller,
  const kafka::client::configuration& client_cfg,
  security::acl_principal principal);

} // namespace kafka::client
