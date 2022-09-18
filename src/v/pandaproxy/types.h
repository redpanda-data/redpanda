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
#include "kafka/client/client.h"
#include "kafka/protocol/errors.h"
#include "model/timestamp.h"

#include <seastar/core/shared_ptr.hh>

#include <chrono>

namespace pandaproxy {

struct timestamped_client {
    kafka::client::client real;
    model::timestamp last_used;

    timestamped_client(YAML::Node const& cfg, model::timestamp t)
      : real{cfg}
      , last_used{t} {}
};
using client_ptr = ss::lw_shared_ptr<timestamped_client>;

struct credential_t {
    ss::sstring name;
    ss::sstring pass;

    credential_t() = default;
    credential_t(ss::sstring n, ss::sstring p)
      : name{std::move(n)}
      , pass{std::move(p)} {}
};

} // namespace pandaproxy
