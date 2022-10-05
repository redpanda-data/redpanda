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

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>

#include <chrono>

namespace pandaproxy {

using client_ptr = ss::lw_shared_ptr<kafka::client::client>;

struct timestamped_user {
    using clock = ss::lowres_clock;
    using time_point = clock::time_point;

    ss::sstring key;
    client_ptr client;
    time_point last_used;

    timestamped_user(ss::sstring k, client_ptr c, time_point t)
      : key{std::move(k)}
      , client{std::move(c)}
      , last_used{t} {}

    timestamped_user(ss::sstring k, client_ptr c)
      : key{std::move(k)}
      , client{std::move(c)}
      , last_used{clock::now()} {}
};

struct credential_t {
    ss::sstring name;
    ss::sstring pass;

    credential_t() = default;
    credential_t(ss::sstring n, ss::sstring p)
      : name{std::move(n)}
      , pass{std::move(p)} {}
};

} // namespace pandaproxy
