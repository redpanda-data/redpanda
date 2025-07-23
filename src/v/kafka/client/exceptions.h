/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "kafka/protocol/exceptions.h"
#include "kafka/protocol/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"

#include <fmt/core.h>

#include <exception>

namespace kafka::client {

struct broker_error final : exception_base {
    broker_error(model::node_id n_id, error_code e, std::string_view msg)
      : exception_base{e, fmt::format("{{ node: {} }}, {}: {}", n_id, e, msg)}
      , node_id{n_id} {}
    broker_error(model::node_id n_id, error_code e)
      : exception_base{e, fmt::format("{{ node: {} }}, {}", n_id, e)}
      , node_id{n_id} {}
    model::node_id node_id;
};

struct topic_error final : exception_base {
    topic_error(model::topic_view tp, error_code e)
      : exception_base{e, fmt::format("{}, {}", tp, e)}
      , topic{tp} {}
    model::topic topic;
};

struct partition_error final : exception_base {
    partition_error(model::topic_partition tp, error_code e)
      : exception_base{e, fmt::format("{}, {}", tp, e)}
      , tp{std::move(tp)} {}
    model::topic_partition tp;
};

struct consumer_error final : exception_base {
    consumer_error(kafka::group_id g_id, kafka::member_id m_id, error_code e)
      : exception_base{e, fmt::format("{}, {}, {}", g_id, m_id, e)}
      , group_id{std::move(g_id)}
      , member_id{std::move(m_id)} {}
    kafka::group_id group_id;
    kafka::member_id member_id;
};

} // namespace kafka::client
