/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "kafka/protocol/errors.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"

#include <fmt/core.h>

#include <exception>

namespace kafka::client {

struct broker_error final : std::exception {
    broker_error(model::node_id n_id, error_code err)
      : std::exception{}
      , msg{fmt::format("{{ node: {} }}, {}", n_id, err)}
      , node_id{n_id}
      , error{err} {}
    const char* what() const noexcept final { return msg.c_str(); }
    std::string msg;
    model::node_id node_id;
    error_code error;
};

struct partition_error final : std::exception {
    partition_error(model::topic_partition tp, error_code e)
      : std::exception{}
      , msg{fmt::format("{}, {}", tp, e)}
      , tp{std::move(tp)}
      , error{e} {}
    const char* what() const noexcept final { return msg.c_str(); }
    std::string msg;
    model::topic_partition tp;
    error_code error;
};

struct consumer_error final : std::exception {
    consumer_error(kafka::group_id g_id, kafka::member_id m_id, error_code e)
      : std::exception{}
      , msg{fmt::format("{}, {}, {}", g_id, m_id, e)}
      , group_id{std::move(g_id)}
      , member_id{std::move(m_id)}
      , error{e} {}
    const char* what() const noexcept final { return msg.c_str(); }
    std::string msg;
    kafka::group_id group_id;
    kafka::member_id member_id;
    error_code error;
};

} // namespace kafka::client
