/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/outcome.h"
#include "base/seastarx.h"
#include "datalake/coordinator/state.h"
#include "model/fundamental.h"
#include "model/timestamp.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

namespace datalake::coordinator {

class snapshot_remover {
public:
    enum class errc {
        failed,
        shutting_down,
    };
    virtual ss::future<checked<std::nullopt_t, errc>> remove_expired_snapshots(
      model::topic, const topics_state&, model::timestamp) const = 0;
    virtual ~snapshot_remover() = default;
};

class noop_snapshot_remover : public snapshot_remover {
public:
    ss::future<checked<std::nullopt_t, snapshot_remover::errc>>
    remove_expired_snapshots(
      model::topic, const topics_state&, model::timestamp) const final {
        co_return std::nullopt;
    }
    ~noop_snapshot_remover() override = default;
};

} // namespace datalake::coordinator
