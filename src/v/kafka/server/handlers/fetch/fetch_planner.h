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
#include "kafka/server/handlers/fetch.h"

namespace kafka {
/**
 * Fetch planer, creates a fetch plan, set of all partition reads groupped into
 * shards, based on the fetch request.
 */
class fetch_planner final {
public:
    struct impl {
        virtual fetch_plan create_plan(op_context&) = 0;
        virtual ~impl() noexcept = default;
    };

    explicit fetch_planner(std::unique_ptr<impl> impl)
      : _impl(std::move(impl)) {}

    fetch_plan create_plan(op_context& octx) {
        return _impl->create_plan(octx);
    }

private:
    std::unique_ptr<impl> _impl;
};

template<typename T, typename... Args>
fetch_planner make_fetch_planner(Args&&... args) {
    return fetch_planner(std::make_unique<T>(std::forward<Args>(args)...));
}

} // namespace kafka
