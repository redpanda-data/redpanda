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
 * Executes fetch plan, it uses op_context to fill fetch response placeholders
 */
class fetch_plan_executor final {
public:
    struct impl {
        virtual ss::future<> execute_plan(op_context&, fetch_plan) = 0;
        virtual ~impl() noexcept = default;
    };

    explicit fetch_plan_executor(std::unique_ptr<impl> impl)
      : _impl(std::move(impl)) {}

    ss::future<> execute_plan(op_context& octx, fetch_plan plan) {
        return _impl->execute_plan(octx, std::move(plan));
    }

private:
    std::unique_ptr<impl> _impl;
};

template<typename T, typename... Args>
fetch_plan_executor make_fetch_plan_executor(Args&&... args) {
    return fetch_plan_executor(
      std::make_unique<T>(std::forward<Args>(args)...));
}

} // namespace kafka
