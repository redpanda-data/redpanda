/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/oncore.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/sharded.hh>

#include <exception>

namespace ssx {

class sharded_abort_source {
public:
    ss::future<> start(ss::abort_source& parent) {
        return _as.start().then([this, &parent]() {
            auto dex = parent.get_default_exception();
            auto sub = parent.subscribe(
              [this, dex](
                const std::optional<std::exception_ptr>& ex) mutable noexcept {
                  dex = ex.value_or(dex);
                  return _as.invoke_on_all(
                    [dex](auto& as) { return as.request_abort_ex(dex); });
              });
            if (sub) {
                _sub.emplace(std::move(*sub));
            }
        });
    }

    ss::future<> stop() noexcept {
        _sub.reset();
        return request_abort().then([this]() { return _as.stop(); });
    }

    auto& local() noexcept { return _as.local(); }
    const auto& local() const noexcept { return _as.local(); }

    template<typename Func>
    [[nodiscard]] auto subscribe(Func&& func) {
        return local().subscribe(std::forward<Func>(func));
    }

    ss::future<> request_abort_ex(std::exception_ptr ex) noexcept {
        return _as.invoke_on_all(
          [ex](auto& s) { return s.request_abort_ex(ex); });
    }

    template<typename Exception>
    ss::future<> request_abort_ex(Exception&& e) noexcept {
        return request_abort_ex(std::make_exception_ptr(e));
    }

    ss::future<> request_abort() noexcept {
        return request_abort_ex(local().get_default_exception());
    }

    auto abort_requested() const noexcept { return local().abort_requested(); }
    auto check() const { return local().check(); }

    bool local_is_initialized() const noexcept {
        return _as.local_is_initialized();
    }

private:
    ss::sharded<ss::abort_source> _as;
    std::optional<ss::abort_source::subscription> _sub;
};

struct shutdown_requested_exception : ss::abort_requested_exception {};
struct connection_aborted_exception : ss::abort_requested_exception {};

} // namespace ssx
