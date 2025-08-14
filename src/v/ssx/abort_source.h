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

#include "base/seastarx.h"
#include "base/vassert.h"

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

/// A composite abort source is a new abort source that is subscribed to
/// the given abort sources and will request abort if any of them
/// requests abort.
///
/// The abort on the composite abort source does not propagate to the
/// underlying abort sources, it only requests abort on the composite
/// abort source itself. Pay attention to this as it changes the
/// semantics of your code if you rely on propagating aborts to the caller.
class composite_abort_source {
public:
    composite_abort_source(
      ss::abort_source& as1, ss::abort_source& as2) noexcept {
        for (auto as : {&as1, &as2}) {
            if (as->abort_requested()) {
                _as.request_abort_ex(as->abort_requested_exception_ptr());
                return;
            }
        }

        for (auto as : {&as1, &as2}) {
            // Safe to capture `this` here because the subscription lambda
            // will be destroyed together with the `composite_abort_source`
            // instance.
            auto sub = as->subscribe(
              [this](const std::optional<std::exception_ptr>& ex) noexcept {
                  if (ex) {
                      _as.request_abort_ex(*ex);
                  } else {
                      _as.request_abort();
                  }
              });

            vassert(
              sub, "Empty subscription after we checked for abort_requested");

            _subs.emplace_back(std::move(*sub));
        }
    }

    /// Returns the underlying abort source for interoperability with
    /// the Seastar APIs.
    ss::abort_source& as() noexcept { return _as; }

    /// Returns the underlying abort source for interoperability with
    /// the Seastar APIs.
    const ss::abort_source& as() const noexcept { return _as; }

private:
    ss::abort_source _as;
    std::vector<ss::abort_source::subscription> _subs;
};

} // namespace ssx
