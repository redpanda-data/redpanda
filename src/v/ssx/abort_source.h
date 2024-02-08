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
#include "ssx/future-util.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/sharded.hh>

#include <exception>

namespace ssx {

/**
 * A sharded version of `ss::abort_source` that allows any shard to request an
 * abort on all other shards.
 *
 * Sharing one instance of this class across threads raises difficult lifetime
 * and thread safety issues, so the intended usage pattern for this class is to
 * create it on one shard, then make a copy of this original object on every
 * shard where it will be used. All internal state is wrapped in a shared
 * pointer so these copies reference the same underlying state.
 */
class sharded_abort_source {
public:
    explicit sharded_abort_source(ss::abort_source& parent)
      : _internal(std::make_shared<sharded_abort_source_internal>(
        ss::this_shard_id(),
        std::vector<ss::abort_source>(ss::smp::count),
        std::nullopt)) {
        auto dex = parent.get_default_exception();
        auto sub = parent.subscribe(
          [as = *this,
           dex](std::optional<std::exception_ptr> const& ex) mutable noexcept {
              dex = ex.value_or(dex);
              ssx::background = as.request_abort_ex(dex);
          });
        if (sub) {
            _internal->sub.emplace(std::move(*sub));
        }
    }

    // Returns a reference to an abort_source local to the calling shard
    //
    // Note: the lifetime of `sharded_abort_source` must exceed the lifetime of
    // this reference.
    auto& local() noexcept { return _internal->as[ss::this_shard_id()]; }

    // Returns a const reference to an abort_source local to the calling shard
    //
    // Note: the lifetime of `sharded_abort_source` must exceed the lifetime of
    // this reference.
    auto const& local() const noexcept {
        return _internal->as[ss::this_shard_id()];
    }

    // Requests an abort with exception_ptr `ex` on every shard's abort_source.
    ss::future<> request_abort_ex(std::exception_ptr ex) noexcept {
        return ss::smp::invoke_on_all(
          [ex, as = *this]() mutable { as.local().request_abort_ex(ex); });
    }

    // Requests an abort with exception `e` on every shard's abort_source.
    template<typename Exception>
    ss::future<> request_abort_ex(Exception&& e) noexcept {
        return request_abort_ex(std::make_exception_ptr(e));
    }

    // Requests an abort with the default_exception type on every shard's
    // abort_source.
    ss::future<> request_abort() noexcept {
        return request_abort_ex(local().get_default_exception());
    }

    // Subscribes to the calling shard's local abort_source.
    template<typename Func>
    [[nodiscard]] auto subscribe(Func&& func) {
        return local().subscribe(std::forward<Func>(func));
    }

    // Checks the calling shard's abort_source to see if an abort was requested.
    auto abort_requested() const noexcept { return local().abort_requested(); }

    // Throws the abort exception of the calling shard's abort_source if its
    // been aborted.
    auto check() const { return local().check(); }

    // Removes subscription to parent abort source and requests an abort on
    // every shard's abort_source.
    ss::future<> stop() noexcept {
        vassert(
          ss::this_shard_id() == _internal->orig_shard,
          "sharded_abort_source must be stopped on its original shard");

        _internal->sub.reset();
        return request_abort();
    }

private:
    struct sharded_abort_source_internal {
        ss::shard_id orig_shard;
        std::vector<ss::abort_source> as;
        std::optional<ss::abort_source::subscription> sub;
    };

    std::shared_ptr<sharded_abort_source_internal> _internal;
};

struct shutdown_requested_exception : ss::abort_requested_exception {};
struct connection_aborted_exception : ss::abort_requested_exception {};

} // namespace ssx
