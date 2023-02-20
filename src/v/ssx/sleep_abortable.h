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

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/weak_ptr.hh>
#include <seastar/util/optimized_optional.hh>

#include <tuple>

namespace ssx {
/// Same as seastar::abort_source but accepts multiple abort sources
template<typename... AbortSource>
seastar::future<>
sleep_abortable(seastar::lowres_clock::duration dur, AbortSource&... src) {
    struct as_state : seastar::weakly_referencable<as_state> {
        std::vector<
          seastar::optimized_optional<seastar::abort_source::subscription>>
          subscriptions;
        seastar::abort_source as;
    };
    struct as_callback {
        explicit as_callback(as_state& st)
          : state(st.weak_from_this()) {}
        void operator()() noexcept {
            if (auto st = state.get();
                st != nullptr && !st->as.abort_requested()) {
                st->as.request_abort();
            }
        }
        seastar::weak_ptr<as_state> state;
    };
    auto state = seastar::make_lw_shared<as_state>();
    state->subscriptions.reserve(sizeof...(AbortSource));
    as_callback cb(*state);
    (cb.state->subscriptions.push_back(src.subscribe(cb)), ...);
    return seastar::sleep_abortable(dur, cb.state->as)
      .finally([st = std::move(state)] {});
}
} // namespace ssx