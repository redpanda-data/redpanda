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

#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "outcome.h"
#include "raft/consensus.h"
#include "raft/errc.h"
#include "raft/state_machine.h"
#include "utils/expiring_promise.h"
#include "utils/mutex.h"
#include "vassert.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/bool_class.hh>
#include <seastar/util/log.hh>

#include <absl/container/node_hash_map.h>

#include <optional>
#include <system_error>
#include <variant>

namespace raft {

// clang-format off
CONCEPT(
    template<typename T>
    concept State = requires(T s,
                             model::record_batch batch,
                             const model::record_batch& const_batch) {
        { s.is_batch_applicable(const_batch) } -> std::convertible_to<bool>;
        { s.apply_update(std::move(batch)) } -> std::same_as<ss::future<std::error_code>>;
    };
)
// clang-format on

using persistent_last_applied
  = ss::bool_class<struct persistent_last_applied_tag>;

// The multiplexing STM allows building multiple state machines on top of
// single consensus instance. The mux_stm dispatches state
// applications to correct state implementations using
// `is_batch_applicable` method result as a discriminator. Each state
// implementation must provide the `apply_update` method that is called by
// the state machine when applying entries. The `apply_update` method
// returns an error code that is propagated to the replicate caller.
// The multiplexing STM is designed in such way to allow state updates
// without locking. The result is determined when applying update to the
// state. Thanks to this approach it is easy to implement the optimistic
// locking concurrency control in state.
//
// IMPORTANT: is_batch_applicable results have to be mutually exclusive. i.e.
// when batch is applicable for one state is has to be not applicable for
// another
//
// +---------+               +---------+      +-------+ +---------+
// | caller  |               | mux_stm |      | raft  | | state_1 |
// +---------+               +---------+      +-------+ +---------+
//      |                         |               |          |
//      | replicate_and_wait      |               |          |
//      |------------------------>|               |          |
//      |                         |               |          |
//      |                         | replicate     |          |
//      |                         |-------------->|          |
//      |      -----------------\ |               |          |
//      |      | wait for apply |-|               |          |
//      |      |----------------| |               |          |
//      |                         |               |          |
//      |                         |         apply |          |
//      |                         |<--------------|          |
//      |                         |               |          |
//      |                         | apply_update  |          |
//      |                         |------------------------->|
//      |                         |               |          |
//      |                         |          std::error_code |
//      |                         |<-------------------------|
//      |                         |               |          |
//      |         std::error_code |               |          |
//      |<------------------------|               |          |
//      |                         |               |          |
template<typename... T>
CONCEPT(requires(State<T>, ...))
class mux_state_machine final : public state_machine {
public:
    explicit mux_state_machine(
      ss::logger&, consensus*, persistent_last_applied, T&...);

    mux_state_machine(mux_state_machine&&) = delete;
    mux_state_machine(const mux_state_machine&) = delete;
    mux_state_machine& operator=(mux_state_machine&&) = delete;
    mux_state_machine& operator=(const mux_state_machine&) = delete;
    ~mux_state_machine() final = default;

    // Lifecycle management
    ss::future<> start() final { return raft::state_machine::start(); }

    ss::future<> stop() final {
        _mutex.broken();
        // close the gate so no new requests will be handled
        co_await raft::state_machine::stop();
        // fail all pending replicate requests
        for (auto& p : _promises) {
            p.second.set_value(std::error_code(errc::shutting_down));
        }
        _promises.clear();
    }

    /// Replicates record batch
    ss::future<result<raft::replicate_result>> replicate(model::record_batch&&);

    /// Replicates record batch and waits until state will be applied to the
    /// state machine
    ss::future<std::error_code> replicate_and_wait(
      model::record_batch&& b,
      model::timeout_clock::time_point timeout,
      ss::abort_source& as);

private:
    using promise_t = expiring_promise<std::error_code>;
    // promises used to wait for result of state applies, keyed by offser
    // returned in replication result (i.e. last batch end offset)
    using container_t
      = absl::node_hash_map<model::offset, expiring_promise<std::error_code>>;

    ss::future<> apply(model::record_batch b) final;

    container_t _promises;

    /*
     * Here the _mutex is used to make sure that promise was inserted into
     * the map before apply is executed. In other words we make sure that
     * calls to replicate and emplacing a promise in the map will happen
     * without apply handler executed in between.
     *
     *       +--------+
     *       |
     *       |        +---------------+
     *       |        |   Replicate   |     trigger, separate fibre
     *       |        +-------+-------+- - - - - - - -+
     *       |                |                       |
     * lock <|                |  offset               |
     *       |                v                       |
     *       |        +-------+------+                |
     *       |        |Insert promise|                |
     *       |        |  for offset  |                |
     *       |        +-------+------+                |
     *       |                |                       |
     *       +--------+       | future                |
     *                        |                       |           +--+
     *                        |                       v              |
     *                        |           +-----------+-----------+  |
     *                        |           |  Apply & set value    |  |> lock
     *                        |           +-----------------------+  |
     *                        |                                      |
     *                        V                                   +--+
     *                 +------+------+
     *                 |  Wait for   |
     *                 |   promise   |
     *                 +-------------+
     *
     * Mutex prevents the 'Apply & set value' to be executed between 'Replicate'
     * and 'Insert promise for offset'
     *
     */
    mutex _mutex;
    consensus* _c;
    const persistent_last_applied _persist_last_applied;
    // we keep states in a tuple to automatically dispatch updates to correct
    // state
    std::tuple<T&...> _state;
};

template<typename... T>
CONCEPT(requires(State<T>, ...))
mux_state_machine<T...>::mux_state_machine(
  ss::logger& logger,
  consensus* c,
  persistent_last_applied persist,
  T&... state)
  : raft::state_machine(c, logger, ss::default_priority_class())
  , _c(c)
  , _persist_last_applied(persist)
  , _state(state...) {}

template<typename... T>
CONCEPT(requires(State<T>, ...))
ss::future<result<raft::replicate_result>> mux_state_machine<T...>::replicate(
  model::record_batch&& batch) {
    return ss::with_gate(_gate, [this, batch = std::move(batch)]() mutable {
        return _c->replicate(
          model::make_memory_record_batch_reader(std::move(batch)),
          raft::replicate_options{raft::consistency_level::quorum_ack});
    });
}

template<typename... T>
CONCEPT(requires(State<T>, ...))
ss::future<std::error_code> mux_state_machine<T...>::replicate_and_wait(
  model::record_batch&& b,
  model::timeout_clock::time_point timeout,
  ss::abort_source& as) {
    using ret_t = std::error_code;
    return ss::with_gate(
      _gate, [this, b = std::move(b), timeout, &as]() mutable {
          return _mutex.get_units().then([this, b = std::move(b), timeout, &as](
                                           ss::semaphore_units<> u) mutable {
              return replicate(std::move(b))
                .then([this, u = std::move(u), timeout, &as](
                        result<raft::replicate_result> r) {
                    if (!r) {
                        return ss::make_ready_future<ret_t>(r.error());
                    }

                    auto last_offset = r.value().last_offset;

                    auto [it, insterted] = _promises.emplace(
                      last_offset, expiring_promise<ret_t>{});
                    vassert(
                      insterted,
                      "Prosmise for offset {} already registered",
                      last_offset);
                    return it->second.get_future_with_timeout(
                      timeout, [] { return errc::timeout; }, as);
                });
          });
      });
}

// return value only if state accepts given batch type
template<typename State>
static std::optional<State*>
is_batch_applicable(State& s, const model::record_batch& batch) {
    if (s.is_batch_applicable(batch)) {
        return &s;
    }
    return std::nullopt;
}

template<typename... T>
CONCEPT(requires(State<T>, ...))
ss::future<> mux_state_machine<T...>::apply(model::record_batch b) {
    return ss::with_gate(_gate, [this, b = std::move(b)]() mutable {
        // lookup for the state to apply the update
        auto state = std::apply(
          [&b](T&... st) {
              using variant_t = std::variant<T*...>;
              std::optional<variant_t> res;
              (void)((res = is_batch_applicable(st, b), res) || ...);
              return res;
          },
          _state);

        // applicable state not found
        if (!state) {
            vassert(
              b.header().type == model::record_batch_type::checkpoint
                || b.header().type
                     == model::record_batch_type::raft_configuration,
              "State handler for batch of type: {} not found",
              b.header().type);
            return ss::now();
        }

        auto last_offset = b.last_offset();
        // apply update
        auto result_f = std::visit(
          [b = std::move(b)](auto& state) mutable {
              return state->apply_update(std::move(b));
          },
          *state);

        return result_f.then([this, last_offset](std::error_code ec) {
            return _mutex.with([this, last_offset, ec] {
                if (auto it = _promises.find(last_offset);
                    it != _promises.end()) {
                    it->second.set_value(ec);
                    _promises.erase(it);
                }
                if (
                  _persist_last_applied
                  && last_offset > _c->read_last_applied()) {
                    (void)ss::with_gate(_gate, [this, last_offset] {
                        return _c->write_last_applied(last_offset);
                    });
                }
            });
        });
    });
}

} // namespace raft
