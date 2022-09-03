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

#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "outcome.h"
#include "raft/consensus.h"
#include "raft/errc.h"
#include "raft/state_machine.h"
#include "ssx/future-util.h"
#include "utils/expiring_promise.h"
#include "vassert.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/util/bool_class.hh>
#include <seastar/util/log.hh>

#include <absl/container/node_hash_map.h>

#include <optional>
#include <system_error>
#include <variant>

namespace raft {

// clang-format off
    template<typename T>
    concept State = requires(T s,
                             model::record_batch batch,
                             const model::record_batch& const_batch) {
        { s.is_batch_applicable(const_batch) } -> std::convertible_to<bool>;
        { s.apply_update(std::move(batch)) } -> std::same_as<ss::future<std::error_code>>;
    };
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
requires(State<T>, ...) class mux_state_machine final : public state_machine {
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
        // close the gate so no new requests will be handled
        _new_result.broken();
        co_await raft::state_machine::stop();
        vassert(
          _pending == 0,
          "destorying state machine with {} pending replicate requests",
          _pending);
    }

    /// Replicates record batch
    ss::future<result<raft::replicate_result>>
    replicate(model::record_batch&&, std::optional<model::term_id>);

    /// Replicates record batch and waits until state will be applied to the
    /// state machine
    ss::future<std::error_code> replicate_and_wait(
      model::record_batch&& b,
      model::timeout_clock::time_point timeout,
      ss::abort_source& as,
      std::optional<model::term_id> term = std::nullopt);

private:
    using promise_t = expiring_promise<std::error_code>;

    ss::future<> apply(model::record_batch b) final;
    class replicate_units {
    public:
        explicit replicate_units(mux_state_machine<T...>* stm)
          : _stm(stm) {
            _stm->_pending++;
            _units = 1;
        }

        replicate_units(replicate_units&& other) noexcept
          : _stm(other._stm)
          , _units(std::exchange(other._units, 0)) {}

        replicate_units(const replicate_units&) noexcept = delete;

        replicate_units& operator=(replicate_units&& o) noexcept {
            _stm = o._stm;
            _units = std::exchange(o._units, 0);
            return *this;
        }
        replicate_units& operator=(const replicate_units&) noexcept = delete;

        void release() {
            _stm->_pending -= _units;
            _units = 0;
        }

        ~replicate_units() { _stm->_pending -= _units; }

    private:
        mux_state_machine<T...>* _stm;
        int _units;
    };

    replicate_units get_units() { return replicate_units(this); }

    consensus* _c;
    absl::node_hash_map<model::offset, std::error_code> _results;
    model::offset _last_applied;
    int64_t _pending = 0;
    const persistent_last_applied _persist_last_applied;
    ss::condition_variable _new_result;
    // we keep states in a tuple to automatically dispatch updates to correct
    // state
    std::tuple<T&...> _state;
};

template<typename... T>
requires(State<T>, ...) mux_state_machine<T...>::mux_state_machine(
  ss::logger& logger,
  consensus* c,
  persistent_last_applied persist,
  T&... state)
  : raft::state_machine(c, logger, ss::default_priority_class())
  , _c(c)
  , _persist_last_applied(persist)
  , _state(state...) {}

template<typename... T>
requires(State<T>, ...)
  ss::future<result<raft::replicate_result>> mux_state_machine<T...>::replicate(
    model::record_batch&& batch, std::optional<model::term_id> term) {
    return ss::with_gate(
      _gate, [this, batch = std::move(batch), term]() mutable {
          if (term) {
              return _c->replicate(
                term.value(),
                model::make_memory_record_batch_reader(std::move(batch)),
                raft::replicate_options{raft::consistency_level::quorum_ack});
          }

          return _c->replicate(
            model::make_memory_record_batch_reader(std::move(batch)),
            raft::replicate_options{raft::consistency_level::quorum_ack});
      });
}

template<typename... T>
requires(State<T>, ...)
  ss::future<std::error_code> mux_state_machine<T...>::replicate_and_wait(
    model::record_batch&& b,
    model::timeout_clock::time_point timeout,
    ss::abort_source& as,
    std::optional<model::term_id> term) {
    if (_gate.is_closed()) {
        return ss::make_ready_future<std::error_code>(errc::shutting_down);
    }

    auto u = get_units();
    auto promise = std::make_unique<promise_t>();
    auto f = promise->get_future_with_timeout(
      timeout, [] { return make_error_code(errc::timeout); }, as);

    ssx::spawn_with_gate(
      _gate,
      [this,
       b = std::move(b),
       term,
       u = std::move(u),
       promise = std::move(promise)]() mutable {
          return replicate(std::move(b), term)
            .then_wrapped(
              [this, u = std::move(u), promise = std::move(promise)](
                ss::future<result<raft::replicate_result>> f) mutable {
                  if (f.failed()) {
                      promise->set_exception(f.get_exception());
                      return ss::now();
                  }
                  auto r = f.get();
                  if (!r) {
                      promise->set_value(r.error());
                      return ss::now();
                  }

                  auto last_offset = r.value().last_offset;

                  return _new_result
                    .wait([this, last_offset] {
                        return _last_applied >= last_offset;
                    })
                    .then_wrapped(
                      [this,
                       last_offset,
                       u = std::move(u),
                       promise = std::move(promise)](ss::future<> f) mutable {
                          u.release();
                          if (f.failed()) {
                              promise->set_exception(f.get_exception());
                              return;
                          }
                          auto it = _results.find(last_offset);
                          vassert(
                            it != _results.end(),
                            "last applied offset {} is greater than "
                            "returned replicate result {}. this must "
                            "imply existence of a result in results map",
                            _last_applied,
                            last_offset);
                          auto res = it->second;
                          _results.erase(it);
                          if (_pending == 0) {
                              _results.clear();
                          }
                          promise->set_value(res);
                      });
              });
      });
    return f;
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
requires(State<T>, ...) ss::future<> mux_state_machine<T...>::apply(
  model::record_batch b) {
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
            _last_applied = last_offset;
            if (_pending > 0) {
                _results.emplace(last_offset, ec);
                _new_result.broadcast();
            } else {
                _results.clear();
            }
            if (
              _persist_last_applied && last_offset > _c->read_last_applied()) {
                ssx::spawn_with_gate(_gate, [this, last_offset] {
                    return _c->write_last_applied(last_offset);
                });
            }
        });
    });
}

} // namespace raft
