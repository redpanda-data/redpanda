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

#include "base/outcome.h"
#include "base/vassert.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/record_batch_types.h"
#include "model/timeout_clock.h"
#include "raft/consensus.h"
#include "raft/errc.h"
#include "raft/state_machine.h"
#include "ssx/future-util.h"
#include "utils/expiring_promise.h"
#include "utils/mutex.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/util/bool_class.hh>
#include <seastar/util/log.hh>

#include <absl/container/flat_hash_set.h>
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
requires(State<T>, ...)
class mux_state_machine : public state_machine {
public:
    using base_t = mux_state_machine<T...>;

    explicit mux_state_machine(
      ss::logger&,
      consensus*,
      persistent_last_applied,
      absl::flat_hash_set<model::record_batch_type>,
      T&...);

    mux_state_machine(mux_state_machine&&) = delete;
    mux_state_machine(const mux_state_machine&) = delete;
    mux_state_machine& operator=(mux_state_machine&&) = delete;
    mux_state_machine& operator=(const mux_state_machine&) = delete;
    ~mux_state_machine() = default;

    // Lifecycle management
    ss::future<> start() final { return raft::state_machine::start(); }

    ss::future<> stop() override {
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

    /// Tries to save the current state of the state machine to the raft
    /// snapshot and, if successful, prefix-truncates the raft log to the
    /// snapshot offset. State machine may refuse to create a snapshot if it is
    /// not ready, in which case false is returned.
    ///
    /// NOTE: this is incompatible with NTPs where log prefix-truncation is
    /// managed by log_eviction_stm (such as ordinary kafka partitions) because
    /// log_eviction_stm will write an empty raft snapshot, making restoring
    /// state from the snapshot impossible.
    ss::future<bool> maybe_write_snapshot();

    model::offset get_last_applied_offset() const {
        return last_applied_offset();
    }

    /// Compose a snapshot without persisting it: this is not for snapshotting
    /// the raft log, but rather for snapshots we will send over the network,
    /// for example to new joining nodes.
    ss::future<std::optional<iobuf>> maybe_compose_snapshot();

private:
    using promise_t = expiring_promise<std::error_code>;

    ss::future<> apply(model::record_batch b) final;
    ss::future<> do_apply(model::record_batch b);
    ss::future<> handle_raft_snapshot() final;

    // called after a batch is applied
    virtual ss::future<> on_batch_applied() { return ss::now(); }

    virtual ss::future<std::optional<iobuf>>
    maybe_make_snapshot(ssx::semaphore_units apply_mtx_holder) = 0;
    virtual ss::future<>
    apply_snapshot(model::offset, storage::snapshot_reader&) = 0;

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
    absl::flat_hash_set<model::record_batch_type> _not_handled_batch_types;

protected:
    // Mutexes must be locked in the same order as declared

    // Locked for the whole duration of writing a snapshot to ensure that there
    // are no concurrent attempts.
    mutex _write_snapshot_mtx{"mex_state_machine::write_snapshot"};
    // Locked when a command is applied to the stm or when creating a snapshot
    // to ensure that the state machine state does not change.
    mutex _apply_mtx{"mex_state_machine::apply"};

    // we keep states in a tuple to automatically dispatch updates to correct
    // state
    std::tuple<T&...> _state;
};

template<typename... T>
requires(State<T>, ...)
mux_state_machine<T...>::mux_state_machine(
  ss::logger& logger,
  consensus* c,
  persistent_last_applied persist,
  absl::flat_hash_set<model::record_batch_type> not_handled_batch_types,
  T&... state)
  : raft::state_machine(c, logger, ss::default_priority_class())
  , _c(c)
  , _persist_last_applied(persist)
  , _not_handled_batch_types(std::move(not_handled_batch_types))
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
    auto f = promise
               ->get_future_with_timeout(
                 timeout, [] { return make_error_code(errc::timeout); }, as)
               .handle_exception_type([](const ss::abort_requested_exception&) {
                   return make_error_code(errc::shutting_down);
               });

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
                          if (
                            it == _results.end()
                            && _raft->start_offset() > last_offset) {
                              // This is unlikely but possible in theory: if we
                              // can't find the result and the raft start offset
                              // has already advanced, this means that the state
                              // machine advanced by loading the snapshot so we
                              // can't tell how this individual operation ended
                              // up. Respond with timeout to signal that the
                              // result is indeterminate.
                              promise->set_value(errc::timeout);
                              return;
                          }
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
requires(State<T>, ...)
ss::future<> mux_state_machine<T...>::apply(model::record_batch b) {
    return ss::with_gate(_gate, [this, b = std::move(b)]() mutable {
        return _apply_mtx
          .with([this, b = std::move(b)]() mutable {
              return do_apply(std::move(b));
          })
          .then([this] { return on_batch_applied(); });
    });
}

template<typename... T>
requires(State<T>, ...)
ss::future<> mux_state_machine<T...>::do_apply(model::record_batch b) {
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
          _not_handled_batch_types.contains(b.header().type),
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
        if (_persist_last_applied && last_offset > _c->read_last_applied()) {
            ssx::spawn_with_gate(_gate, [this, last_offset] {
                return _c->write_last_applied(last_offset);
            });
        }
    });
}

template<typename... T>
requires(State<T>, ...)
ss::future<> mux_state_machine<T...>::handle_raft_snapshot() {
    // We end up here if the last applied offset of the state machine is less
    // than the raft log start offset (this can happen during startup or when an
    // out-of-date node re-joins the cluster). To continue making progress the
    // state machine must "jump" to an offset that will allow it to continue
    // applying entries from the raft log (i.e. to an offset >=
    // prev(_raft->start_offset())). mux_state_machine achieves this by loading
    // the state from the raft snapshot (the state that was previously saved to
    // the snapshot by mux_state_machine::write_snapshot).

    auto gate_holder = _gate.hold();

    auto snap = co_await _raft->open_snapshot();
    if (!snap) {
        throw std::runtime_error{fmt_with_ctx(
          fmt::format,
          "encountered a gap in the raft log (last_applied: {}, log start "
          "offset: {}), but can't find the snapshot",
          get_last_applied_offset(),
          _raft->start_offset())};
    }

    auto snapshot_offset = snap->metadata.last_included_index;

    auto apply_mtx_holder = co_await _apply_mtx.get_units();

    co_await apply_snapshot(snapshot_offset, snap->reader).finally([&snap] {
        return snap->close();
    });

    auto new_last_applied = std::max(_last_applied, snapshot_offset);
    _last_applied = new_last_applied;

    if (_pending > 0) {
        _new_result.broadcast();
    } else {
        _results.clear();
    }

    if (_persist_last_applied && new_last_applied > _c->read_last_applied()) {
        ssx::spawn_with_gate(_gate, [this, new_last_applied] {
            return _c->write_last_applied(new_last_applied);
        });
    }

    set_next(model::next_offset(new_last_applied));
}

template<typename... T>
requires(State<T>, ...)
ss::future<bool> mux_state_machine<T...>::maybe_write_snapshot() {
    auto gate_holder = _gate.hold();
    auto write_snapshot_mtx_holder = co_await _write_snapshot_mtx.get_units();

    auto apply_mtx_holder = co_await _apply_mtx.get_units();

    if (_raft->last_snapshot_index() >= get_last_applied_offset()) {
        co_return false;
    }
    model::offset offset = get_last_applied_offset();

    auto snapshot_buf = co_await maybe_make_snapshot(
      std::move(apply_mtx_holder));
    if (!snapshot_buf) {
        co_return false;
    }

    co_await _raft->write_snapshot(
      raft::write_snapshot_cfg(offset, std::move(snapshot_buf.value())));
    co_return true;
}

} // namespace raft
