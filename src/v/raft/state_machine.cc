// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/state_machine.h"

#include "base/likely.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "raft/consensus.h"
#include "ssx/future-util.h"
#include "storage/log.h"
#include "storage/record_batch_builder.h"

#include <exception>

namespace raft {

state_machine::state_machine(
  consensus* raft, ss::logger& log, ss::io_priority_class io_prio)
  : _raft(raft)
  , _io_prio(io_prio)
  , _log(log)
  , _next(0)
  , _bootstrap_last_applied(_raft->read_last_applied()) {}

ss::future<> state_machine::start() {
    vlog(_log.debug, "Starting state machine for ntp={}", _raft->ntp());
    ssx::spawn_with_gate(_gate, [this] {
        return ss::do_until(
          [this] { return _gate.is_closed(); }, [this] { return apply(); });
    });
    return ss::now();
}

void state_machine::set_next(model::offset offset) {
    _next = offset;
    _waiters.notify(model::prev_offset(offset));
}

ss::future<> state_machine::handle_raft_snapshot() {
    vlog(
      _log.warn,
      "{} state_machine should support handle_eviction",
      _raft->ntp());
    return ss::now();
}

ss::future<> state_machine::stop() {
    vlog(_log.debug, "Asked to stop state_machine {}", _raft->ntp());
    _waiters.stop();
    _as.request_abort();
    return _gate.close().then([this] {
        vlog(_log.debug, "state_machine is stopped {}", _raft->ntp());
    });
}

ss::future<> state_machine::wait(
  model::offset offset,
  model::timeout_clock::time_point timeout,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    return ss::with_gate(_gate, [this, timeout, offset, as] {
        return _waiters.wait(offset, timeout, as);
    });
}

model::offset state_machine::bootstrap_last_applied() const {
    return _bootstrap_last_applied;
}

state_machine::batch_applicator::batch_applicator(state_machine* machine)
  : _machine(machine) {}

ss::future<ss::stop_iteration>
state_machine::batch_applicator::operator()(model::record_batch batch) {
    if (_machine->stop_batch_applicator()) {
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::yes);
    }

    const auto committed_offset = _machine->_raft->committed_offset();
    vassert(
      batch.last_offset() <= committed_offset,
      "Can not apply not committed batches to stm [{}]. Applied batch "
      "header: {}, raft committed offset: {}",
      _machine->_raft->ntp(),
      batch.header(),
      committed_offset);

    auto last_offset = batch.last_offset();
    return _machine->apply(std::move(batch)).then([this, last_offset] {
        _machine->_next = model::next_offset(last_offset);
        _machine->_waiters.notify(last_offset);
        return ss::stop_iteration::no;
    });
}

bool state_machine::stop_batch_applicator() { return _gate.is_closed(); }

model::record_batch_reader make_checkpoint() {
    storage::record_batch_builder builder(
      model::record_batch_type::checkpoint, model::offset(0));
    builder.add_raw_kv(iobuf(), iobuf());
    return model::make_memory_record_batch_reader(std::move(builder).build());
}

// FIXME: implement linearizable reads in a way similar to Logcabin
// implementation. For now we replicate single record to make sure leader is up
// to date
ss::future<result<replicate_result>> state_machine::quorum_write_empty_batch(
  model::timeout_clock::time_point timeout) {
    using ret_t = result<replicate_result>;
    // replicate checkpoint batch
    return _raft
      ->replicate(
        make_checkpoint(), replicate_options(consistency_level::quorum_ack))
      .then([this, timeout](ret_t r) {
          if (!r) {
              return ss::make_ready_future<ret_t>(r);
          }
          return wait(r.value().last_offset, timeout).then([r]() mutable {
              return r;
          });
      });
}
ss::future<> state_machine::maybe_apply_raft_snapshot() {
    // a loop here is required as install snapshot request may be processed by
    // Raft while handling the other snapshot. In this case a new snapshot
    // should be applied to the STM.

    while (_next < _raft->start_offset()) {
        try {
            co_await handle_raft_snapshot();
        } catch (...) {
            const auto& e = std::current_exception();
            if (!ssx::is_shutdown_exception(e)) {
                vlog(_log.error, "Error applying Raft snapshot - {}", e);
            }
            std::rethrow_exception(e);
        }
    }
}
ss::future<> state_machine::apply() {
    // wait until consensus commit index is >= _next
    return _raft->events()
      .wait(_next, model::no_timeout, _as)
      .then([this] {
          return maybe_apply_raft_snapshot().then([this] {
              /**
               * Raft make_reader method allows callers reading up to
               * last_visible index. In order to make the STMs safe and working
               * with the raft semantics (i.e. what is applied must be comitted)
               * we have to limit reading to the committed offset.
               */
              storage::log_reader_config config(
                _next, _raft->committed_offset(), _io_prio);
              return _raft->make_reader(config);
          });
      })
      .then([this](model::record_batch_reader reader) {
          // apply each batch to the state machine
          return std::move(reader).consume(
            batch_applicator(this), model::no_timeout);
      })
      .handle_exception_type([this](const ss::timed_out_error&) {
          // This is a safe retry, but if it happens in tests we're interested
          // in seeing what happened in the debug log
          vlog(
            _log.debug,
            "Timeout in state_machine::apply on ntp {}",
            _raft->ntp());
      })
      .handle_exception([this](const std::exception_ptr& e) {
          // do not log shutdown exceptions not to pollute logs with irrelevant
          // errors
          if (ssx::is_shutdown_exception(e)) {
              return;
          }

          vlog(
            _log.error,
            "State machine for ntp={} caught exception {}",
            _raft->ntp(),
            e);
      });
}

ss::future<> state_machine::write_last_applied(model::offset o) {
    return _raft->write_last_applied(o);
}

ss::future<result<model::offset>> state_machine::insert_linearizable_barrier(
  model::timeout_clock::time_point timeout) {
    /**
     * Inject leader barrier and wait until returned offset is applied
     */

    return _raft->linearizable_barrier(timeout).then(
      [this, timeout](result<model::offset> r) {
          if (!r) {
              return ss::make_ready_future<result<model::offset>>(r.error());
          }

          // wait for the returned offset to be applied
          return wait(r.value(), timeout).then([r] {
              return result<model::offset>(r.value());
          });
      });
}

ss::future<model::offset> state_machine::bootstrap_committed_offset() {
    /// It is useful for some STMs to know what the committed offset is so they
    /// may do things like block until they have consumed all known committed
    /// records. To achieve this, this method waits on offset 0, so on the first
    /// call to `event_manager::notify_commit_index`, it is known that the
    /// committed offset is in an initialized state.
    return _raft->events()
      .wait(model::offset(0), model::no_timeout, _as)
      .then([this] { return _raft->committed_offset(); });
}

} // namespace raft
