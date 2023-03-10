// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/state_machine.h"

#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "raft/consensus.h"
#include "storage/log.h"
#include "storage/record_batch_builder.h"

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

ss::future<> state_machine::handle_eviction() {
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

ss::future<> state_machine::apply() {
    // wait until consensus commit index is >= _next
    return _raft->events()
      .wait(_next, model::no_timeout, _as)
      .then([this] {
          auto f = ss::now();
          if (_next < _raft->start_offset()) {
              f = handle_eviction();
          }
          return f.then([this] {
              // build a reader for log range [_next, +inf).
              storage::log_reader_config config(
                _next, model::model_limits<model::offset>::max(), _io_prio);
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
      .handle_exception_type([](const ss::abort_requested_exception&) {})
      .handle_exception_type([](const ss::gate_closed_exception&) {})
      .handle_exception([this](const std::exception_ptr& e) {
          vlog(
            _log.info,
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
    return ss::with_timeout(timeout, _raft->linearizable_barrier())
      .handle_exception_type([](const ss::timed_out_error&) {
          return result<model::offset>(errc::timeout);
      });
}

} // namespace raft
