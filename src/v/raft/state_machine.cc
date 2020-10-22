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
    vlog(_log.info, "Starting state machine");
    (void)ss::with_gate(_gate, [this] {
        return ss::do_until(
          [this] { return _gate.is_closed(); }, [this] { return apply(); });
    });
    return ss::now();
}

ss::future<> state_machine::stop() {
    _waiters.stop();
    _as.request_abort();
    return _gate.close();
}

ss::future<> state_machine::wait(
  model::offset offset, model::timeout_clock::time_point timeout) {
    return ss::with_gate(_gate, [this, timeout, offset] {
        return _waiters.wait(offset, timeout, std::nullopt);
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
        _last_applied = last_offset;
        _machine->_waiters.notify(_last_applied);
        return ss::stop_iteration::no;
    });
}

bool state_machine::stop_batch_applicator() { return _gate.is_closed(); }

model::record_batch_reader make_checkpoint() {
    storage::record_batch_builder builder(
      state_machine::checkpoint_batch_type, model::offset(0));
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
          // build a reader for log range [_next, +inf).
          storage::log_reader_config config(
            _next, model::model_limits<model::offset>::max(), _io_prio);
          return _raft->make_reader(config);
      })
      .then([this](model::record_batch_reader reader) {
          // apply each batch to the state machine
          return std::move(reader)
            .consume(batch_applicator(this), model::no_timeout)
            .then([this](model::offset last_applied) {
                if (last_applied >= model::offset(0)) {
                    _next = last_applied + model::offset(1);
                }
            });
      })
      .handle_exception([this](const std::exception_ptr& e) {
          vlog(_log.info, "State machine handles {}", e);
      });
}

ss::future<> state_machine::write_last_applied(model::offset o) {
    return _raft->write_last_applied(o);
}

} // namespace raft
