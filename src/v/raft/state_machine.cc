#include "raft/state_machine.h"

#include "raft/consensus.h"
#include "storage/log.h"

namespace raft {

state_machine::state_machine(ss::logger& log, ss::io_priority_class io_prio)
  : _io_prio(io_prio)
  , _log(log)
  , _next(0) {}

ss::future<> state_machine::start(consensus* raft) {
    vlog(_log.info, "Starting state machine");
    _raft = raft;
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
    return _waiters.wait(offset, timeout, std::nullopt);
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

} // namespace raft
