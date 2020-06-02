#include "raft/recovery_stm.h"

#include "model/record_batch_reader.h"
#include "outcome_future_utils.h"
#include "raft/consensus_utils.h"
#include "raft/errc.h"
#include "raft/logger.h"
#include "raft/raftgen_service.h"

#include <seastar/core/future-util.hh>

#include <chrono>

namespace raft {
using namespace std::chrono_literals;

recovery_stm::recovery_stm(
  consensus* p, model::node_id node_id, ss::io_priority_class prio)
  : _ptr(p)
  , _node_id(node_id)
  , _prio(prio)
  , _ctxlog(_ptr->_self, _ptr->group()) {}

ss::future<> recovery_stm::do_one_read() {
    // We have to send all the records that leader have, event those that are
    // beyond commit index, thanks to that after majority have recovered
    // leader can update its commit index
    auto meta = get_follower_meta();
    if (!meta) {
        // stop recovery when node was removed
        _stop_requested = true;
        return ss::make_ready_future<>();
    }
    auto lstats = _ptr->_log.offsets();
    storage::log_reader_config cfg(
      meta.value()->next_index,
      lstats.dirty_offset,
      1,
      // 32KB is a modest estimate. It has good batching and it also prevents an
      // OOM situation where we have a lot of raft groups recovering at the same
      // time and all drawing from memory. If this setting proves difficult,
      // we'll need to throttle with a core-local semaphore
      32 * 1024,
      _prio,
      std::nullopt,
      std::nullopt,
      _ptr->_as);

    // TODO: add timeout of maybe 1minute?
    return _ptr->_log.make_reader(cfg)
      .then([](model::record_batch_reader reader) {
          return model::consume_reader_to_memory(
            std::move(reader), model::no_timeout);
      })
      .then([this](ss::circular_buffer<model::record_batch> batches) {
          // wrap in a foreign core destructor
          vlog(
            _ctxlog.trace,
            "Read {} batches for {} node recovery",
            batches.size(),
            _node_id);
          if (batches.empty()) {
              _stop_requested = true;
              return ss::make_ready_future<>();
          }
          _base_batch_offset = batches.begin()->base_offset();
          _last_batch_offset = batches.back().last_offset();

          auto f_reader = model::make_foreign_record_batch_reader(
            model::make_memory_record_batch_reader(std::move(batches)));

          return replicate(std::move(f_reader));
      });
}

ss::future<> recovery_stm::replicate(model::record_batch_reader&& reader) {
    // collect metadata for append entries request
    // last persisted offset is last_offset of batch before the first one in the
    // reader
    auto prev_log_idx = details::prev_offset(_base_batch_offset);
    // get term for prev_log_idx batch
    auto prev_log_term = _ptr->get_term(prev_log_idx);
    // calculate commit index for follower to update immediately
    auto commit_idx = std::min(_last_batch_offset, _ptr->committed_offset());
    // build request
    append_entries_request r(
      _ptr->self(),
      protocol_metadata{.group = _ptr->group(),
                        .commit_index = commit_idx,
                        .term = _ptr->term(),
                        .prev_log_index = prev_log_idx,
                        .prev_log_term = prev_log_term},
      std::move(reader),
      append_entries_request::flush_after_append::no);

    _ptr->update_node_hbeat_timestamp(_node_id);

    auto seq = _ptr->next_follower_sequence(_node_id);
    return dispatch_append_entries(std::move(r)).then([this, seq](auto r) {
        if (!r) {
            vlog(
              _ctxlog.error,
              "recovery_stm: not replicate entry: {} - {}",
              r,
              r.error().message());
            _stop_requested = true;
        }
        _ptr->process_append_entries_reply(_node_id, r.value(), seq);
        // If request was reordered we have to stop recovery as follower state
        // is not known
        if (seq < _ptr->_fstats.get(_node_id).last_received_seq) {
            _stop_requested = true;
            return;
        }
        // move the follower next index backward if recovery were not
        // successfull
        //
        // Raft paper:
        // If AppendEntries fails because of log inconsistency: decrement
        // nextIndex and retry(§5.3)

        if (r.value().result == append_entries_reply::status::failure) {
            auto meta = get_follower_meta();
            if (!meta) {
                _stop_requested = true;
                return;
            }
            meta.value()->next_index = std::max(
              model::offset(0), details::prev_offset(_base_batch_offset));
            vlog(
              _ctxlog.trace,
              "Move node {} next index {} backward",
              _node_id,
              meta.value()->next_index);
        }
    });
}

clock_type::time_point recovery_stm::append_entries_timeout() {
    return raft::clock_type::now() + _ptr->_replicate_append_timeout;
}

ss::future<result<append_entries_reply>>
recovery_stm::dispatch_append_entries(append_entries_request&& r) {
    _ptr->_probe.recovery_append_request();
    return _ptr->_client_protocol.append_entries(
      _node_id, std::move(r), rpc::client_opts(append_entries_timeout()));
}

bool recovery_stm::is_recovery_finished() {
    if (_ptr->_bg.is_closed()) {
        return true;
    }

    auto meta = get_follower_meta();
    if (!meta) {
        return true;
    }
    auto lstats = _ptr->_log.offsets();
    auto max_offset = lstats.dirty_offset();
    vlog(
      _ctxlog.trace,
      "Recovery status - node {}, match idx: {}, max offset: {}",
      _node_id,
      meta.value()->match_index,
      max_offset);
    return meta.value()->match_index == max_offset // fully caught up
           || _stop_requested                      // stop requested
           || _ptr->_vstate
                != consensus::vote_state::leader; // not a leader anymore
}

ss::future<> recovery_stm::apply() {
    return ss::with_gate(
             _ptr->_bg,
             [this] {
                 return do_one_read().then([this] {
                     return ss::do_until(
                       [this] { return is_recovery_finished(); },
                       [this] { return do_one_read(); });
                 });
             })
      .finally([this] {
          vlog(_ctxlog.trace, "Finished node {} recovery", _node_id);
          auto meta = get_follower_meta();
          if (meta) {
              meta.value()->is_recovering = false;
          }
      });
}

std::optional<follower_index_metadata*> recovery_stm::get_follower_meta() {
    auto it = _ptr->_fstats.find(_node_id);
    if (it == _ptr->_fstats.end()) {
        vlog(_ctxlog.info, "Node {} is not longer in followers list", _node_id);
        return std::nullopt;
    }
    return &it->second;
}

} // namespace raft
