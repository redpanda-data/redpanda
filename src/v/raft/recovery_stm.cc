#include "raft/recovery_stm.h"

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
  , _ctxlog(_ptr->_self, raft::group_id(_ptr->_meta.group)) {}

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

    storage::log_reader_config cfg(
      meta.value()->next_index, model::offset(_ptr->_log.max_offset()), _prio);

    // TODO: add timeout of maybe 1minute?
    return model::consume_reader_to_memory(
             _ptr->_log.make_reader(cfg), model::no_timeout)
      .then([this](ss::circular_buffer<model::record_batch> batches) {
          // wrap in a foreign core destructor
          _ctxlog.trace(
            "Read {} batches for {} node recovery", batches.size(), _node_id);
          if (batches.empty()) {
              return ss::make_ready_future<
                std::vector<model::record_batch_reader>>();
          }
          _base_batch_offset = batches.begin()->base_offset();
          _last_batch_offset = batches.back().last_offset();
          return details::foreign_share_n(
            model::make_memory_record_batch_reader(std::move(batches)), 1);
      })
      .then([this](std::vector<model::record_batch_reader> readers) {
          // Stall recovery, ignore it
          if (readers.empty()) {
              return ss::make_ready_future<>();
          }
          return replicate(std::move(readers.back()));
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
    auto commit_idx = std::min(
      _last_batch_offset(),
      static_cast<model::offset::type>(_ptr->_meta.commit_index));
    // build request
    auto r = append_entries_request{
      .node_id = _ptr->self(),
      .meta = protocol_metadata{.group = _ptr->_meta.group,
                                .commit_index = commit_idx,
                                .term = _ptr->_meta.term,
                                .prev_log_index = prev_log_idx(),
                                .prev_log_term = prev_log_term()},
      .batches = std::move(reader)};

    _ptr->update_node_hbeat_timestamp(_node_id);

    return dispatch_append_entries(std::move(r)).then([this](auto r) {
        if (!r) {
            _ctxlog.error(
              "recovery_stm: not replicate entry: {} - {}",
              r,
              r.error().message());
            _stop_requested = true;
        }

        _ptr->process_append_reply(_node_id, r.value());
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
            _ctxlog.trace(
              "Move node {} next index {} backward",
              _node_id,
              meta.value()->next_index);
        }
    });
}

ss::future<result<append_entries_reply>>
recovery_stm::dispatch_append_entries(append_entries_request&& r) {
    return _ptr->_client_protocol.append_entries(
      _node_id, std::move(r), raft::clock_type::now() + 1s);
}

bool recovery_stm::is_recovery_finished() {
    auto meta = get_follower_meta();
    if (!meta) {
        return true;
    }
    auto max_offset = _ptr->_log.max_offset();
    _ctxlog.trace(
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
    return ss::do_until(
             [this] { return is_recovery_finished(); },
             [this] { return do_one_read(); })
      .finally([this] {
          _ctxlog.trace("Finished node {} recovery", _node_id);
          auto meta = get_follower_meta();
          if (meta) {
              meta.value()->is_recovering = false;
          }
      });
}

std::optional<follower_index_metadata*> recovery_stm::get_follower_meta() {
    auto it = _ptr->_fstats.find(_node_id);
    if (it == _ptr->_fstats.end()) {
        _ctxlog.info("Node {} is not longer in followers list", _node_id);
        return std::nullopt;
    }
    return &it->second;
}

} // namespace raft
