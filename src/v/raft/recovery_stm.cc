// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/recovery_stm.h"

#include "base/outcome_future_utils.h"
#include "bytes/iostream.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "raft/consensus.h"
#include "raft/consensus_utils.h"
#include "raft/errc.h"
#include "raft/logger.h"
#include "raft/raftgen_service.h"
#include "ssx/sformat.h"
#include "storage/snapshot.h"
#include "utils/human.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/with_scheduling_group.hh>

#include <chrono>
#include <optional>
#include <vector>

namespace raft {
using namespace std::chrono_literals;

recovery_stm::recovery_stm(
  consensus* p,
  vnode node_id,
  scheduling_config scheduling,
  recovery_memory_quota& quota)
  : _ptr(p)
  , _node_id(node_id)
  , _term(_ptr->term())
  , _scheduling(scheduling)
  , _ctxlog(
      raftlog,
      ssx::sformat(
        "[follower: {}] [group_id:{}, {}]",
        _node_id,
        _ptr->group(),
        _ptr->ntp()))
  , _memory_quota(quota) {}

ss::future<> recovery_stm::recover() {
    auto meta = get_follower_meta();
    if (!meta) {
        // stop recovery when node was removed
        _stop_requested = true;
        return ss::now();
    }
    auto sg = meta.value()->is_learner ? _scheduling.learner_recovery_sg
                                       : _scheduling.default_sg;
    auto iopc = meta.value()->is_learner ? _scheduling.learner_recovery_iopc
                                         : _scheduling.default_iopc;

    return ss::with_scheduling_group(
      sg, [this, iopc] { return do_recover(iopc); });
}

ss::future<> recovery_stm::do_recover(ss::io_priority_class iopc) {
    // We have to send all the records that leader have, event those that are
    // beyond commit index, thanks to that after majority have recovered
    // leader can update its commit index
    auto meta = get_follower_meta();
    if (!meta) {
        // stop recovery when node was removed
        _stop_requested = true;
        co_return;
    }

    auto lstats = _ptr->_log->offsets();

    // follower last index was already evicted at the leader, use snapshot
    const required_snapshot_type snapshot_needed = get_required_snapshot_type(
      *meta.value());
    if (snapshot_needed != required_snapshot_type::none) {
        co_return co_await install_snapshot(snapshot_needed);
    }

    /**
     * We have to store committed_index before doing read as we perform
     * recovery without holding consensus op_lock. Storing committed index
     * to use in append entries request sent during recovery will guarantee
     * correctness. If we would read data before storing committed index we
     * could incorrectly advance follower commit index with data that
     * were actually incorrect.
     *
     * Consider following scenario:
     *
     * Fiber 1 (f1: recovery) - read some data for follower recovery
     * Fiber 2 (f2: request handling) - received append entries request from
     * new
     *                                  leader.
     * 1. f1: start recovery
     * 2. f1: read batches for recovery
     * 3. f2: received append entries
     * 4. f2: truncated log
     * 5. f2: appended to log
     * 6. f2: updated committed_index
     * 7. f1: read committed offset
     * 8. f1: create append entries request
     * 9. f1: send batches to follower
     * 10. f1: stop recovery - not longer a leader
     *
     * In step 9. follower will receive request that will cause the committed
     * offset to be update event though the batches were truncated on the node
     * which sent the request
     *
     */
    _committed_offset = _ptr->committed_offset();

    auto follower_next_offset = meta.value()->next_index;
    auto follower_committed_match_index = meta.value()->match_committed_index();
    auto is_learner = meta.value()->is_learner;

    meta = get_follower_meta();
    if (!meta) {
        _stop_requested = true;
        co_return;
    }

    if (is_recovery_finished()) {
        _stop_requested = true;
        co_return;
    }

    /**
     * If expected_log_end_offset is indicating that all the requests were
     * already dispatched to the follower wait for append entries responses. The
     * responses will trigger the follower state condition variable and
     * recovery_stm will redo the check if follower still needs to be recovered.
     */
    if (meta.value()->expected_log_end_offset >= lstats.dirty_offset) {
        co_await meta.value()
          ->follower_state_change.wait()
          .handle_exception_type([this](const ss::broken_condition_variable&) {
              _stop_requested = true;
          });
        co_return;
    }
    // acquire read memory:
    auto read_memory_units = co_await _memory_quota.acquire_read_memory();
    auto reader = co_await read_range_for_recovery(
      follower_next_offset, iopc, is_learner, read_memory_units.count());
    // no batches for recovery, do nothing
    if (!reader) {
        co_return;
    }

    if (is_recovery_finished()) {
        _stop_requested = true;
        co_return;
    }

    auto flush = should_flush(follower_committed_match_index);
    if (flush == flush_after_append::yes) {
        _recovered_bytes_since_flush = 0;
    }

    co_await replicate(std::move(*reader), flush, std::move(read_memory_units));
}

flush_after_append
recovery_stm::should_flush(model::offset follower_committed_match_index) const {
    constexpr size_t checkpoint_flush_size = 1_MiB;

    auto lstats = _ptr->_log->offsets();

    /**
     * We request follower to flush only when follower is fully caught
     * up and its match committed offset is smaller than last replicated
     * offset with quorum level. This way when follower flush, leader
     * will be able to update committed_index up to the last offset
     * of last batch replicated with quorum_acks consistency level. Recovery STM
     * works outside of the Raft mutex. It is possible that it read batches that
     * were appended with quorum consistency level but the
     * _last_quorum_replicated_index wasn't yet updated, hence we have to check
     * if last log append was executed with quorum write and if this is true
     * force the flush on follower.
     */

    const bool is_last_batch = _last_batch_offset == lstats.dirty_offset;
    const bool follower_has_batches_to_commit
      = follower_committed_match_index
        < _ptr->_last_quorum_replicated_index_with_flush;

    const bool is_last
      = is_last_batch
        && (follower_has_batches_to_commit || _ptr->_last_write_flushed);

    // Flush every `checkpoint_flush_size` bytes recovered to ensure that the
    // follower's stms can apply batches from the cache rather than disk.
    const bool should_checkpoint_flush = _recovered_bytes_since_flush
                                         >= checkpoint_flush_size;

    return flush_after_append(is_last || should_checkpoint_flush);
}

bool recovery_stm::is_snapshot_at_offset_supported() const {
    return !_ptr->stm_manager().has_value()
           || _ptr->stm_manager()->supports_snapshot_at_offset();
}

recovery_stm::required_snapshot_type recovery_stm::get_required_snapshot_type(
  const follower_index_metadata& follower_metadata) const {
    /**
     * For on demand snapshot we compare next index with follower start offset
     * i.e. next offset of last included in on demand snapshot hence we need to
     * use greater than (not greater than or equal) while the other branch is
     * comparing next index with last included snapshot offset
     */

    if (
      is_snapshot_at_offset_supported() && follower_metadata.is_learner
      && _ptr->get_learner_start_offset()
      && follower_metadata.next_index < *_ptr->get_learner_start_offset()) {
        // current snapshot moved beyond configured learner start offset, we can
        // use current snapshot instead creating a new on demand one
        if (*_ptr->get_learner_start_offset() <= _ptr->last_snapshot_index()) {
            return required_snapshot_type::current;
        }
        return required_snapshot_type::on_demand;
    } else if (follower_metadata.next_index <= _ptr->_last_snapshot_index) {
        return required_snapshot_type::current;
    }
    return required_snapshot_type::none;
}

ss::future<std::optional<model::record_batch_reader>>
recovery_stm::read_range_for_recovery(
  model::offset start_offset,
  ss::io_priority_class iopc,
  bool is_learner,
  size_t read_size) {
    storage::log_reader_config cfg(
      start_offset,
      model::offset::max(),
      1,
      read_size,
      iopc,
      std::nullopt,
      std::nullopt,
      _ptr->_as);

    if (is_learner || _ptr->estimate_recovering_followers() == 1) {
        // skip cache insertion on miss for learners which are throttled and
        // often catching up from the beginning of the log (e.g. new nodes)
        //
        // also skip if there is only one replica recovering as there is no
        // need to add batches to the cache for read-once workloads.
        cfg.skip_batch_cache = true;
    }
    cfg.fill_gaps = true;

    vlog(_ctxlog.trace, "Reading batches, starting from: {}", start_offset);
    auto reader = co_await _ptr->_log->make_reader(cfg);
    try {
        auto gap_filled_batches
          = co_await model::consume_reader_to_fragmented_memory(
            std::move(reader),
            _ptr->_disk_timeout() + model::timeout_clock::now());

        if (gap_filled_batches.empty()) {
            vlog(_ctxlog.trace, "Read no batches for recovery, stopping");
            _stop_requested = true;
            co_return std::nullopt;
        }
        vlog(
          _ctxlog.trace,
          "Read batches in range [{},{}] for recovery",
          gap_filled_batches.front().base_offset(),
          gap_filled_batches.back().last_offset());

        _base_batch_offset = gap_filled_batches.front().base_offset();
        _last_batch_offset = gap_filled_batches.back().last_offset();

        const auto size = std::accumulate(
          gap_filled_batches.cbegin(),
          gap_filled_batches.cend(),
          size_t{0},
          [](size_t acc, const auto& batch) {
              return acc + batch.size_bytes();
          });
        _recovered_bytes_since_flush += size;

        if (is_learner && _ptr->_recovery_throttle) {
            vlog(
              _ctxlog.trace,
              "Requesting throttle for {} bytes, available in throttle: {}",
              size,
              _ptr->_recovery_throttle->get().available());
            co_await _ptr->_recovery_throttle->get()
              .throttle(size, _ptr->_as)
              .handle_exception_type([this](const ss::broken_semaphore&) {
                  vlog(_ctxlog.info, "Recovery throttling has stopped");
              });
        }

        co_return model::make_foreign_fragmented_memory_record_batch_reader(
          std::move(gap_filled_batches));
    } catch (const ss::timed_out_error& e) {
        vlog(
          _ctxlog.error,
          "Timeout reading batches starting from {}. Stopping recovery",
          start_offset);
        _stop_requested = true;
        co_return std::nullopt;
    }
}

ss::future<> recovery_stm::open_current_snapshot() {
    return _ptr->_snapshot_mgr.open_snapshot().then(
      [this](std::optional<storage::snapshot_reader> rdr) {
          if (rdr) {
              _snapshot_reader = std::make_unique<snapshot_reader_t>(
                std::move(*rdr));
              _inflight_snapshot_last_included_index
                = _ptr->_last_snapshot_index;
              return std::get<storage::snapshot_reader>(*_snapshot_reader)
                .get_snapshot_size()
                .then([this](size_t sz) { _snapshot_size = sz; });
          }
          return ss::now();
      });
}

ss::future<> recovery_stm::send_install_snapshot_request() {
    /**
     * If follower is being sent current raft snapshot its content may change.
     * In this case the read_snapshot_chunk will thrown and will force recovery
     * to stop. New snapshot will be delivered with new recovery round.
     */
    return read_snapshot_chunk().then([this](iobuf chunk) mutable {
        auto chunk_size = chunk.size_bytes();
        install_snapshot_request req{
          .target_node_id = _node_id,
          .term = _ptr->term(),
          .group = _ptr->group(),
          .node_id = _ptr->_self,
          .last_included_index = _inflight_snapshot_last_included_index,
          .file_offset = _sent_snapshot_bytes,
          .chunk = std::move(chunk),
          .done = (_sent_snapshot_bytes + chunk_size) == _snapshot_size,
          .dirty_offset = _ptr->dirty_offset()};

        _sent_snapshot_bytes += chunk_size;
        if (is_recovery_finished()) {
            return ss::now();
        }

        if (req.done) {
            auto meta = get_follower_meta();
            if (!meta) {
                // stop recovery when node was removed
                _stop_requested = true;
                return ss::make_ready_future<>();
            }
            (*meta)->expected_log_end_offset
              = _inflight_snapshot_last_included_index;
        }
        vlog(_ctxlog.trace, "sending install_snapshot request: {}", req);
        auto append_guard = _ptr->track_append_inflight(_node_id);
        return _ptr->_client_protocol
          .install_snapshot(
            _node_id.id(),
            std::move(req),
            rpc::client_opts(append_entries_timeout()))
          .then([this](result<install_snapshot_reply> reply) {
              return handle_install_snapshot_reply(
                _ptr->validate_reply_target_node(
                  "install_snapshot", reply, _node_id.id()));
          })
          .finally([append_guard = std::move(append_guard)] {});
    });
}
ss::future<iobuf> recovery_stm::read_snapshot_chunk() {
    return ss::visit(*_snapshot_reader, [](auto& rdr) {
        return read_iobuf_exactly(rdr.input(), 32_KiB);
    });
}

ss::future<> recovery_stm::close_snapshot_reader() {
    return ss::visit(*_snapshot_reader, [](auto& rdr) { return rdr.close(); })
      .then([this] {
          _snapshot_reader.reset();
          _snapshot_size = 0;
          _sent_snapshot_bytes = 0;
      });
}

ss::future<> recovery_stm::handle_install_snapshot_reply(
  result<install_snapshot_reply> reply) {
    vlog(_ctxlog.trace, "received install_snapshot reply: {}", reply);
    // snapshot delivery failed
    if (reply.has_error() || !reply.value().success) {
        // if snapshot delivery failed, stop recovery to update follower state
        // and retry
        _stop_requested = true;
        return close_snapshot_reader();
    }
    if (reply.value().term > _ptr->_term) {
        return close_snapshot_reader().then([this, term = reply.value().term] {
            return _ptr->step_down(term, "snapshot response with greater term");
        });
    }

    // we will send next chunk as a part of recovery loop
    if (reply.value().bytes_stored != _snapshot_size) {
        return ss::now();
    }

    auto meta = get_follower_meta();
    if (!meta) {
        // stop recovery when node was removed
        _stop_requested = true;
        return ss::make_ready_future<>();
    }

    // snapshot received by the follower, continue with recovery
    (*meta)->match_index = _inflight_snapshot_last_included_index;
    (*meta)->next_index = model::next_offset(
      _inflight_snapshot_last_included_index);
    return close_snapshot_reader();
}

ss::future<> recovery_stm::install_snapshot(required_snapshot_type s_type) {
    const auto learner_start_offset = _ptr->get_learner_start_offset();
    // open reader if not yet available
    if (!_snapshot_reader) {
        if (
          s_type == required_snapshot_type::on_demand
          && learner_start_offset > _ptr->start_offset()) {
            co_await take_on_demand_snapshot(
              model::prev_offset(*learner_start_offset));
        } else {
            co_await open_current_snapshot();
        }
    }

    // we are outside of raft operation lock if snapshot isn't yet ready we
    // have to wait for it till next recovery loop
    if (!_snapshot_reader) {
        _stop_requested = true;
        co_return;
    }
    co_return co_await send_install_snapshot_request();
}

ss::future<>
recovery_stm::take_on_demand_snapshot(model::offset last_included_offset) {
    vlog(
      _ctxlog.info,
      "creating on demand snapshot with last included offset: {}, current "
      "leader start offset: {}. Total partition size on leader {}, expected to "
      "transfer to learner: {}",
      last_included_offset,
      _ptr->start_offset(),
      human::bytes(_ptr->log()->size_bytes()),
      human::bytes(_ptr->log()->size_bytes_after_offset(last_included_offset)));

    _inflight_snapshot_last_included_index = last_included_offset;
    // if there is no stm_manager available for the raft group use empty
    // snapshot
    iobuf snapshot_data;

    if (_ptr->stm_manager()) {
        snapshot_data = (co_await _ptr->stm_manager()->take_snapshot(
                           last_included_offset))
                          .data;
    }
    auto cfg = _ptr->_configuration_manager.get(last_included_offset);
    const auto term = _ptr->log()->get_term(last_included_offset);

    if (!cfg || !term) {
        vlog(
          _ctxlog.info,
          "Configuration or term for on demand snapshot offset {} is not "
          "available, stopping recovery",
          last_included_offset);
        _stop_requested = true;
        co_return;
    }

    iobuf snapshot;
    // using snapshot writer to populate all relevant snapshot metadata i.e.
    // header and crc
    storage::snapshot_writer writer(make_iobuf_ref_output_stream(snapshot));

    snapshot_metadata metadata{
      .last_included_index = last_included_offset,
      .last_included_term = *term,
      .latest_configuration = std::move(*cfg),
      .log_start_delta = offset_translator_delta(
        _ptr->log()->offset_delta(model::next_offset(last_included_offset))),
    };

    co_await writer.write_metadata(reflection::to_iobuf(std::move(metadata)));
    co_await write_iobuf_to_output_stream(
      std::move(snapshot_data), writer.output());
    co_await writer.close();

    _snapshot_size = snapshot.size_bytes();
    _snapshot_reader = std::make_unique<snapshot_reader_t>(
      on_demand_snapshot_reader{
        .stream = make_iobuf_input_stream(std::move(snapshot)),
      });
}

ss::future<> recovery_stm::replicate(
  model::record_batch_reader&& reader,
  flush_after_append flush,
  ssx::semaphore_units mem_units) {
    // collect metadata for append entries request
    // last persisted offset is last_offset of batch before the first one in the
    // reader
    auto prev_log_idx = model::prev_offset(_base_batch_offset);
    model::term_id prev_log_term;

    // get term for prev_log_idx batch
    if (prev_log_idx > _ptr->_last_snapshot_index) {
        prev_log_term = *_ptr->_log->get_term(prev_log_idx);
    } else if (prev_log_idx < model::offset(0)) {
        prev_log_term = model::term_id{};
    } else if (prev_log_idx == _ptr->_last_snapshot_index) {
        prev_log_term = _ptr->_last_snapshot_term;
    } else {
        // no entry for prev_log_idx, will fallback to install snapshot on next
        // iteration
        return ss::now();
    }

    // calculate commit index for follower to update immediately
    auto commit_idx = std::min(_last_batch_offset, _committed_offset);
    auto last_visible_idx = std::min(
      _last_batch_offset, _ptr->last_visible_index());

    auto lstats = _ptr->_log->offsets();

    // build request
    append_entries_request r(
      _ptr->self(),
      _node_id,
      protocol_metadata{
        .group = _ptr->group(),
        .commit_index = commit_idx,
        .term = _term,
        .prev_log_index = prev_log_idx,
        .prev_log_term = prev_log_term,
        .last_visible_index = last_visible_idx,
        .dirty_offset = lstats.dirty_offset},
      std::move(reader),
      flush);
    auto meta = get_follower_meta();

    if (!meta) {
        _stop_requested = true;
        return ss::now();
    }
    if (meta.value()->expected_log_end_offset >= _last_batch_offset) {
        vlog(
          _ctxlog.trace,
          "follower expected log end offset is already updated, stopping "
          "recovery. Expected log end offset: {}, recovery range last offset: "
          "{}",
          meta.value()->expected_log_end_offset,
          _last_batch_offset);

        _stop_requested = true;
        return ss::now();
    }
    /**
     * Update follower expected log end. It is equal to the last batch in a set
     * of batches read for this recovery round.
     */
    meta.value()->expected_log_end_offset = _last_batch_offset;
    meta.value()->last_sent_protocol_meta = r.metadata();
    _ptr->update_node_append_timestamp(_node_id);

    auto seq = _ptr->next_follower_sequence(_node_id);
    auto append_guard = _ptr->track_append_inflight(_node_id);

    std::vector<ssx::semaphore_units> units;
    units.push_back(std::move(mem_units));
    return dispatch_append_entries(std::move(r), std::move(units))
      .finally([append_guard = std::move(append_guard)] {})
      .then([this, seq, dirty_offset = lstats.dirty_offset](auto r) {
          if (!r) {
              vlog(
                _ctxlog.warn,
                "recovery append entries error: {}",
                r.error().message());
              _stop_requested = true;
              _ptr->get_probe().recovery_request_error();
          }
          _ptr->process_append_entries_reply(
            _node_id.id(), r.value(), seq, dirty_offset);
          // If follower stats aren't present we have to stop recovery as
          // follower was removed from configuration
          if (!_ptr->_fstats.contains(_node_id)) {
              _stop_requested = true;
              return;
          }
          // If request was reordered we have to stop recovery as follower state
          // is not known
          if (seq < _ptr->_fstats.get(_node_id).last_received_seq) {
              _stop_requested = true;
              return;
          }
          // move the follower next index backward if recovery were not
          // successful
          //
          // Raft paper:
          // If AppendEntries fails because of log inconsistency: decrement
          // nextIndex and retry(§5.3)

          if (r.value().result == reply_result::failure) {
              auto meta = get_follower_meta();
              if (!meta) {
                  _stop_requested = true;
                  return;
              }
              meta.value()->next_index = std::max(
                model::offset(0), model::prev_offset(_base_batch_offset));

              vlog(
                _ctxlog.trace,
                "Move next index {} backward",
                meta.value()->next_index);
          }
      });
}

clock_type::time_point recovery_stm::append_entries_timeout() {
    return raft::clock_type::now() + _ptr->_recovery_append_timeout;
}

ss::future<result<append_entries_reply>> recovery_stm::dispatch_append_entries(
  append_entries_request&& r, std::vector<ssx::semaphore_units> units) {
    _ptr->_probe->recovery_append_request();

    rpc::client_opts opts(append_entries_timeout());
    opts.resource_units = ss::make_foreign(
      ss::make_lw_shared<std::vector<ssx::semaphore_units>>(std::move(units)));

    return _ptr->_client_protocol
      .append_entries(
        _node_id.id(),
        std::move(r),
        std::move(opts),
        _ptr->use_all_serde_append_entries())
      .then([this](result<append_entries_reply> reply) {
          return _ptr->validate_reply_target_node(
            "append_entries_recovery", reply, _node_id.id());
      });
}

bool recovery_stm::is_recovery_finished() {
    /**
     * finish recovery loop when:
     *
     * preliminary checks:
     *  1) consensus is shutting down
     *  2) stop was requested
     *  3) term changed
     *  4) node is not longer a leader
     *  5) follower was deleted
     *
     */
    if (
      _ptr->_as.abort_requested() || _ptr->_bg.is_closed() || _stop_requested
      || _term != _ptr->term() || !_ptr->is_elected_leader()) {
        return true;
    }

    auto meta = get_follower_meta();
    if (!meta) {
        return true;
    }
    auto leader_dirty_offset = _ptr->_log->offsets().dirty_offset;
    return meta.value()->last_dirty_log_index >= leader_dirty_offset
           && meta.value()->match_index == leader_dirty_offset;
}

ss::future<> recovery_stm::apply() {
    return ss::with_gate(
             _ptr->_bg,
             [this] {
                 return recover().then([this] {
                     return ss::do_until(
                       [this] { return is_recovery_finished(); },
                       [this] { return recover(); });
                 });
             })
      .finally([this] {
          vlog(_ctxlog.trace, "Finished recovery");
          auto meta = get_follower_meta();
          if (meta) {
              meta.value()->is_recovering = false;
              meta.value()->recovery_finished.broadcast();
          }
          if (_snapshot_reader != nullptr) {
              return close_snapshot_reader();
          }
          return ss::now();
      });
}

std::optional<follower_index_metadata*> recovery_stm::get_follower_meta() {
    auto it = _ptr->_fstats.find(_node_id);
    if (it == _ptr->_fstats.end()) {
        vlog(_ctxlog.info, "Node is not longer in followers list");
        return std::nullopt;
    }
    return &it->second;
}

} // namespace raft
