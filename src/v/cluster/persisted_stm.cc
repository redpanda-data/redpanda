// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/persisted_stm.h"

#include "bytes/iostream.h"
#include "cluster/logger.h"
#include "raft/consensus.h"
#include "raft/errc.h"
#include "raft/offset_monitor.h"
#include "raft/types.h"
#include "ssx/sformat.h"
#include "storage/record_batch_builder.h"
#include "storage/snapshot.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include <filesystem>

namespace cluster {

persisted_stm::persisted_stm(
  ss::sstring snapshot_mgr_name, ss::logger& logger, raft::consensus* c)
  : raft::state_machine(c, logger, ss::default_priority_class())
  , _c(c)
  , _snapshot_mgr(
      std::filesystem::path(c->log_config().work_directory()),
      snapshot_mgr_name,
      ss::default_priority_class())
  , _log(logger, ssx::sformat("[{} ({})]", _c->ntp(), name())) {}

ss::future<> persisted_stm::remove_persistent_state() {
    _snapshot_size = 0;
    co_await _snapshot_mgr.remove_snapshot();
    co_await _snapshot_mgr.remove_partial_snapshots();
}

ss::future<std::optional<stm_snapshot>> persisted_stm::load_snapshot() {
    auto maybe_reader = co_await _snapshot_mgr.open_snapshot();
    if (!maybe_reader) {
        co_return std::nullopt;
    }

    storage::snapshot_reader& reader = *maybe_reader;
    iobuf meta_buf = co_await reader.read_metadata();
    iobuf_parser meta_parser(std::move(meta_buf));

    auto version = reflection::adl<int8_t>{}.from(meta_parser);
    vassert(
      version == snapshot_version || version == snapshot_version_v0,
      "[{} ({})] Unsupported persisted_stm snapshot_version {}",
      _c->ntp(),
      name(),
      version);

    if (version == snapshot_version_v0) {
        vlog(
          _log.warn,
          "Skipping snapshot {} due to old format",
          _snapshot_mgr.snapshot_path());

        // can't load old format of the snapshot, since snapshot is missing
        // it will be reconstructed by replaying the log
        co_await reader.close();
        co_return std::nullopt;
    }

    stm_snapshot snapshot;
    snapshot.header.offset = model::offset(
      reflection::adl<int64_t>{}.from(meta_parser));
    snapshot.header.version = reflection::adl<int8_t>{}.from(meta_parser);
    snapshot.header.snapshot_size = reflection::adl<int32_t>{}.from(
      meta_parser);
    snapshot.data = co_await read_iobuf_exactly(
      reader.input(), snapshot.header.snapshot_size);

    _snapshot_size = co_await reader.get_snapshot_size();
    co_await reader.close();
    co_await _snapshot_mgr.remove_partial_snapshots();

    co_return snapshot;
}

ss::future<> persisted_stm::wait_for_snapshot_hydrated() {
    auto f = ss::now();
    if (unlikely(!_resolved_when_snapshot_hydrated.available())) {
        f = _resolved_when_snapshot_hydrated.get_shared_future();
    }
    return f;
}

ss::future<> persisted_stm::persist_snapshot(
  storage::simple_snapshot_manager& snapshot_mgr, stm_snapshot&& snapshot) {
    iobuf data_size_buf;

    int8_t version = snapshot_version;
    int64_t offset = snapshot.header.offset();
    int8_t data_version = snapshot.header.version;
    int32_t data_size = snapshot.header.snapshot_size;
    reflection::serialize(
      data_size_buf, version, offset, data_version, data_size);

    return snapshot_mgr.start_snapshot().then(
      [&snapshot_mgr,
       snapshot = std::move(snapshot),
       data_size_buf = std::move(data_size_buf)](
        storage::snapshot_writer writer) mutable {
          return ss::do_with(
            std::move(writer),
            [&snapshot_mgr,
             snapshot = std::move(snapshot),
             data_size_buf = std::move(data_size_buf)](
              storage::snapshot_writer& writer) mutable {
                return writer.write_metadata(std::move(data_size_buf))
                  .then([&writer, snapshot = std::move(snapshot)]() mutable {
                      return write_iobuf_to_output_stream(
                        std::move(snapshot.data), writer.output());
                  })
                  .finally([&writer] { return writer.close(); })
                  .then([&snapshot_mgr, &writer] {
                      return snapshot_mgr.finish_snapshot(writer);
                  });
            });
      });
}

ss::future<> persisted_stm::persist_snapshot(stm_snapshot&& snapshot) {
    return persist_snapshot(_snapshot_mgr, std::move(snapshot)).then([this] {
        return _snapshot_mgr.get_snapshot_size().then(
          [this](uint64_t size) { _snapshot_size = size; });
    });
}

ss::future<> persisted_stm::do_make_snapshot() {
    auto snapshot = co_await take_snapshot();
    auto offset = snapshot.header.offset;

    co_await persist_snapshot(std::move(snapshot));
    _last_snapshot_offset = std::max(_last_snapshot_offset, offset);
}

void persisted_stm::make_snapshot_in_background() {
    ssx::spawn_with_gate(_gate, [this] { return make_snapshot(); });
}

ss::future<> persisted_stm::make_snapshot() {
    return _op_lock.with([this]() {
        auto f = wait_for_snapshot_hydrated();
        return f.then([this] { return do_make_snapshot(); });
    });
}

uint64_t persisted_stm::get_snapshot_size() const { return _snapshot_size; }

ss::future<>
persisted_stm::ensure_snapshot_exists(model::offset target_offset) {
    return _op_lock.with([this, target_offset]() {
        auto f = wait_for_snapshot_hydrated();

        return f.then([this, target_offset] {
            if (target_offset <= _last_snapshot_offset) {
                return ss::now();
            }
            return wait(target_offset, model::no_timeout)
              .then([this, target_offset]() {
                  vassert(
                    target_offset <= _insync_offset,
                    "[{} ({})]  after we waited for target_offset ({}) "
                    "_insync_offset "
                    "({}) should have matched it or bypassed",
                    _c->ntp(),
                    name(),
                    target_offset,
                    _insync_offset);
                  return do_make_snapshot();
              });
        });
    });
}

model::offset persisted_stm::max_collectible_offset() {
    return model::offset::max();
}

ss::future<fragmented_vector<model::tx_range>>
persisted_stm::aborted_tx_ranges(model::offset, model::offset) {
    return ss::make_ready_future<fragmented_vector<model::tx_range>>();
}

ss::future<> persisted_stm::wait_offset_committed(
  model::timeout_clock::duration timeout,
  model::offset offset,
  model::term_id term) {
    auto stop_cond = [this, offset, term] {
        return _c->committed_offset() >= offset || _c->term() > term;
    };

    return _c->commit_index_updated().wait(timeout, stop_cond);
}

ss::future<bool> persisted_stm::do_sync(
  model::timeout_clock::duration timeout,
  model::offset offset,
  model::term_id term) {
    const auto committed = _c->committed_offset();
    const auto ntp = _c->ntp();
    _c->events().notify_commit_index();

    if (offset > committed) {
        try {
            co_await wait_offset_committed(timeout, offset, term);
        } catch (const ss::broken_condition_variable&) {
            co_return false;
        } catch (const ss::gate_closed_exception&) {
            co_return false;
        } catch (const ss::abort_requested_exception&) {
            co_return false;
        } catch (const ss::condition_variable_timed_out&) {
            co_return false;
        } catch (...) {
            vlog(
              _log.error,
              "sync error: wait_offset_committed failed with {}; "
              "offsets: dirty={}, committed={}",
              std::current_exception(),
              offset,
              committed);
            co_return false;
        }
    } else {
        offset = committed;
    }

    if (_c->term() == term) {
        try {
            co_await wait(offset, model::timeout_clock::now() + timeout);
        } catch (const ss::broken_condition_variable&) {
            co_return false;
        } catch (const ss::gate_closed_exception&) {
            co_return false;
        } catch (const ss::abort_requested_exception&) {
            co_return false;
        } catch (const ss::condition_variable_timed_out&) {
            co_return false;
        } catch (const ss::timed_out_error&) {
            vlog(
              _log.warn,
              "sync timeout: waiting for offset={}; committed "
              "offset={}",
              offset,
              committed);
            co_return false;
        } catch (...) {
            vlog(
              _log.error,
              "sync error: waiting for offset={} failed with {}; committed "
              "offset={};",
              offset,
              std::current_exception(),
              committed);
            co_return false;
        }
        if (_c->term() == term) {
            _insync_term = term;
            co_return true;
        }
        // we lost leadership during waiting
    }

    co_return false;
}

ss::future<bool> persisted_stm::sync(model::timeout_clock::duration timeout) {
    auto term = _c->term();
    if (!_c->is_leader()) {
        co_return false;
    }
    if (_insync_term == term) {
        co_return true;
    }
    if (_is_catching_up) {
        auto deadline = model::timeout_clock::now() + timeout;
        auto sync_waiter = ss::make_lw_shared<expiring_promise<bool>>();
        _sync_waiters.push_back(sync_waiter);
        co_return co_await sync_waiter->get_future_with_timeout(
          deadline, [] { return false; });
    }
    _is_catching_up = true;

    // To guarantee that we have caught up with all records written by previous
    // leaders, we choose the last offset before the start of the current term
    // as the sync offset. Because the leader replicates the configuration batch
    // at the start of the term, this offset is guaranteed to be less than
    // committed index, so we won't need any additional flushes even if the
    // client produces with acks=1.
    model::offset sync_offset;
    auto log_offsets = _c->log().offsets();
    if (log_offsets.dirty_offset_term == term) {
        if (log_offsets.last_term_start_offset > model::offset{0}) {
            sync_offset = log_offsets.last_term_start_offset - model::offset{1};
        } else {
            sync_offset = model::offset{};
        }
    } else {
        // We haven't been able to append the configuration batch at the start
        // of the term yet.
        sync_offset = log_offsets.dirty_offset;
    }
    auto is_synced = co_await do_sync(timeout, sync_offset, term);

    _is_catching_up = false;
    for (auto& sync_waiter : _sync_waiters) {
        sync_waiter->set_value(is_synced);
    }
    _sync_waiters.clear();
    co_return is_synced;
}

ss::future<bool> persisted_stm::wait_no_throw(
  model::offset offset,
  model::timeout_clock::time_point deadline,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    return wait(offset, deadline, as)
      .then([] { return true; })
      .handle_exception_type([](const ss::abort_requested_exception&) {
          // Shutting down
          return false;
      })
      .handle_exception_type(
        [this, offset, ntp = _c->ntp()](const ss::timed_out_error&) {
            vlog(_log.warn, "timed out while waiting for offset: {}", offset);
            return false;
        })
      .handle_exception([this, offset, ntp = _c->ntp()](std::exception_ptr e) {
          vlog(
            _log.error,
            "An error {} happened during waiting for offset: {}",
            e,
            offset);
          return false;
      });
}

ss::future<> persisted_stm::start() {
    std::optional<stm_snapshot> maybe_snapshot;
    try {
        maybe_snapshot = co_await load_snapshot();
    } catch (...) {
        vassert(
          false,
          "[[{}] ({})]Can't load snapshot from '{}'. Got error: {}",
          _c->ntp(),
          name(),
          _snapshot_mgr.snapshot_path(),
          std::current_exception());
    }

    if (maybe_snapshot) {
        stm_snapshot& snapshot = *maybe_snapshot;

        auto next_offset = model::next_offset(snapshot.header.offset);
        if (next_offset >= _c->start_offset()) {
            vlog(
              _log.debug,
              "start with applied snapshot, set_next {}",
              next_offset);
            co_await apply_snapshot(snapshot.header, std::move(snapshot.data));
            set_next(next_offset);
        } else {
            // This can happen on an out-of-date replica that re-joins the group
            // after other replicas have already evicted logs to some offset
            // greater than snapshot.header.offset. We print a warning and
            // continue. The stm will later detect this situation and deal with
            // it in the apply fiber by calling handle_eviction.
            vlog(
              _log.warn,
              "Skipping snapshot {} since it's out of sync with the log",
              _snapshot_mgr.snapshot_path());
            vlog(
              _log.debug,
              "start with non-applied snapshot, set_next {}",
              next_offset);
            _insync_offset = model::prev_offset(next_offset);
            set_next(next_offset);
        }

        _resolved_when_snapshot_hydrated.set_value();
    } else {
        auto offset = _c->start_offset();
        vlog(_log.debug, "start without snapshot, maybe set_next {}", offset);

        if (offset >= model::offset(0)) {
            _insync_offset = model::prev_offset(offset);
            set_next(offset);
        }
        _resolved_when_snapshot_hydrated.set_value();
    }
    co_await state_machine::start();
}

} // namespace cluster
