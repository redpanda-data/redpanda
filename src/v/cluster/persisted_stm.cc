// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/persisted_stm.h"

#include "cluster/logger.h"
#include "raft/errc.h"
#include "raft/types.h"
#include "storage/record_batch_builder.h"

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
  , _log(logger)
  , _snapshot_recovery_policy(
      config::shard_local_cfg().stm_snapshot_recovery_policy.value()) {}

ss::future<> persisted_stm::hydrate_snapshot(storage::snapshot_reader& reader) {
    return reader.read_metadata()
      .then([this, &reader](iobuf meta_buf) {
          iobuf_parser meta_parser(std::move(meta_buf));

          auto version = reflection::adl<int8_t>{}.from(meta_parser);
          vassert(
            version == snapshot_version,
            "Only support persisted_snapshot_version {} but got {}",
            snapshot_version,
            version);

          stm_snapshot_header hdr;
          hdr.version = reflection::adl<int8_t>{}.from(meta_parser);
          hdr.snapshot_size = reflection::adl<int32_t>{}.from(meta_parser);

          return read_iobuf_exactly(reader.input(), hdr.snapshot_size)
            .then([this, hdr](iobuf data_buf) {
                return load_snapshot(hdr, std::move(data_buf));
            })
            .then(
              [this]() { return _snapshot_mgr.remove_partial_snapshots(); });
      })
      .handle_exception([this](std::exception_ptr e) {
          vlog(
            clusterlog.error,
            "Can't hydrate from {} error {}",
            _snapshot_mgr.snapshot_path(),
            e);
          if (
            _snapshot_recovery_policy
            == model::violation_recovery_policy::crash) {
              vassert(
                false,
                "Can't hydrate from {} error {}",
                _snapshot_mgr.snapshot_path(),
                e);
          } else if (
            _snapshot_recovery_policy
            == model::violation_recovery_policy::best_effort) {
              vlog(
                clusterlog.warn,
                "Rolling back to an empty snapshot (potential consistency "
                "violation)");
          } else {
              vassert(
                false, "Unknown recovery policy {}", _snapshot_recovery_policy);
          }
      });
}

ss::future<> persisted_stm::wait_for_snapshot_hydrated() {
    auto f = ss::now();
    if (unlikely(!_resolved_when_snapshot_hydrated.available())) {
        f = _resolved_when_snapshot_hydrated.get_shared_future();
    }
    return f;
}

ss::future<> persisted_stm::persist_snapshot(stm_snapshot&& snapshot) {
    iobuf data_size_buf;
    reflection::serialize(
      data_size_buf,
      snapshot_version,
      snapshot.header.version,
      snapshot.header.snapshot_size);

    return _snapshot_mgr.start_snapshot().then(
      [this,
       snapshot = std::move(snapshot),
       data_size_buf = std::move(data_size_buf)](
        storage::snapshot_writer writer) mutable {
          return ss::do_with(
            std::move(writer),
            [this,
             snapshot = std::move(snapshot),
             data_size_buf = std::move(data_size_buf)](
              storage::snapshot_writer& writer) mutable {
                return writer.write_metadata(std::move(data_size_buf))
                  .then([&writer, snapshot = std::move(snapshot)]() mutable {
                      return write_iobuf_to_output_stream(
                        std::move(snapshot.data), writer.output());
                  })
                  .finally([&writer] { return writer.close(); })
                  .then([this, &writer] {
                      return _snapshot_mgr.finish_snapshot(writer);
                  });
            });
      });
}

ss::future<> persisted_stm::do_make_snapshot() {
    auto snapshot = co_await take_snapshot();
    auto offset = snapshot.offset;

    co_await persist_snapshot(std::move(snapshot));
    _last_snapshot_offset = std::max(_last_snapshot_offset, offset);
}

ss::future<> persisted_stm::make_snapshot() {
    return _op_lock.with([this]() {
        auto f = wait_for_snapshot_hydrated();
        return f.then([this] { return do_make_snapshot(); });
    });
}

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
                    "after we waited for target_offset ({}) _insync_offset "
                    "({}) should have matched it or bypassed",
                    target_offset,
                    _insync_offset);
                  return do_make_snapshot();
              });
        });
    });
}

ss::future<bool> persisted_stm::sync(model::timeout_clock::duration timeout) {
    if (!_c->is_leader()) {
        return ss::make_ready_future<bool>(false);
    }
    if (_insync_term == _c->term()) {
        return ss::make_ready_future<bool>(true);
    }
    if (_is_catching_up) {
        auto deadline = model::timeout_clock::now() + timeout;
        auto sync_waiter = ss::make_lw_shared<expiring_promise<bool>>();
        _sync_waiters.push_back(sync_waiter);
        return sync_waiter->get_future_with_timeout(
          deadline, [] { return false; });
    }
    _is_catching_up = true;
    auto term = _c->term();
    return quorum_write_empty_batch(model::timeout_clock::now() + timeout)
      .then_wrapped([this, term](ss::future<result<raft::replicate_result>> f) {
          auto is_synced = false;
          try {
              is_synced = (bool)f.get();
          } catch (...) {
              vlog(
                clusterlog.error,
                "Error during writing a checkpoint batch: {}",
                std::current_exception());
          }
          is_synced = is_synced && (term == _c->term());
          if (is_synced) {
              _insync_term = term;
          }
          for (auto& sync_waiter : _sync_waiters) {
              sync_waiter->set_value(is_synced);
          }
          _sync_waiters.clear();
          return is_synced;
      });
}

ss::future<bool> persisted_stm::wait_no_throw(
  model::offset offset, model::timeout_clock::duration timeout) {
    auto deadline = model::timeout_clock::now() + timeout;
    return wait(offset, deadline)
      .then([] { return true; })
      .handle_exception([offset](std::exception_ptr e) {
          vlog(
            clusterlog.error,
            "An error {} happened during waiting for offset:{}",
            e,
            offset);
          return false;
      });
}

ss::future<> persisted_stm::start() {
    return _snapshot_mgr.open_snapshot().then(
      [this](std::optional<storage::snapshot_reader> reader) {
          auto f = ss::now();
          if (reader) {
              f = ss::do_with(
                std::move(*reader), [this](storage::snapshot_reader& reader) {
                    return hydrate_snapshot(reader).then([this, &reader] {
                        auto offset = std::max(
                          _insync_offset, _c->start_offset());
                        if (offset >= model::offset(0)) {
                            set_next(offset);
                        }
                        _resolved_when_snapshot_hydrated.set_value();
                        return reader.close();
                    });
                });
          } else {
              auto offset = _c->start_offset();
              if (offset >= model::offset(0)) {
                  set_next(offset);
              }
              _resolved_when_snapshot_hydrated.set_value();
          }

          return f.then([this]() { return state_machine::start(); })
            .then([this]() {
                auto offset = _c->meta().commit_index;
                if (offset >= model::offset(0)) {
                    (void)ss::with_gate(_gate, [this, offset] {
                        // saving a snapshot after catchup with the tip of the
                        // log
                        return ensure_snapshot_exists(offset);
                    });
                }
            });
      });
}

} // namespace cluster
