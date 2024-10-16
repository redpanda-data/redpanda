// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/persisted_stm.h"

#include "bytes/iostream.h"
#include "raft/consensus.h"
#include "raft/state_machine_base.h"
#include "ssx/sformat.h"
#include "storage/api.h"
#include "storage/kvstore.h"
#include "storage/snapshot.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include <filesystem>
namespace raft {
namespace {

std::optional<raft::stm_snapshot_header> read_snapshot_header(
  iobuf_parser& parser, const model::ntp& ntp, const ss::sstring& name) {
    auto version = reflection::adl<int8_t>{}.from(parser);
    vassert(
      version == raft::stm_snapshot_version
        || version == raft::stm_snapshot_version_v0,
      "[{} ({})] Unsupported persisted_stm snapshot_version {}",
      ntp,
      name,
      version);

    if (version == raft::stm_snapshot_version_v0) {
        return std::nullopt;
    }

    raft::stm_snapshot_header header;
    header.offset = model::offset(reflection::adl<int64_t>{}.from(parser));
    header.version = reflection::adl<int8_t>{}.from(parser);
    header.snapshot_size = reflection::adl<int32_t>{}.from(parser);
    return header;
}

ss::sstring
stm_snapshot_key(const ss::sstring& snapshot_name, const model::ntp& ntp) {
    return ssx::sformat("{}/{}", snapshot_name, ntp);
}

} // namespace

template<typename BaseT, supported_stm_snapshot T>
template<typename... Args>
persisted_stm_base<BaseT, T>::persisted_stm_base(
  ss::sstring snapshot_mgr_name,
  ss::logger& logger,
  raft::consensus* c,
  Args&&... args)
  : _raft(c)
  , _log(logger, ssx::sformat("[{} ({})]", _raft->ntp(), snapshot_mgr_name))
  , _snapshot_backend(snapshot_mgr_name, _log, c, std::forward<Args>(args)...) {
}

template<typename BaseT, supported_stm_snapshot T>
ss::future<> persisted_stm_base<BaseT, T>::apply(
  const model::record_batch& b, const ssx::semaphore_units&) {
    return do_apply(b);
}

template<typename BaseT, supported_stm_snapshot T>
ss::future<std::optional<stm_snapshot>>
persisted_stm_base<BaseT, T>::load_local_snapshot() {
    return _snapshot_backend.load_snapshot();
}
template<typename BaseT, supported_stm_snapshot T>
ss::future<> persisted_stm_base<BaseT, T>::stop() {
    co_await raft::state_machine_base::stop();
    co_await _gate.close();
}

template<typename BaseT, supported_stm_snapshot T>
ss::future<> persisted_stm_base<BaseT, T>::remove_persistent_state() {
    return _snapshot_backend.remove_persistent_state();
}

file_backed_stm_snapshot::file_backed_stm_snapshot(
  ss::sstring snapshot_name, prefix_logger& log, raft::consensus* c)
  : _ntp(c->ntp())
  , _log(log)
  , _snapshot_mgr(
      std::filesystem::path(c->log_config().work_directory()),
      std::move(snapshot_name),
      ss::default_priority_class()) {}

ss::future<> file_backed_stm_snapshot::perform_initial_cleanup() {
    // Do nothing as the log directory name contains the partition revision,
    // therefore any snapshots that we'll find there for a newly created log are
    // not coming from the previous incarnation of this ntp (and were likely put
    // there deliberately).
    return ss::now();
}

ss::future<> file_backed_stm_snapshot::remove_persistent_state() {
    _snapshot_size = 0;
    co_await _snapshot_mgr.remove_snapshot();
    co_await _snapshot_mgr.remove_partial_snapshots();
}

ss::future<std::optional<stm_snapshot>>
file_backed_stm_snapshot::load_snapshot() {
    auto maybe_reader = co_await _snapshot_mgr.open_snapshot();
    if (!maybe_reader) {
        co_return std::nullopt;
    }

    storage::snapshot_reader& reader = *maybe_reader;
    iobuf meta_buf = co_await reader.read_metadata();
    iobuf_parser meta_parser(std::move(meta_buf));
    auto header = read_snapshot_header(meta_parser, _ntp, name());
    if (!header) {
        vlog(_log.warn, "Skipping snapshot {} due to old format", store_path());

        // can't load old format of the snapshot, since snapshot is missing
        // it will be reconstructed by replaying the log
        co_await reader.close();
        co_return std::nullopt;
    }
    stm_snapshot snapshot;
    snapshot.header = *header;
    snapshot.data = co_await read_iobuf_exactly(
      reader.input(), snapshot.header.snapshot_size);

    _snapshot_size = co_await reader.get_snapshot_size();
    co_await reader.close();
    co_await _snapshot_mgr.remove_partial_snapshots();

    co_return snapshot;
}

ss::future<> file_backed_stm_snapshot::persist_local_snapshot(
  storage::simple_snapshot_manager& snapshot_mgr, stm_snapshot&& snapshot) {
    iobuf data_size_buf;

    int8_t version = stm_snapshot_version;
    int64_t offset = snapshot.header.offset();
    int8_t data_version = snapshot.header.version;
    int32_t data_size = snapshot.header.snapshot_size;
    reflection::serialize(
      data_size_buf, version, offset, data_version, data_size);

    return snapshot_mgr.start_snapshot().then(
      [&snapshot_mgr,
       snapshot = std::move(snapshot),
       data_size_buf = std::move(data_size_buf)](
        storage::file_snapshot_writer writer) mutable {
          return ss::do_with(
            std::move(writer),
            [&snapshot_mgr,
             snapshot = std::move(snapshot),
             data_size_buf = std::move(data_size_buf)](
              storage::file_snapshot_writer& writer) mutable {
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

ss::future<>
file_backed_stm_snapshot::persist_local_snapshot(stm_snapshot&& snapshot) {
    return persist_local_snapshot(_snapshot_mgr, std::move(snapshot))
      .then([this] {
          return _snapshot_mgr.get_snapshot_size().then(
            [this](uint64_t size) { _snapshot_size = size; });
      });
}

const ss::sstring& file_backed_stm_snapshot::name() {
    return _snapshot_mgr.name();
}

ss::sstring file_backed_stm_snapshot::store_path() const {
    return _snapshot_mgr.snapshot_path().string();
}

size_t file_backed_stm_snapshot::get_snapshot_size() const {
    return _snapshot_size;
}

kvstore_backed_stm_snapshot::kvstore_backed_stm_snapshot(
  ss::sstring snapshot_name,
  prefix_logger& log,
  raft::consensus* c,
  storage::kvstore& kvstore)
  : _ntp(c->ntp())
  , _name(snapshot_name)
  , _snapshot_key(stm_snapshot_key(snapshot_name, c->ntp()))
  , _log(log)
  , _kvstore(kvstore) {}

kvstore_backed_stm_snapshot::kvstore_backed_stm_snapshot(
  ss::sstring snapshot_name,
  prefix_logger& log,
  model::ntp ntp,
  storage::kvstore& kvstore)
  : _ntp(ntp)
  , _name(snapshot_name)
  , _snapshot_key(stm_snapshot_key(snapshot_name, ntp))
  , _log(log)
  , _kvstore(kvstore) {}

bytes kvstore_backed_stm_snapshot::snapshot_key() const {
    return bytes::from_string(_snapshot_key);
}

const ss::sstring& kvstore_backed_stm_snapshot::name() { return _name; }

ss::sstring kvstore_backed_stm_snapshot::store_path() const {
    return _snapshot_key;
}

/// Serialized format of snapshots for newer format used by
/// kvstore_backed_stm_snapshot, once deserialized struct will be decomposed
/// into a normal stm_snapshot to keep the interface the same across impls
struct stm_thin_snapshot
  : serde::
      envelope<stm_thin_snapshot, serde::version<0>, serde::compat_version<0>> {
    model::offset offset;
    iobuf data;

    auto serde_fields() { return std::tie(offset, data); }
};

ss::future<> kvstore_backed_stm_snapshot::perform_initial_cleanup() {
    // Persistent state in the kvstore is keyed by ntp without revision and
    // therefore could come from the previous partition incarnation. Remove it.
    return remove_persistent_state();
}

ss::future<std::optional<stm_snapshot>>
kvstore_backed_stm_snapshot::load_snapshot() {
    auto snapshot_blob = _kvstore.get(
      storage::kvstore::key_space::stms, snapshot_key());
    if (!snapshot_blob) {
        co_return std::nullopt;
    }
    auto thin_snapshot = serde::from_iobuf<stm_thin_snapshot>(
      std::move(*snapshot_blob));
    stm_snapshot snapshot;
    snapshot.header = raft::stm_snapshot_header{
      .version = stm_snapshot_version,
      .snapshot_size = static_cast<int32_t>(thin_snapshot.data.size_bytes()),
      .offset = thin_snapshot.offset};
    snapshot.data = std::move(thin_snapshot.data);
    co_return snapshot;
}

ss::future<>
kvstore_backed_stm_snapshot::persist_local_snapshot(stm_snapshot&& snapshot) {
    stm_thin_snapshot thin_snapshot{
      .offset = snapshot.header.offset, .data = std::move(snapshot.data)};
    auto serialized_snapshot = serde::to_iobuf(std::move(thin_snapshot));
    co_await _kvstore.put(
      storage::kvstore::key_space::stms,
      snapshot_key(),
      std::move(serialized_snapshot));
}

ss::future<> kvstore_backed_stm_snapshot::remove_persistent_state() {
    co_await _kvstore.remove(storage::kvstore::key_space::stms, snapshot_key());
}

size_t kvstore_backed_stm_snapshot::get_snapshot_size() const {
    /// get_snapshot_size() is used to account for total size of files across
    /// the disk, kvstore has its own accounting method so return 0 here to not
    /// skew the existing metrics.
    return 0;
}

template<typename BaseT, supported_stm_snapshot T>
ss::future<> persisted_stm_base<BaseT, T>::wait_for_snapshot_hydrated() {
    return _on_snapshot_hydrated.wait([this] { return _snapshot_hydrated; });
}

template<typename BaseT, supported_stm_snapshot T>
ss::future<> persisted_stm_base<BaseT, T>::do_write_local_snapshot() {
    auto u = co_await BaseT::_apply_lock.get_units();
    auto snapshot = co_await take_local_snapshot(std::move(u));
    auto offset = snapshot.header.offset;

    co_await _snapshot_backend.persist_local_snapshot(std::move(snapshot));
    _last_snapshot_offset = std::max(_last_snapshot_offset, offset);
}

template<typename BaseT, supported_stm_snapshot T>
void persisted_stm_base<BaseT, T>::write_local_snapshot_in_background() {
    ssx::spawn_with_gate(_gate, [this] { return write_local_snapshot(); });
}

template<typename BaseT, supported_stm_snapshot T>
ss::future<> persisted_stm_base<BaseT, T>::write_local_snapshot() {
    return _op_lock.with([this]() {
        return wait_for_snapshot_hydrated().then(
          [this] { return do_write_local_snapshot(); });
    });
}

template<typename BaseT, supported_stm_snapshot T>
uint64_t persisted_stm_base<BaseT, T>::get_local_snapshot_size() const {
    return _snapshot_backend.get_snapshot_size();
}

template<typename BaseT, supported_stm_snapshot T>
ss::future<> persisted_stm_base<BaseT, T>::ensure_local_snapshot_exists(
  model::offset target_offset) {
    vlog(
      _log.debug,
      "ensure snapshot_exists with target offset: {}",
      target_offset);
    return _op_lock.with([this, target_offset]() {
        return wait_for_snapshot_hydrated().then([this, target_offset] {
            if (target_offset <= _last_snapshot_offset) {
                return ss::now();
            }
            return BaseT::wait(target_offset, model::no_timeout)
              .then([this, target_offset]() {
                  vassert(
                    target_offset < BaseT::next(),
                    "[{} ({})]  after we waited for target_offset ({}) "
                    "next ({}) must be greater",
                    _raft->ntp(),
                    name(),
                    target_offset,
                    BaseT::next());
                  return do_write_local_snapshot();
              });
        });
    });
}

template<typename BaseT, supported_stm_snapshot T>
model::offset persisted_stm_base<BaseT, T>::max_collectible_offset() {
    return model::offset::max();
}

template<typename BaseT, supported_stm_snapshot T>
ss::future<fragmented_vector<model::tx_range>>
persisted_stm_base<BaseT, T>::aborted_tx_ranges(model::offset, model::offset) {
    return ss::make_ready_future<fragmented_vector<model::tx_range>>();
}

template<typename BaseT, supported_stm_snapshot T>
ss::future<> persisted_stm_base<BaseT, T>::wait_offset_committed(
  model::timeout_clock::duration timeout,
  model::offset offset,
  model::term_id term) {
    auto stop_cond = [this, offset, term] {
        return _raft->committed_offset() >= offset || _raft->term() > term;
    };

    return _raft->commit_index_updated().wait(timeout, stop_cond);
}

template<typename BaseT, supported_stm_snapshot T>
ss::future<bool> persisted_stm_base<BaseT, T>::do_sync(
  model::timeout_clock::duration timeout,
  model::offset offset,
  model::term_id term) {
    const auto committed = _raft->committed_offset();
    const auto ntp = _raft->ntp();
    _raft->events().notify_commit_index();

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

    if (_raft->term() == term) {
        try {
            co_await BaseT::wait(offset, model::timeout_clock::now() + timeout);
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
        if (_raft->term() == term) {
            _insync_term = term;
            co_return true;
        }
        // we lost leadership during waiting
    }

    co_return false;
}

template<typename BaseT, supported_stm_snapshot T>
ss::future<bool>
persisted_stm_base<BaseT, T>::sync(model::timeout_clock::duration timeout) {
    auto term = _raft->term();
    if (!_raft->is_leader()) {
        return ss::make_ready_future<bool>(false);
    }
    if (_insync_term == term) {
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

    // To guarantee that we have caught up with all records written by previous
    // leaders, we choose the last offset before the start of the current term
    // as the sync offset. Because the leader replicates the configuration batch
    // at the start of the term, this offset is guaranteed to be less than
    // committed index, so we won't need any additional flushes even if the
    // client produces with acks=1.
    model::offset sync_offset;
    auto log_offsets = _raft->log()->offsets();
    if (log_offsets.dirty_offset_term == term) {
        auto last_term_start_offset
          = _raft->log()->find_last_term_start_offset();
        if (last_term_start_offset > model::offset{0}) {
            sync_offset = last_term_start_offset - model::offset{1};
        } else {
            sync_offset = model::offset{};
        }
    } else {
        // We haven't been able to append the configuration batch at the start
        // of the term yet.
        sync_offset = log_offsets.dirty_offset;
    }

    return do_sync(timeout, sync_offset, term).then([this](bool is_synced) {
        _is_catching_up = false;
        for (auto& sync_waiter : _sync_waiters) {
            sync_waiter->set_value(is_synced);
        }
        _sync_waiters.clear();

        return is_synced;
    });
}

template<typename BaseT, supported_stm_snapshot T>
ss::future<bool> persisted_stm_base<BaseT, T>::wait_no_throw(
  model::offset offset,
  model::timeout_clock::time_point deadline,
  std::optional<std::reference_wrapper<ss::abort_source>> as) noexcept {
    return BaseT::wait(offset, deadline, as)
      .then([] { return true; })
      .handle_exception_type([](const ss::abort_requested_exception&) {
          // Shutting down
          return false;
      })
      .handle_exception_type(
        [this, offset, ntp = _raft->ntp()](const ss::timed_out_error&) {
            vlog(_log.warn, "timed out while waiting for offset: {}", offset);
            return false;
        })
      .handle_exception(
        [this, offset, ntp = _raft->ntp()](std::exception_ptr e) {
            vlog(
              _log.error,
              "An error {} happened during waiting for offset: {}",
              e,
              offset);
            return false;
        });
}

template<typename BaseT, supported_stm_snapshot T>
ss::future<> persisted_stm_base<BaseT, T>::start() {
    if (_raft->dirty_offset() == model::offset{}) {
        co_await _snapshot_backend.perform_initial_cleanup();
    }

    std::optional<stm_snapshot> maybe_snapshot;
    try {
        maybe_snapshot = co_await load_local_snapshot();
    } catch (...) {
        vassert(
          false,
          "[[{}] ({})]Can't load snapshot from '{}'. Got error: {}",
          _raft->ntp(),
          name(),
          _snapshot_backend.store_path(),
          std::current_exception());
    }

    if (maybe_snapshot) {
        stm_snapshot& snapshot = *maybe_snapshot;

        auto next_offset = model::next_offset(snapshot.header.offset);
        if (next_offset >= _raft->start_offset()) {
            vlog(
              _log.debug,
              "start with applied snapshot, set_next {}",
              next_offset);
            co_await apply_local_snapshot(
              snapshot.header, std::move(snapshot.data));
            BaseT::set_next(next_offset);
            _last_snapshot_offset = snapshot.header.offset;
        } else {
            // This can happen on an out-of-date replica that re-joins the group
            // after other replicas have already evicted logs to some offset
            // greater than snapshot.header.offset. We print a warning and
            // continue. The stm will later detect this situation and deal with
            // it in the apply fiber by calling handle_eviction.
            vlog(
              _log.warn,
              "Skipping snapshot {} since it's out of sync with the log",
              _snapshot_backend.store_path());
        }

    } else {
        vlog(_log.debug, "starting without snapshot");
    }
    _snapshot_hydrated = true;
    _on_snapshot_hydrated.broadcast();
}

template class persisted_stm_base<state_machine_base, file_backed_stm_snapshot>;
template class persisted_stm_base<
  state_machine_base,
  kvstore_backed_stm_snapshot>;

template class persisted_stm_base<
  no_at_offset_snapshot_stm_base,
  file_backed_stm_snapshot>;
template class persisted_stm_base<
  no_at_offset_snapshot_stm_base,
  kvstore_backed_stm_snapshot>;

template persisted_stm_base<state_machine_base, file_backed_stm_snapshot>::
  persisted_stm_base(ss::sstring, seastar::logger&, raft::consensus*);

template persisted_stm_base<state_machine_base, kvstore_backed_stm_snapshot>::
  persisted_stm_base(
    ss::sstring, seastar::logger&, raft::consensus*, storage::kvstore&);

template persisted_stm_base<
  no_at_offset_snapshot_stm_base,
  file_backed_stm_snapshot>::
  persisted_stm_base(ss::sstring, seastar::logger&, raft::consensus*);

template persisted_stm_base<
  no_at_offset_snapshot_stm_base,
  kvstore_backed_stm_snapshot>::
  persisted_stm_base(
    ss::sstring, seastar::logger&, raft::consensus*, storage::kvstore&);
ss::sstring kvstore_backed_stm_snapshot::snapshot_key(
  const ss::sstring& snapshot_name, const model::ntp& ntp) {
    return stm_snapshot_key(snapshot_name, ntp);
}

ss::future<> do_copy_persistent_stm_state(
  ss::sstring snapshot_name,
  model::ntp ntp,
  storage::kvstore& source_kvs,
  ss::shard_id target_shard,
  ss::sharded<storage::api>& api) {
    const auto key_as_str = raft::kvstore_backed_stm_snapshot::snapshot_key(
      snapshot_name, ntp);
    auto key = bytes::from_string(key_as_str);

    auto snapshot = source_kvs.get(storage::kvstore::key_space::stms, key);
    if (snapshot) {
        co_await api.invoke_on(
          target_shard, [key, &snapshot](storage::api& api) {
              return api.kvs().put(
                storage::kvstore::key_space::stms, key, snapshot->copy());
          });
    }
}

ss::future<> do_remove_persistent_stm_state(
  ss::sstring snapshot_name, model::ntp ntp, storage::kvstore& kvs) {
    const auto key_as_str = raft::kvstore_backed_stm_snapshot::snapshot_key(
      snapshot_name, ntp);
    auto key = bytes::from_string(key_as_str);
    co_await kvs.remove(storage::kvstore::key_space::stms, key);
}
} // namespace raft
