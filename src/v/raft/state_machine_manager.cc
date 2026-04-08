/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "raft/state_machine_manager.h"

#include "bytes/iostream.h"
#include "config/property.h"
#include "model/fundamental.h"
#include "model/timeout_clock.h"
#include "raft/consensus.h"
#include "raft/logger.h"
#include "raft/state_machine_base.h"
#include "raft/types.h"
#include "serde/async.h"
#include "serde/rw/rw.h"
#include "ssx/future-util.h"
#include "ssx/mutex.h"
#include "ssx/semaphore.h"
#include "ssx/watchdog.h"
#include "storage/snapshot.h"
#include "storage/types.h"

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/coroutine/switch_to.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/later.hh>

#include <stdexcept>
#include <vector>

namespace raft {

/**
 * Applicator which is a batch consumer that applies the same batch to multiple
 * state machines. If one of the STMs throws an exception from `apply` method
 * then not further batches are applied to that STM but the others continue
 */
class batch_applicator {
public:
    batch_applicator(
      const char* ctx,
      const std::vector<state_machine_manager::entry_ptr>& machines,
      ss::abort_source& as,
      ctx_log& log);

    ss::future<ss::stop_iteration> operator()(model::record_batch);

    model::offset end_of_stream() const { return _max_last_applied; }

private:
    struct apply_state {
        state_machine_manager::entry_ptr stm_entry;
        bool error{false};
    };
    using applied_successfully
      = ss::bool_class<struct applied_successfully_tag>;
    ss::future<applied_successfully>
    apply_to_stm(const model::record_batch& batch, apply_state& state);

    const char* _ctx;
    std::vector<apply_state> _machines;
    model::offset _max_last_applied;
    ss::abort_source& _as;
    ctx_log& _log;
};

batch_applicator::batch_applicator(
  const char* ctx,
  const std::vector<state_machine_manager::entry_ptr>& entries,
  ss::abort_source& as,
  ctx_log& log)
  : _ctx(ctx)
  , _as(as)
  , _log(log) {
    for (auto& m : entries) {
        _machines.push_back(apply_state{.stm_entry = m});
    }
}

ss::future<ss::stop_iteration>
batch_applicator::operator()(model::record_batch batch) {
    const auto last_offset = batch.last_offset();
    std::vector<ss::future<applied_successfully>> futures;
    futures.reserve(_machines.size());
    for (auto& state : _machines) {
        if (state.error) {
            continue;
        }
        futures.push_back(apply_to_stm(batch, state));
    }
    if (futures.empty()) {
        co_return ss::stop_iteration::yes;
    }

    auto results = co_await ss::when_all_succeed(
      futures.begin(), futures.end());
    /**
     * If any of the STMs applied batch successfully update _max_last_applied
     */
    if (
      std::any_of(
        results.begin(),
        results.end(),
        std::bind_front(std::equal_to<>(), applied_successfully::yes))) {
        _max_last_applied = last_offset;
    }

    co_return ss::stop_iteration(_as.abort_requested());
}
ss::future<batch_applicator::applied_successfully>
batch_applicator::apply_to_stm(
  const model::record_batch& batch, apply_state& state) {
    const auto last_offset = batch.last_offset();
    vlog(
      _log.trace,
      "[{}][{}] applying batch with base {} and last {} offsets",
      _ctx,
      state.stm_entry->name,
      batch.header().base_offset,
      last_offset);

    try {
        /**
         * If an apply timed out (took long time) the stm may already advanced
         * past what was requested to read
         */
        if (state.stm_entry->stm->next() > batch.base_offset()) {
            co_return applied_successfully::no;
        }

        co_await state.stm_entry->stm->apply(batch);
        co_return applied_successfully::yes;
    } catch (...) {
        vlog(
          _log.warn,
          "[{}][{}] error applying batch with base_offset: {} - {}",
          _ctx,
          state.stm_entry->name,
          batch.base_offset(),
          std::current_exception());
        state.error = true;
        co_return applied_successfully::no;
    }
}

state_machine_manager::named_stm::named_stm(ss::sstring name, stm_ptr stm)
  : name(std::move(name))
  , stm(std::move(stm)) {}

state_machine_manager::state_machine_manager(
  consensus* raft,
  std::vector<named_stm> stms,
  ss::scheduling_group apply_sg,
  config::binding<std::chrono::milliseconds> stm_shutdown_timeout)
  : _raft(raft)
  , _log(ctx_log(_raft->group(), _raft->ntp()))
  , _apply_sg(apply_sg)
  , _initial_recovery_snapshot_mgr(
      std::filesystem::path(_raft->log_config().work_directory()),
      "stm_manager.snapshot")
  , _stm_shutdown_timeout(std::move(stm_shutdown_timeout)) {
    for (auto& n_stm : stms) {
        _supports_snapshot_at_offset
          = _supports_snapshot_at_offset
            && n_stm.stm->supports_snapshot_at_offset();
        _machines.try_emplace(
          n_stm.name,
          ss::make_lw_shared<state_machine_entry>(
            n_stm.name, std::move(n_stm.stm)));
    }
}

model::offset state_machine_manager::max_next_offset() const {
    auto offsets = _machines | std::views::transform([](const auto& entry) {
                       return entry.second->stm->last_applied_offset();
                   });
    return model::next_offset(*std::ranges::max_element(offsets));
}

ss::future<> state_machine_manager::start() {
    vlog(_log.debug, "starting state machine manager");
    if (_machines.empty()) {
        co_return;
    }
    co_await ss::coroutine::parallel_for_each(_machines, [this](auto& pair) {
        vlog(_log.trace, "starting {} state machine", pair.first);
        return pair.second->stm->start();
    });
    std::vector<model::offset> offsets;
    for (const auto& [name, stm_meta] : _machines) {
        vlog(
          _log.trace,
          "state machine {} last applied offset: {}",
          name,
          stm_meta->stm->last_applied_offset());
        offsets.push_back(stm_meta->stm->last_applied_offset());
    }
    _next = model::next_offset(*std::ranges::max_element(offsets));
    auto log_offsets = _raft->log()->offsets();
    /**
     * Special case for the `archival_metadata_stm` local snapshot created
     * during recovery. The snapshot last included offset was set to the log
     * start offset.
     */
    if (log_offsets.start_offset > log_offsets.committed_offset) {
        vlog(
          _log.info,
          "starting state machine manager with empty log. Clamping _next {} to "
          "the start offset: {}",
          _next,
          log_offsets.start_offset);
        _next = log_offsets.start_offset;
    }
    vlog(
      _log.debug,
      "started state machine manager with initial next offset: {}",
      _next);
    // it safe to update next here as the apply loop didn't start yet.
    co_await apply_initial_recovery_policy();
    ssx::spawn_with_gate(_gate, [this] {
        return ss::do_until(
          [this] { return _as.abort_requested(); }, [this] { return apply(); });
    });
}

ss::future<> state_machine_manager::do_stop_stm(entry_ptr entry) {
    ssx::watchdog wd(
      _stm_shutdown_timeout(), [ntp = _raft->ntp(), name = entry->name] {
          vlog(
            raftlog.error,
            "[{}] Timedout waiting for {} state machine to stop",
            ntp,
            name);
      });
    // stop the state machine
    co_await entry->stm->stop();
}

ss::future<> state_machine_manager::stop() {
    vlog(
      _log.debug,
      "stopping state machine manager with {} state machines",
      _machines.size());
    _apply_mutex.broken();
    _as.request_abort();

    auto gate_f = _gate.close();
    co_await ss::coroutine::parallel_for_each(
      _machines, [this](auto p) { return do_stop_stm(p.second); });
    co_await std::move(gate_f);
}

ss::future<> state_machine_manager::apply_initial_recovery_policy() {
    auto snapshot = co_await read_initial_recovery_snapshot();
    if (!snapshot) {
        /**
         * If snapshot is not available create one, it will be populated during
         * initial recovery
         */
        vlog(_log.debug, "no initial recovery snapshot found");
        snapshot.emplace(initial_recovery_snapshot{});
    }
    vlog(_log.debug, "Starting with initial recovery snapshot: {}", *snapshot);
    for (auto& [name, entry] : _machines) {
        auto it = snapshot->initial_recovery_next_offsets.find(name);
        if (it != snapshot->initial_recovery_next_offsets.end()) {
            const auto init_recovery_offset = it->second;
            vlog(
              _log.debug,
              "skipping initial recovery for {} state machine as it was "
              "recovered from snapshot",
              name);
            if (
              entry->stm->next() > model::offset{0}
              && entry->stm->next() < init_recovery_offset) {
                vlog(
                  _log.error,
                  "State machine '{}' has next offset set to {} which is "
                  "less than the snapshot offset {}. Skipping initial "
                  "recovery.",
                  name,
                  entry->stm->next(),
                  init_recovery_offset);
                continue;
            }
            entry->stm->set_next(
              std::max(init_recovery_offset, entry->stm->next()));
            continue;
        }
        // Stm needs initial recovery
        const auto policy = entry->stm->get_initial_recovery_policy();
        vlog(
          _log.info,
          "Applying '{}' initial recovery policy for '{}' state machine",
          policy,
          name);
        switch (policy) {
        case stm_initial_recovery_policy::read_everything:
            snapshot->initial_recovery_next_offsets.emplace(
              name, model::offset{0});
            continue;
        case stm_initial_recovery_policy::skip_to_end:
            /**
             * special case of a state machine that already existed in the
             * previous version but the initial policy logic wasn't there yet
             */
            if (entry->stm->next() > model::offset{0}) {
                vlog(
                  _log.info,
                  "State machine '{}' already has next offset set to {}",
                  name,
                  entry->stm->next());
                snapshot->initial_recovery_next_offsets.emplace(
                  name, model::offset{0});
                continue;
            }
            /**
             * Here we apply the skip to end policy.
             *
             * The policy leverages the fact that the _next offset is already
             * established as all local snapshots from the other state machines
             * were already applied.
             *
             * We must not use the log end offset here as the end offset may
             * change with the truncation as it haven't yet been committed in
             * Raft sense.
             *
             * The `_next` is safe as it is based on the last applied offset of
             * the other state machines i.e. the `_next` offset is already
             * committed.
             *
             * This policy comes with the trade off. The newly added state
             * machines will not actually rewind to the physical end of the log.
             * But will read a small part of it.
             */
            vlog(
              _log.info,
              "Setting next offset: {} for: '{}' state machine as a part of "
              "initial recovery policy.",
              _next,
              name);
            entry->stm->set_next(_next);
            /**
             * Initial recovery finished, marked as done in the snapshot.
             */
            snapshot->initial_recovery_next_offsets.emplace(
              name, entry->stm->next());
        }
    }

    // clean up offsets from state machines that are not longer present in the
    // manager but are still in the initial recovery snapshot.
    absl::erase_if(
      snapshot->initial_recovery_next_offsets,
      [this](const auto& pair) { return !_machines.contains(pair.first); });

    co_await write_initial_recovery_snapshot(std::move(*snapshot));
}

std::vector<state_machine_manager::entry_ptr>
state_machine_manager::all_state_machines() const {
    std::vector<entry_ptr> all_stms;
    all_stms.reserve(_machines.size());
    std::ranges::copy(
      std::views::values(_machines), std::back_inserter(all_stms));
    return all_stms;
}

ss::future<> state_machine_manager::apply_raft_snapshot() {
    auto snapshot = co_await _raft->open_snapshot();
    if (!snapshot) {
        co_return;
    }
    auto fut = co_await ss::coroutine::as_future(
      acquire_background_apply_mutexes().then([&, this](auto units) mutable {
          return do_apply_raft_snapshot(
            all_state_machines(),
            std::move(snapshot->metadata),
            snapshot->reader,
            std::move(units));
      }));
    // update the _next offset to the max of the state machines applied offset
    // as some of them might have thrown
    _next = std::max(max_next_offset(), _next);
    co_await snapshot->reader.close();
    if (fut.failed()) {
        const auto e = fut.get_exception();
        // do not log known shutdown exceptions as errors
        if (!ssx::is_shutdown_exception(e)) {
            vlog(_log.error, "error applying raft snapshot - {}", e);
        }
        std::rethrow_exception(e);
    }
}

ss::future<> state_machine_manager::do_apply_raft_snapshot(
  std::vector<entry_ptr> state_machines,
  snapshot_metadata metadata,
  storage::snapshot_reader& reader,
  [[maybe_unused]] std::vector<ssx::semaphore_units> background_apply_units) {
    const auto snapshot_file_sz = co_await reader.get_snapshot_size();
    const auto last_offset = metadata.last_included_index;

    auto snapshot_content = co_await read_iobuf_exactly(
      reader.input(), snapshot_file_sz);
    const auto snapshot_content_sz = snapshot_content.size_bytes();

    vlog(
      _log.debug,
      "applying snapshot of size {} with last included offset: {}.",
      snapshot_content_sz,
      last_offset);
    /**
     * Previously all the STMs in Redpanda (excluding controller) were
     * using empty Raft snapshots. If snapshot is empty we still apply
     * it to maintain backward compatibility.
     */
    if (snapshot_content_sz == 0) {
        vlog(
          _log.debug,
          "applying empty snapshot at offset: {} for backward "
          "compatibility",
          last_offset);
        co_await ss::coroutine::parallel_for_each(
          state_machines, [last_offset](entry_ptr& entry) {
              auto stm = entry->stm;
              if (stm->last_applied_offset() >= last_offset) {
                  return ss::now();
              }
              return stm->apply_raft_snapshot(iobuf{}).then([stm, last_offset] {
                  stm->set_next(
                    std::max(model::next_offset(last_offset), stm->next()));
              });
          });

    } else {
        iobuf_parser parser(std::move(snapshot_content));
        auto snap = co_await serde::read_async<managed_snapshot>(parser);

        co_await ss::coroutine::parallel_for_each(
          state_machines,
          [this, snap = std::move(snap), last_offset](
            entry_ptr& entry) mutable {
              return apply_snapshot_to_stm(entry, snap, last_offset);
          });
    }
}

ss::future<> state_machine_manager::apply_snapshot_to_stm(
  ss::lw_shared_ptr<state_machine_entry> stm_entry,
  const managed_snapshot& snapshot,
  model::offset last_offset) {
    auto it = snapshot.snapshot_map.find(stm_entry->name);

    if (stm_entry->stm->last_applied_offset() < last_offset) {
        if (it != snapshot.snapshot_map.end()) {
            co_await stm_entry->stm->apply_raft_snapshot(it->second);
        } else {
            /**
             * In order to hold the stm contract we need to call the
             * apply_raft_snapshot with empty data
             */
            co_await stm_entry->stm->apply_raft_snapshot(iobuf{});
        }
    }

    stm_entry->stm->set_next(
      std::max(model::next_offset(last_offset), stm_entry->stm->next()));
}

ss::future<> state_machine_manager::try_apply_in_foreground() {
    try {
        ss::coroutine::switch_to sg_sw(_apply_sg);
        // wait until consensus commit index is >= _next
        co_await _raft->events().wait(_next, model::no_timeout, _as);
        auto u = co_await _apply_mutex.get_units();

        if (_next < _raft->start_offset()) {
            /**
             * We need to return here as applied snapshot may not yet be
             * committed.
             */
            co_return co_await apply_raft_snapshot();
        }

        // collect STMs which has the same _next offset as the offset in
        // manager and there is no background apply taking place
        std::vector<entry_ptr> machines;
        for (auto& [_, entry] : _machines) {
            /**
             * We can simply check if a mutex is ready here as calling
             * maybe_start_background_apply() will make the mutex underlying
             * semaphore immediately not ready as there are no scheduling points
             * before calling `get_units`
             */
            if (
              entry->stm->next() == _next
              && entry->background_apply_mutex.ready()) {
                machines.push_back(entry);
            }
        }
        if (machines.empty()) {
            vlog(
              _log.debug,
              "no machines were selected to apply in foreground, current next "
              "offset: {}",
              _next);
            co_return;
        }
        /**
         * Raft make_reader method allows callers reading up to
         * last_visible index. In order to make the STMs safe and working
         * with the raft semantics (i.e. what is applied must be comitted)
         * we have to limit reading to the committed offset.
         */
        vlog(
          _log.trace,
          "reading batches in range [{}, {}]",
          _next,
          _raft->committed_offset());
        /**
         * Use default priority for now, it is going to be unified with apply
         * scheduling group soon
         */
        auto config = storage::local_log_reader_config(
          _next, _raft->committed_offset());

        model::record_batch_reader reader = co_await _raft->make_reader(config);

        auto max_last_applied = co_await std::move(reader).consume(
          batch_applicator(default_ctx, machines, _as, _log),
          model::no_timeout);

        if (max_last_applied == model::offset{}) {
            vlogl(
              _log,
              _raft->log_config().cache_enabled() ? ss::log_level::warn
                                                  : ss::log_level::debug,
              "no progress has been made during state machine apply. Current "
              "next offset: {}",
              _next);
            /**
             * If no progress has been made, yield to prevent busy looping
             */
            co_await ss::sleep_abortable(100ms, _as);
            co_return;
        }
        _next = std::max(model::next_offset(max_last_applied), _next);
        vlog(_log.trace, "updating _next offset with: {}", _next);
    } catch (const ss::timed_out_error&) {
        vlog(_log.debug, "state machine apply timeout");
    } catch (const ss::abort_requested_exception&) {
    } catch (const ss::gate_closed_exception&) {
    } catch (const ss::broken_semaphore&) {
    } catch (...) {
        vlog(
          _log.warn, "manager apply exception: {}", std::current_exception());
    }
}

ss::future<> state_machine_manager::apply() noexcept {
    /**
     * If any of the state machine is behind, dispatch background apply fibers
     */
    if (!_as.abort_requested()) {
        for (auto& [_, entry] : _machines) {
            maybe_start_background_apply(entry);
        }
    }
    co_await try_apply_in_foreground();
}

void state_machine_manager::maybe_start_background_apply(
  const entry_ptr& entry) {
    if (likely(entry->stm->next() == _next)) {
        return;
    }

    /**
     * Do not wait for a mutex if background apply fiber is already active
     */
    if (!entry->background_apply_mutex.ready()) {
        return;
    }

    vlog(
      _log.debug,
      "starting background apply fiber for '{}' state machine",
      entry->name);

    ssx::spawn_with_gate(_gate, [this, entry] {
        return entry->background_apply_mutex.get_units().then(
          [this, entry](auto u) {
              return ss::with_scheduling_group(
                       _apply_sg,
                       [this, entry, u = std::move(u)]() mutable {
                           return background_apply_fiber(entry, std::move(u));
                       })
                .handle_exception([this](const std::exception_ptr& e) {
                    if (ssx::is_shutdown_exception(e)) {
                        vlog(
                          _log.debug,
                          "background_apply_fiber exited due to shutdown "
                          "exception {}",
                          e);
                    } else {
                        vlog(
                          _log.warn, "unexpected error in the bg fiber: {}", e);
                    }
                });
          });
    });
}

ss::future<> state_machine_manager::background_apply_fiber(
  entry_ptr entry, ssx::semaphore_units units) {
    while (!_as.abort_requested() && entry->stm->next() < _next) {
        if (entry->stm->next() < _raft->start_offset()) {
            auto snapshot = co_await _raft->open_snapshot();
            if (!snapshot) {
                vlog(
                  _log.warn,
                  "background apply fiber was not able to open snapshot for "
                  "'{}' stm",
                  entry->name);
                co_await ss::sleep_abortable(100ms, _as);
                continue;
            }
            vlog(
              _log.info,
              "background apply fiber applying raft snapshot with offset {} "
              "for {} state machine",
              snapshot->metadata.last_included_index,
              entry->name);

            auto fut = co_await ss::coroutine::as_future(do_apply_raft_snapshot(
              {entry},
              std::move(snapshot->metadata),
              snapshot->reader,
              std::vector<ssx::semaphore_units>{}));
            co_await snapshot->reader.close();
            if (fut.failed()) {
                const auto e = fut.get_exception();
                // do not log known shutdown exceptions as errors
                if (ssx::is_shutdown_exception(e)) {
                    std::rethrow_exception(e);
                } else {
                    vlog(_log.error, "error applying raft snapshot - {}", e);
                    co_await ss::sleep_abortable(100ms, _as);
                }
            }
            continue;
        }
        auto config = storage::local_log_reader_config(
          entry->stm->next(), model::prev_offset(_next));

        vlog(
          _log.debug,
          "reading batches in range [{}, {}] for '{}' stm background apply",
          entry->stm->next(),
          _next,
          entry->name);
        /**
         * As the STM is catching up and not reading the tip of the log it is
         * pointless to populate the batch cache with the read batches.
         */
        config.skip_batch_cache = true;
        bool error = false;
        try {
            model::record_batch_reader reader = co_await _raft->make_reader(
              config);
            auto last_applied_before = entry->stm->last_applied_offset();
            auto last_applied_after = co_await std::move(reader).consume(
              batch_applicator(background_ctx, {entry}, _as, _log),
              model::no_timeout);
            if (last_applied_before >= last_applied_after) {
                error = true;
            }
        } catch (...) {
            error = true;
            vlog(
              _log.warn,
              "exception thrown from background apply fiber for {} - {}",
              entry->name,
              std::current_exception());
        }
        if (error) {
            co_await ss::sleep_abortable(100ms, _as);
        }
    }
    units.return_all();
    vlog(
      _log.debug,
      "finished background apply for '{}' state machine",
      entry->name);
}

ss::future<state_machine_manager::snapshot_result>
state_machine_manager::take_snapshot(model::offset last_included_offset) {
    vassert(
      static_cast<bool>(_supports_snapshot_at_offset),
      "Snapshot at arbitrary offset can only be taken if manager supports fast "
      "reconfigurations");

    vlog(
      _log.debug,
      "taking snapshot with last included offset: {}",
      last_included_offset);
    if (last_included_offset < _raft->start_offset()) {
        throw std::logic_error(
          fmt::format(
            "Can not take snapshot of a state from before raft start offset. "
            "Requested offset: {}, start offset: {}",
            last_included_offset,
            _raft->start_offset()));
    }
    auto holder = _gate.hold();
    // wait for all STMs to be on the same page
    co_await wait(last_included_offset, model::no_timeout, _as);

    auto u = co_await _apply_mutex.get_units();
    // snapshot can only be taken  after  all background applies finished
    auto units = co_await acquire_background_apply_mutexes();

    managed_snapshot snapshot;
    co_await ss::coroutine::parallel_for_each(
      _machines, [last_included_offset, &snapshot](auto entry_pair) {
          return entry_pair.second->stm
            ->take_raft_snapshot(last_included_offset)
            .then([&snapshot, key = entry_pair.first](auto snapshot_part) {
                snapshot.snapshot_map.try_emplace(
                  key, std::move(snapshot_part));
            });
      });

    co_return state_machine_manager::snapshot_result{
      serde::to_iobuf(std::move(snapshot)), last_included_offset};
}

ss::future<state_machine_manager::snapshot_result>
state_machine_manager::take_snapshot() {
    auto holder = _gate.hold();
    // wait for all STMs to be on the same page
    co_await wait(last_applied(), model::no_timeout, _as);

    auto u = co_await _apply_mutex.get_units();
    if (last_applied() < _raft->start_offset()) {
        throw std::logic_error(
          fmt::format(
            "Can not take snapshot of a state from before raft start offset. "
            "Requested offset: {}, start offset: {}",
            last_applied(),
            _raft->start_offset()));
    }
    // wait once again for all state machines to finish applying batches
    co_await wait(last_applied(), model::no_timeout, _as);
    // snapshot can only be taken  after  all background applies finished
    auto units = co_await acquire_background_apply_mutexes();
    auto snapshot_offset = last_applied();
    managed_snapshot snapshot;
    co_await ss::coroutine::parallel_for_each(
      _machines, [snapshot_offset, &snapshot](auto entry_pair) {
          return entry_pair.second->stm->take_raft_snapshot(snapshot_offset)
            .then([&snapshot, key = entry_pair.first](auto snapshot_part) {
                snapshot.snapshot_map.try_emplace(
                  key, std::move(snapshot_part));
            });
      });

    co_return state_machine_manager::snapshot_result{
      serde::to_iobuf(std::move(snapshot)), snapshot_offset};
}

ss::future<> state_machine_manager::wait(
  model::offset offset,
  model::timeout_clock::time_point timeout,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    std::vector<ss::future<>> futures;
    futures.reserve(_machines.size());
    for (const auto& [_, entry] : _machines) {
        futures.push_back(entry->stm->wait(offset, timeout, as));
    }
    return ss::when_all_succeed(futures.begin(), futures.end());
}

ss::future<std::vector<ssx::semaphore_units>>
state_machine_manager::acquire_background_apply_mutexes() {
    std::vector<ss::future<ssx::semaphore_units>> futures;
    futures.reserve(_machines.size());
    for (auto& [_, entry] : _machines) {
        futures.push_back(entry->background_apply_mutex.get_units(_as));
    }
    return ss::when_all_succeed(futures.begin(), futures.end());
}

ss::future<> state_machine_manager::remove_local_state() {
    co_await ss::coroutine::parallel_for_each(_machines, [](auto entry_pair) {
        return entry_pair.second->stm->remove_local_state();
    });
    co_await _initial_recovery_snapshot_mgr.remove_snapshot();
}

ss::future<std::optional<state_machine_manager::initial_recovery_snapshot>>
state_machine_manager::read_initial_recovery_snapshot() {
    auto reader = co_await _initial_recovery_snapshot_mgr.open_snapshot();
    if (!reader) {
        co_return std::nullopt;
    }

    auto md_buffer_f = co_await ss::coroutine::as_future(
      reader->read_metadata());

    if (md_buffer_f.failed()) {
        auto e = md_buffer_f.get_exception();
        vlog(
          _log.error,
          "failed to read initial recovery snapshot metadata: {}",
          e);
        co_await reader->close();
        std::rethrow_exception(e);
    }

    auto snap_sz_f = co_await ss::coroutine::as_future(
      reader->get_snapshot_size());
    if (snap_sz_f.failed()) {
        auto e = snap_sz_f.get_exception();
        vlog(
          _log.error, "failed to read initial recovery snapshot size: {}", e);
        co_await reader->close();
        std::rethrow_exception(e);
    }
    auto snapshot_content_f = co_await ss::coroutine::as_future(
      read_iobuf_exactly(reader->input(), snap_sz_f.get()));

    if (snapshot_content_f.failed()) {
        auto e = snapshot_content_f.get_exception();
        vlog(_log.error, "failed to read recovery snapshot: {}", e);
        co_await reader->close();
        std::rethrow_exception(e);
    }
    co_await reader->close();

    co_await _initial_recovery_snapshot_mgr.remove_partial_snapshots();
    co_return serde::from_iobuf<initial_recovery_snapshot>(
      std::move(snapshot_content_f.get()));
}

ss::future<> state_machine_manager::write_initial_recovery_snapshot(
  initial_recovery_snapshot snap) {
    vlog(_log.trace, "writing initial recovery snapshot: {}", snap);
    auto writer = co_await _initial_recovery_snapshot_mgr.start_snapshot();

    auto md_f = co_await ss::coroutine::as_future(
      writer.write_metadata(iobuf{}));
    if (md_f.failed()) {
        auto e = md_f.get_exception();
        vlog(
          _log.error,
          "failed to write initial recovery snapshot metadata: {}",
          e);
        co_await writer.close();
        std::rethrow_exception(e);
    }

    auto data_f = co_await ss::coroutine::as_future(
      write_iobuf_to_output_stream(
        serde::to_iobuf(std::move(snap)), writer.output()));
    if (data_f.failed()) {
        auto e = data_f.get_exception();
        vlog(
          _log.error, "failed to write initial recovery snapshot data: {}", e);
        co_await writer.close();
        std::rethrow_exception(e);
    }

    co_await writer.close();
    co_await _initial_recovery_snapshot_mgr.finish_snapshot(writer);
}

std::ostream& operator<<(
  std::ostream& o, const state_machine_manager::initial_recovery_snapshot& s) {
    fmt::print(
      o,
      "{{ initial_recovery_next_offsets: {} }}",
      s.initial_recovery_next_offsets);
    return o;
}
} // namespace raft
