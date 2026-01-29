/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "lsm/db/impl.h"

#include "base/vlog.h"
#include "lsm/core/exceptions.h"
#include "lsm/core/internal/files.h"
#include "lsm/core/internal/iterator.h"
#include "lsm/core/internal/keys.h"
#include "lsm/core/internal/logger.h"
#include "lsm/core/internal/merging_iterator.h"
#include "lsm/db/compaction_task.h"
#include "lsm/db/flush_task.h"
#include "lsm/db/iter.h"
#include "lsm/io/persistence.h"
#include "lsm/sst/block_cache.h"
#include "ssx/clock.h"

#include <seastar/core/sleep.hh>
#include <seastar/coroutine/as_future.hh>

#include <chrono>
#include <exception>
#include <memory>
#include <utility>

namespace lsm::db {

using internal::operator""_level;
using internal::operator""_file_id;

impl::impl(ctor, io::persistence p, ss::lw_shared_ptr<internal::options> o)
  : _persistence(std::move(p))
  , _opts(std::move(o))
  , _mem(ss::make_lw_shared<memtable>())
  , _table_cache(
      std::make_unique<table_cache>(
        _persistence.data.get(),
        _opts->max_open_files,
        ss::make_lw_shared<sst::block_cache>(
          _opts->block_cache_size / _opts->sst_block_size)))
  , _versions(
      std::make_unique<version_set>(
        _persistence.metadata.get(), _table_cache.get(), _opts))
  , _gc_actor(_persistence.data.get(), _opts, _table_cache.get())
  , _manifest_write_mu("lsm::db::impl::manifest_write_mu") {}

ss::future<std::unique_ptr<impl>> impl::open(
  ss::lw_shared_ptr<internal::options> opts, io::persistence persistence) {
    vlog(log.trace, "open_start");
    auto db = std::make_unique<impl>(
      ctor{}, std::move(persistence), std::move(opts));
    auto fut = co_await ss::coroutine::as_future(db->recover());
    if (fut.failed()) {
        auto ex = fut.get_exception();
        co_await db->close().handle_exception([](const std::exception_ptr&) {});
        std::rethrow_exception(ex);
    }
    // If we're readonly, we don't need to start any compaction loop.
    if (db->_opts->readonly) {
        vlog(log.trace, "open_end readonly=true");
        co_return db;
    }
    co_await db->_gc_actor.start();
    vlog(log.trace, "open_end readonly=false");
    co_return db;
}

ss::future<> impl::apply(ss::lw_shared_ptr<memtable> batch) {
    if (_opts->readonly) [[unlikely]] {
        throw invalid_argument_exception(
          "attempted to write to a readonly database");
    }
    if (batch->empty()) {
        co_return;
    }
    co_await make_room_for_write();
    _mem->merge(std::move(batch));
}

ss::future<> impl::make_room_for_write() {
    bool allow_delay = true;
    while (true) {
        auto num_l0_files = _versions->current()->num_files(0_level);
        if (
          allow_delay
          && num_l0_files > _opts->level_zero_slowdown_writes_trigger) {
            // We're in throttling mode
            vlog(log.debug, "throttling_writes reason=l0_file_count");
            constexpr auto throttle_duration = std::chrono::milliseconds(100);
            try {
                co_await ss::sleep_abortable(throttle_duration, _as);
            } catch (...) {
                throw abort_requested_exception(
                  "shutdown requested during write throttling");
            }
            // Only throttle once.
            allow_delay = false;
            continue;
        }
        if (_mem->approximate_memory_usage() <= _opts->write_buffer_size) {
            // We're under our write buffer limit, let's proceed
            // Note there is a scheduling point here, so this is a soft
            // limit.
            co_return;
        }
        if (_imm) {
            vlog(log.warn, "blocking_writes reason=memtable_full");
            // We are over the write buffer limit and we have a pending
            // memtable flush, wait for it to finish.
            co_await _background_work_finished_signal.wait(_as);
            continue;
        }
        if (num_l0_files > _opts->level_zero_stop_writes_trigger) {
            vlog(log.warn, "blocking_writes reason=l0_full");
            // We've hit out L0 file limit, wait for compaction to finish.
            co_await _background_work_finished_signal.wait(_as);
            continue;
        }
        // We're over our limit, let's make a new memtable
        vlog(
          log.trace,
          "scheduling_memtable_flush seqno={}",
          _mem->last_seqno().value());
        _imm = std::exchange(_mem, ss::make_lw_shared<memtable>());
        maybe_schedule_compaction();
    }
}

ss::future<lookup_result> impl::get(internal::key_view key) {
    // Lookup in the mutable memtable
    {
        auto result = _mem->get(key);
        if (!result.is_missing()) {
            co_return result;
        }
    }
    // Lookup in the frozen memtable
    if (_imm) {
        auto result = (*_imm)->get(key);
        if (!result.is_missing()) {
            co_return result;
        }
    }
    // Lookup in the files
    auto current = _versions->current();
    version::get_stats stats{};
    auto result = co_await current->get(key, &stats);
    if (current->update_stats(stats)) {
        maybe_schedule_compaction();
    }
    co_return result;
}

ss::future<std::unique_ptr<internal::iterator>>
impl::create_iterator(iterator_options opts) {
    std::optional<internal::sequence_number> iter_seqno = max_applied_seqno();
    std::unique_ptr<internal::iterator> iter;
    if (!iter_seqno) {
        // If there is no data in the database, then we create
        // a view of empty data, since we cannot pin before 0.
        iter = internal::iterator::create_empty();
    } else {
        iter = create_db_iterator(
          co_await create_internal_iterator(),
          opts.snapshot ? (*opts.snapshot)->seqno() : iter_seqno.value(),
          _opts,
          [this](internal::key_view key) {
              return _versions->current()->record_read_sample(key).then(
                [this](bool compaction_needed) {
                    if (compaction_needed) {
                        maybe_schedule_compaction();
                    }
                });
          });
    }
    // If there is a non-empty memtable, wrap our existing iterator on top of
    // it and clamp further writes to the memtable to be applied.
    if (auto table = opts.memtable->get(); table && !table->empty()) {
        chunked_vector<std::unique_ptr<internal::iterator>> merged;
        merged.push_back(table->create_iterator());
        merged.push_back(std::move(iter));
        iter = create_db_iterator(
          internal::create_merging_iterator(std::move(merged)),
          table->last_seqno().value(),
          _opts,
          [](internal::key_view) { return ss::now(); });
    }
    co_return iter;
}

ss::future<std::unique_ptr<internal::iterator>>
impl::create_internal_iterator() {
    chunked_vector<std::unique_ptr<internal::iterator>> list;
    list.push_back(_mem->create_iterator());
    if (_imm) {
        list.push_back((*_imm)->create_iterator());
    }
    co_await _versions->current()->add_iterators(&list);
    co_return internal::create_merging_iterator(std::move(list));
}

ss::future<> impl::flush(ssx::instant deadline) {
    if (_opts->readonly) [[unlikely]] {
        throw invalid_argument_exception(
          "attempted to flush a readonly database");
    }
    auto applied_seqno = max_applied_seqno();
    while (applied_seqno > max_persisted_seqno()) {
        if (ssx::lowres_steady_clock().now() > deadline) {
            throw io_error_exception(
              "failed to persist up to seqno {} in time: current persisted "
              "seqno {}",
              applied_seqno.value_or(internal::sequence_number(0)),
              max_persisted_seqno().value_or(internal::sequence_number(0)));
        }
        if (_imm) {
            co_await _background_work_finished_signal.wait(
              deadline.to_chrono<ss::lowres_clock>(), _as);
        } else if (!_mem->empty()) {
            _imm = std::exchange(_mem, ss::make_lw_shared<memtable>());
            maybe_schedule_compaction();
        }
    }
}

ss::future<> impl::flush() {
    return impl::flush(ssx::instant::infinite_future());
}

ss::future<> impl::close() {
    vlog(log.trace, "close_start");
    _as.request_abort_ex(abort_requested_exception("database closing"));
    co_await _gc_actor.stop();
    co_await std::exchange(_compaction_task, std::nullopt).value_or(ss::now());
    co_await std::exchange(_flush_task, std::nullopt).value_or(ss::now());
    co_await _table_cache->close();
    co_await _persistence.data->close();
    co_await _persistence.metadata->close();
    vlog(log.trace, "close_end");
}

ss::future<> impl::recover() {
    vlog(log.trace, "recover_start");
    co_await _versions->recover();
    // If requested, then pre-open all the files we know about.
    if (auto max_fibers = _opts->max_pre_open_fibers) {
        chunked_vector<ss::lw_shared_ptr<file_meta_data>> all_files;
        for (auto level : _opts->levels) {
            auto files = _versions->current()->get_overlapping_inputs(
              level.number, std::nullopt, std::nullopt);
            for (auto& file : files) {
                all_files.push_back(std::move(file));
            }
        }
        vlog(log.trace, "recover_pre_open_start files={}", all_files.size());
        co_await ss::max_concurrent_for_each(
          all_files, max_fibers, [this](ss::lw_shared_ptr<file_meta_data> f) {
              return _table_cache->create_iterator(f->handle, f->file_size)
                .discard_result();
          });
        vlog(log.trace, "recover_pre_open_end files={}", all_files.size());
    }
    vlog(log.trace, "recover_end");
}

void impl::maybe_schedule_compaction() {
    if (_as.abort_requested()) {
        return;
    }
    if (!_flush_task && _imm) {
        vlog(log.trace, "flush_task_start");
        auto task = do_flush().then_wrapped([this](ss::future<> f) {
            if (f.failed()) {
                auto ex = f.get_exception();
                vlog(log.warn, "flush_task_end error=\"{}\"", ex);
            } else {
                // Notify all waiters that work has been finished.
                _background_work_finished_signal.broadcast();
                vlog(log.trace, "flush_task_end");
            }
            // Check and see if the new manifest we wrote requires compaction
            // due to number files in L0 (or retry if there was an error).
            _flush_task = std::nullopt;
            maybe_schedule_compaction();
        });
        // It is possible in release mode tests that the above closure has
        // executed already (because it's using in0memory IO). In this case we
        // don't want to assign the flush task otherwise nothing would be able
        // to remove it.
        if (!task.available()) {
            _flush_task = std::move(task);
        }
    }
    if (!_compaction_task && _versions->needs_compaction()) {
        vlog(log.trace, "compaction_task_start");
        auto task = do_compaction().then_wrapped([this](ss::future<> f) {
            if (f.failed()) {
                auto ex = f.get_exception();
                vlog(log.warn, "compaction_task_end error=\"{}\"", ex);
            } else {
                // Notify all waiters that work has been finished.
                _background_work_finished_signal.broadcast();
                vlog(log.trace, "compaction_task_end");
            }
            // Check and see if the new manifest we wrote requires compaction
            // due to level size limits (or retry if there was an error).
            _compaction_task = std::nullopt;
            maybe_schedule_compaction();
        });
        if (!task.available()) {
            _compaction_task = std::move(task);
        }
    }
}

ss::future<> impl::apply_edits(ss::lw_shared_ptr<version_edit> edit) {
    auto units = co_await _manifest_write_mu.get_units();
    vlog(log.trace, "apply_edits_start");
    auto fut = co_await ss::coroutine::as_future(
      _versions->log_and_apply(std::move(edit)));
    if (fut.failed()) {
        auto ex = fut.get_exception();
        vlog(log.warn, "apply_edits_end error=\"{}\"", ex);
        std::rethrow_exception(ex);
    }
    vlog(
      log.trace, "apply_edits_end seqno={}", _versions->last_seqno().value());
}

ss::future<> impl::do_flush() {
    if (!_imm) {
        co_return;
    }
    auto edit = co_await run_flush_task(
      _opts, _persistence.data.get(), _versions.get(), *_imm, &_as);
    if (!edit) {
        _imm = std::nullopt;
        co_return;
    }
    co_await apply_edits(std::move(edit.value()));
    // Now that the new version has been applied, it's safe to remove the
    // immutable memtable, as readers will pick up the new file instead.
    //
    // Note that it's possible for a reader to pick up both the memtable
    // and the new version with the file. This is OK because all iterators
    // deduplicate already.
    _imm = std::nullopt;
}

ss::future<> impl::do_compaction() {
    auto compact = _versions->pick_compaction();
    if (!compact) {
        co_return;
    }
    auto edit = co_await run_compaction_task(
      _persistence.data.get(),
      &_snapshots,
      _versions.get(),
      _opts,
      std::move(compact.value()),
      &_as);
    co_await apply_edits(std::move(edit));
    // TODO: only call GC actor when we delete a file. This requires
    // the GC actor still doing cleanup when it is not called.
    auto safe_highest = _versions->min_uncommitted_file_id() - 1_file_id;
    _gc_actor.tell(
      gc_message{
        .live_files = _versions->get_live_files(),
        .safe_highest_file_id = safe_highest,
      });
}

std::optional<internal::sequence_number> impl::max_persisted_seqno() const {
    return _versions->last_seqno();
}

std::optional<internal::sequence_number> impl::max_applied_seqno() const {
    if (auto seqno = _mem->last_seqno()) {
        return seqno;
    }
    if (_imm) {
        if (auto seqno = (*_imm)->last_seqno()) {
            return seqno;
        }
    }
    return max_persisted_seqno();
}

ss::optimized_optional<std::unique_ptr<snapshot>> impl::create_snapshot() {
    if (auto seqno = max_applied_seqno()) {
        return _snapshots.create(*seqno);
    }
    return std::nullopt;
}

} // namespace lsm::db
