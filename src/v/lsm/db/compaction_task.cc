// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "lsm/db/compaction_task.h"

#include "base/vlog.h"
#include "lsm/core/exceptions.h"
#include "lsm/core/internal/logger.h"
#include "lsm/sst/builder.h"
#include "ssx/when_all.h"

#include <exception>

namespace lsm::db {

namespace {

struct compaction_state {
    struct output {
        internal::file_handle handle;
        uint64_t file_size = 0;
        internal::key smallest, largest;
        internal::sequence_number oldest, newest;
    };

    output& current_output() { return outputs.back(); }

    ss::future<> open_current_builder(
      internal::file_handle h,
      io::data_persistence* p,
      sst::builder::options opts) {
        outputs.emplace_back(h);
        auto w = co_await p->open_sequential_writer(h);
        builder.emplace(std::move(w), opts);
    }

    struct closing_builder {
        sst::builder builder;
        ss::future<> close_fut;

        explicit closing_builder(sst::builder b)
          : builder(std::move(b))
          , close_fut(builder.close()) {}
    };

    ss::future<> finish_current_builder() {
        if (!builder) {
            co_return;
        }
        auto b = std::exchange(builder, std::nullopt);
        auto finish_fut = co_await ss::coroutine::as_future(b->finish());
        uint64_t current_bytes = b->file_size();
        closes.emplace_back(std::move(*b));
        if (finish_fut.failed()) {
            std::rethrow_exception(finish_fut.get_exception());
        }
        current_output().file_size = current_bytes;
        total_bytes += current_bytes;
    }

    ss::future<> finish() {
        if (builder) {
            auto ready_future = co_await ss::coroutine::as_future(
              finish_current_builder());
            if (err) {
                ready_future.ignore_ready_future();
            } else if (ready_future.failed()) {
                err = ready_future.get_exception();
            }
        }
        for (auto& c : closes) {
            auto ready_future = co_await ss::coroutine::as_future(
              std::move(c.close_fut));
            if (err) {
                ready_future.ignore_ready_future();
            } else if (ready_future.failed()) {
                err = ready_future.get_exception();
            }
        }
        if (err) {
            std::rethrow_exception(err);
        }
    }

    chunked_vector<closing_builder> closes;
    std::exception_ptr err;
    chunked_vector<output> outputs;
    // Sequence numbers < smallest_snapshot are not significant since we
    // will never have to service a snapshot below smallest_snapshot.
    // Therefore if we have seen a sequence number S <=
    // smallest_snapshot, we can drop all entries for the same key with
    // sequence numbers < S.
    internal::sequence_number smallest_snapshot;
    std::optional<sst::builder> builder;
    uint64_t total_bytes = 0;
};

using internal::operator""_level;

ss::future<ss::lw_shared_ptr<version_edit>> do_run_compaction_task(
  io::data_persistence* persistence,
  snapshot_list* snapshots,
  version_set* versions,
  ss::lw_shared_ptr<internal::options> opts,
  compaction* compaction,
  ss::abort_source* as) {
    auto input_level = compaction->level();
    auto output_level = input_level + 1_level;
    auto num_input_files
      = compaction->num_input_files(compaction::which::input_level)
        + compaction->num_input_files(compaction::which::output_level);
    vlog(
      log.trace,
      "compaction_start input_level={} output_level={} input_files={}",
      input_level,
      output_level,
      num_input_files);
    if (compaction->is_trivial_move()) {
        auto input_level_files = compaction->num_input_files(
          compaction::which::input_level);
        vassert(
          input_level_files == 1,
          "trivial compactions should only be for a single input file: {}",
          input_level_files);
        auto file = compaction->input(compaction::which::input_level, 0);
        compaction->edit()->remove_file(compaction->level(), file->handle);
        compaction->edit()->add_file({
          .level = compaction->level() + 1_level,
          .file_handle = file->handle,
          .file_size = file->file_size,
          .smallest = file->smallest,
          .largest = file->largest,
          .oldest_seqno = file->oldest_seqno,
          .newest_seqno = file->newest_seqno,
        });
        vlog(
          log.trace,
          "compaction_end input_level={} output_level={} output_files=1 "
          "output_bytes={} trivial_move=true",
          input_level,
          output_level,
          file->file_size);
        co_return compaction->edit();
    }
    compaction_state state{
      // We need to preserve intermediate data between snapshots so we can
      // surface the correct snapshot isolation. So we use the oldest snapshot,
      // otherwise the lastest sequence number because we don't have to preserve
      // any intermediate versions.
      // Note we can call `value` on `last_seqno` because we have to have
      // some data in the database in order to trigger compaction.
      .smallest_snapshot = snapshots->oldest_seqno().value_or(
        versions->last_seqno().value()),
    };
    auto max_file_size = opts->levels[output_level].max_file_size;
    sst::builder::options sst_options{
      .block_size = opts->sst_block_size,
      .filter_period = opts->sst_filter_period,
      .compression = opts->levels[output_level].compression,
    };
    try {
        auto input = co_await versions->make_input_iterator(
          compaction, {.readahead_size = opts->compaction_readahead_size});
        std::optional<internal::key> current_key;
        internal::sequence_number last_seqno_for_key
          = internal::sequence_number::max();
        co_await input->seek_to_first();
        while (input->valid() && !as->abort_requested()) {
            auto key = input->key();
            if (compaction->should_stop_before(key) && state.builder) {
                co_await state.finish_current_builder();
            }
            bool drop = false;
            if (!current_key || key.user_key() != current_key->user_key()) {
                // First occurrence of this user key
                current_key = internal::key(key);
                last_seqno_for_key = internal::sequence_number::max();
            }
            auto key_seqno = key.seqno();
            // NOLINTNEXTLINE(*branch-clone*)
            if (last_seqno_for_key <= state.smallest_snapshot) {
                // Hidden by a newer entry for the same user key
                drop = true;
            } else if (
              key.is_tombstone() && key_seqno <= state.smallest_snapshot
              && compaction->is_base_level_for_key(key)) {
                // For this user key:
                // (1) there is no data in higher levels
                // (2) data in lower levels will have larger sequence numbers
                // (3) data in layers that are being compacted here
                // and have smaller sequence numbers will be dropped in the
                // next few iterations of this loop (by rule (A) above).
                // Therefore this deletion marker is obsolete and can be
                // dropped.
                drop = true;
            }
            last_seqno_for_key = key_seqno;
            if (!drop) {
                if (!state.builder) {
                    auto id = compaction->edit()->allocate_id();
                    vlog(log.trace, "compaction_start_new_file file_id={}", id);
                    co_await state.open_current_builder(
                      {
                        .id = id,
                        .epoch = opts->database_epoch,
                      },
                      persistence,
                      sst_options);
                }
                auto& current = state.current_output();
                if (state.builder->num_entries() == 0) {
                    current.smallest = internal::key(key);
                    current.oldest = key_seqno;
                    current.newest = key_seqno;
                }
                current.largest = internal::key(key);
                current.oldest = std::min(key_seqno, current.oldest);
                current.newest = std::max(key_seqno, current.newest);
                co_await state.builder->add(internal::key(key), input->value());
                // Close output file if it is big enough
                if (state.builder->file_size() >= max_file_size) {
                    co_await state.finish_current_builder();
                }
            }
            co_await input->next();
        }
    } catch (const base_exception& ex) {
        state.err = std::make_exception_ptr(ex);
    }
    co_await state.finish();
    auto edit = compaction->edit();
    compaction->add_input_deletions(edit.get());
    for (auto& output : state.outputs) {
        edit->add_file({
          .level = compaction->level() + 1_level,
          .file_handle = output.handle,
          .file_size = output.file_size,
          .smallest = std::move(output.smallest),
          .largest = std::move(output.largest),
          .oldest_seqno = output.oldest,
          .newest_seqno = output.newest,
        });
    }
    vlog(
      log.trace,
      "compaction_end input_level={} output_level={} output_files={} "
      "output_bytes={}",
      input_level,
      output_level,
      state.outputs.size(),
      state.total_bytes);
    co_return edit;
}

} // namespace

ss::future<ss::lw_shared_ptr<version_edit>> run_compaction_task(
  io::data_persistence* persistence,
  snapshot_list* snapshots,
  version_set* versions,
  ss::lw_shared_ptr<internal::options> opts,
  compaction* compaction,
  ss::abort_source* as) {
    try {
        co_return co_await do_run_compaction_task(
          persistence, snapshots, versions, std::move(opts), compaction, as);
    } catch (...) {
        vlog(log.warn, "compaction_end error=\"{}\"", std::current_exception());
        throw;
    }
}

} // namespace lsm::db
