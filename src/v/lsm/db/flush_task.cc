// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "lsm/db/flush_task.h"

#include "base/vlog.h"
#include "lsm/core/internal/files.h"
#include "lsm/core/internal/logger.h"
#include "lsm/db/table_builder.h"

#include <seastar/core/coroutine.hh>

#include <exception>

namespace lsm::db {

using internal::operator""_level;

namespace {

ss::future<std::optional<ss::lw_shared_ptr<version_edit>>> do_run_flush_task(
  ss::lw_shared_ptr<internal::options> opts,
  io::data_persistence* persistence,
  version_set* versions,
  ss::lw_shared_ptr<memtable> imm,
  ss::abort_source* as) {
    auto v = versions->current();
    auto edit = versions->new_edit();
    auto id = edit->allocate_id();
    auto level = imm->empty()
                   ? 0_level
                   : v->pick_level_for_memtable_output(
                       imm->min_key().user_key(), imm->max_key().user_key());
    auto mem_bytes = imm->approximate_memory_usage();
    vlog(
      log.trace,
      "flush_memtable_start level={} file_id={} mem_bytes={}",
      level,
      id,
      mem_bytes);
    sst::builder::options sst_options{
      .block_size = opts->sst_block_size,
      .filter_period = opts->sst_filter_period,
      .compression = opts->levels[level].compression,
    };
    auto result = co_await build_table(
      persistence,
      {.id = id, .epoch = opts->database_epoch},
      imm->create_iterator(),
      sst_options,
      as);
    if (!result) {
        vlog(
          log.trace,
          "flush_memtable_end level={} file_bytes=0 empty=true",
          level);
        co_return std::nullopt;
    }
    edit->set_last_seqno(result->newest_seqno);
    edit->add_file({
      .level = level,
      .file_handle = {.id = id, .epoch = opts->database_epoch},
      .file_size = result->file_size,
      .smallest = std::move(result->smallest),
      .largest = std::move(result->largest),
      .oldest_seqno = result->oldest_seqno,
      .newest_seqno = result->newest_seqno,
    });
    vlog(
      log.trace,
      "flush_memtable_end level={} file_id={} file_bytes={}",
      level,
      id,
      result->file_size);
    co_return edit;
}

} // namespace

ss::future<std::optional<ss::lw_shared_ptr<version_edit>>> run_flush_task(
  ss::lw_shared_ptr<internal::options> opts,
  io::data_persistence* persistence,
  version_set* versions,
  ss::lw_shared_ptr<memtable> imm,
  ss::abort_source* as) {
    try {
        co_return co_await do_run_flush_task(
          std::move(opts), persistence, versions, std::move(imm), as);
    } catch (...) {
        vlog(
          log.warn,
          "flush_memtable_end error=\"{}\"",
          std::current_exception());
        throw;
    }
}

} // namespace lsm::db
