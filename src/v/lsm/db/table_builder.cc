// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "lsm/db/table_builder.h"

#include "lsm/core/internal/files.h"
#include "lsm/sst/builder.h"

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/as_future.hh>

namespace lsm::db {

namespace {

ss::future<std::optional<build_table_result>> write_sst_file(
  std::unique_ptr<internal::iterator> iter,
  sst::builder* builder,
  ss::abort_source* as) {
    co_await iter->seek_to_first();
    if (!iter->valid()) {
        // There are no keys
        co_return std::nullopt;
    }
    auto key_view = iter->key();
    auto seqno = key_view.seqno();
    auto key = internal::key(key_view);
    build_table_result result{
      .file_size = 0,
      .smallest = key,
      .oldest_seqno = seqno,
      .newest_seqno = seqno,
    };
    while (iter->valid() && !as->abort_requested()) {
        auto key_view = iter->key();
        seqno = key_view.seqno();
        result.oldest_seqno = std::min(result.oldest_seqno, seqno);
        result.newest_seqno = std::max(result.newest_seqno, seqno);
        // It's a bummer we have to copy the key everytime, we actually do this
        // within the SST file builder as well, maybe there is a way to optimize
        // this without leaking implementation details of the builder?
        key = internal::key(key_view);
        co_await builder->add(key, iter->value());
        co_await iter->next();
    }
    result.largest = std::move(key);
    if (as->abort_requested()) {
        co_return std::nullopt;
    }
    co_await builder->finish();
    result.file_size = builder->file_size();
    co_return result;
}

} // namespace

ss::future<std::optional<build_table_result>> build_table(
  io::data_persistence* persistence,
  internal::file_handle handle,
  std::unique_ptr<internal::iterator> iter,
  sst::builder::options opts,
  ss::abort_source* as) {
    auto writer = co_await persistence->open_sequential_writer(handle);
    sst::builder builder{std::move(writer), opts};
    auto result = co_await write_sst_file(std::move(iter), &builder, as)
                    .finally([&builder] { return builder.close(); });
    if (!result) {
        co_await persistence->remove_file(handle);
    }
    co_return result;
}

} // namespace lsm::db
