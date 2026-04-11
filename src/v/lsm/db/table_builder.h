// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#pragma once

#include "base/seastarx.h"
#include "lsm/core/internal/files.h"
#include "lsm/core/internal/iterator.h"
#include "lsm/core/internal/keys.h"
#include "lsm/io/persistence.h"
#include "lsm/sst/builder.h"

#include <seastar/core/future.hh>

#include <memory>

namespace lsm::db {

// The result of building an SST table. It includes the size of the file as well
// as metadata about the bounds of data written.
struct build_table_result {
    uint64_t file_size;
    internal::key smallest;
    internal::key largest;
    internal::sequence_number oldest_seqno;
    internal::sequence_number newest_seqno;
};

// Create an SST file with the given ID from the provided iterator.
ss::future<std::optional<build_table_result>> build_table(
  io::data_persistence* persistence,
  internal::file_handle handle,
  std::unique_ptr<internal::iterator> iter,
  sst::builder::options opts,
  ss::abort_source*);

} // namespace lsm::db
