// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#pragma once

#include "lsm/core/internal/iterator.h"
#include "lsm/core/internal/keys.h"
#include "lsm/core/internal/options.h"

namespace lsm::db {

// A function that records a sample of bytes read at the specified internal key.
// Samples are taken approximately every
// `internal::config::read_bytes_period`.
using key_sample_fn
  = ss::noncopyable_function<ss::future<>(internal::key_view)>;

// Return a new iterator that converts internal keys that were live at the
// specified  seqno into appropriate user keys.
std::unique_ptr<internal::iterator> create_db_iterator(
  std::unique_ptr<internal::iterator> db_iter,
  internal::sequence_number seqno,
  ss::lw_shared_ptr<internal::options> options,
  key_sample_fn);

} // namespace lsm::db
