// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#pragma once

#include "lsm/core/internal/options.h"
#include "lsm/db/memtable.h"
#include "lsm/db/version_set.h"
#include "lsm/io/persistence.h"

namespace lsm::db {

// Run a flush task
ss::future<std::optional<ss::lw_shared_ptr<version_edit>>> run_flush_task(
  ss::lw_shared_ptr<internal::options> opts,
  io::data_persistence* persistence,
  version_set* versions,
  ss::lw_shared_ptr<memtable> imm,
  ss::abort_source* as);

} // namespace lsm::db
