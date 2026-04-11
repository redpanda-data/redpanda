// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#pragma once

#include "lsm/core/internal/options.h"
#include "lsm/db/snapshot.h"
#include "lsm/db/version_set.h"
#include "lsm/io/persistence.h"

namespace lsm::db {

// Run a compaction task
ss::future<ss::lw_shared_ptr<version_edit>> run_compaction_task(
  io::data_persistence* persistence,
  snapshot_list* snapshots,
  version_set* versions,
  ss::lw_shared_ptr<internal::options> opts,
  compaction* compaction,
  ss::abort_source* as);

} // namespace lsm::db
