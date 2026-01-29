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
  compaction compaction,
  ss::abort_source* as);

} // namespace lsm::db
