/*
 * Copyright 2026 Redpanda Data, Inc.
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
