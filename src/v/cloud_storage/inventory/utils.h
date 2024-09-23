/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "container/fragmented_vector.h"
#include "model/fundamental.h"

#include <seastar/core/file.hh>
#include <seastar/core/iostream.hh>

namespace cloud_storage::inventory {

// Writes hashes for a single NTP to a data file. The data file is
// located in path: namespace/topic/partition_id/{seq}. The parent
// directories are created if missing. A new data file is created for each
// flush operation.
ss::future<> flush_ntp_hashes(
  std::filesystem::path root,
  model::ntp ntp,
  fragmented_vector<uint64_t> hashes,
  uint64_t file_name);

} // namespace cloud_storage::inventory
