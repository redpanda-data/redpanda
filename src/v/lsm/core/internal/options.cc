// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "lsm/core/internal/options.h"

#include <utility>

namespace lsm::internal {

fmt::iterator options::level_config::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{number:{},max_total_bytes:{},max_file_size:{},compression:{}}}",
      number,
      max_total_bytes,
      max_file_size,
      std::to_underlying(compression));
}

fmt::iterator options::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{levels:{}}}", levels);
}

} // namespace lsm::internal
