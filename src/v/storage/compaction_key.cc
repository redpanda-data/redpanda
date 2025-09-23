// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/compaction_key.h"

#include "model/record_batch_types.h"

#include <algorithm>
#include <type_traits>

namespace storage {

compaction::compaction_key enhance_key(
  model::record_batch_type type, bool is_control_batch, bytes_view key) {
    auto bt_le = ss::cpu_to_le(
      static_cast<std::underlying_type_t<model::record_batch_type>>(type));
    auto ctrl_le = ss::cpu_to_le(static_cast<int8_t>(is_control_batch));
    auto total_size = sizeof(bt_le) + key.size() + sizeof(ctrl_le);
    bytes enriched_key(bytes::initialized_later{}, total_size);
    auto out = enriched_key.begin();
    out = std::copy_n(
      reinterpret_cast<const char*>(&bt_le), sizeof(bt_le), out);
    out = std::copy_n(
      reinterpret_cast<const char*>(&ctrl_le), sizeof(ctrl_le), out);
    std::copy_n(key.begin(), key.size(), out);
    return compaction::compaction_key(std::move(enriched_key));
}

} // namespace storage
