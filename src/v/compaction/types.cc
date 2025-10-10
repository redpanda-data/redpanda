// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "compaction/types.h"

#include "utils/to_string.h"

#include <fmt/core.h>
#include <fmt/ostream.h>

#include <ostream>

namespace compaction {

std::ostream& operator<<(std::ostream& o, const compaction_config& c) {
    fmt::print(
      o,
      "{{max_removable_local_log_offset:{}, "
      "max_tombstone_remove_offset:{}, "
      "max_tx_remove_offset:{}, "
      "should_sanitize:{}, "
      "tombstone_retention_ms:{}, "
      "tx_retention_ms:{}}}",
      c.max_removable_local_log_offset,
      c.max_tombstone_remove_offset,
      c.max_tx_remove_offset,
      c.sanitizer_config,
      c.tombstone_retention_ms,
      c.tx_retention_ms);
    return o;
}

std::ostream& operator<<(std::ostream& o, const stats& s) {
    auto control_batches_discarded_str
      = s.control_batches_discarded > 0
          ? fmt::format(
              ", control_batches_discarded: {}", s.control_batches_discarded)
          : "";
    auto offsets_processed_str = s.start_offset.has_value()
                                     && s.last_offset.has_value()
                                   ? fmt::format(
                                       ", offset_range: {}-{}",
                                       s.start_offset.value(),
                                       s.last_offset.value())
                                   : "";
    fmt::print(
      o,
      "{{ batches_processed: {}, batches_discarded: {}, "
      "records_discarded: {}, expired_tombstones_discarded: {}, "
      "non_compactible_batches: {}{}{}}}",
      s.batches_processed,
      s.batches_discarded,
      s.records_discarded,
      s.expired_tombstones_discarded,
      s.non_compactible_batches,
      control_batches_discarded_str,
      offsets_processed_str);
    return o;
}

} // namespace compaction
