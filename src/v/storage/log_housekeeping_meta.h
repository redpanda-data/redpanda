/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "container/intrusive_list_helpers.h"
#include "storage/log.h"

namespace storage {
struct log_housekeeping_meta {
    enum class bitflags : uint32_t {
        none = 0,
        compacted = 1U,
        lifetime_checked = 1U << 1U,
    };
    explicit log_housekeeping_meta(ss::shared_ptr<log> l) noexcept
      : handle(std::move(l)) {}

    ss::shared_ptr<log> handle;
    bitflags flags{bitflags::none};
    ss::lowres_clock::time_point last_compaction;

    intrusive_list_hook link;
};

inline log_housekeeping_meta::bitflags operator|(
  log_housekeeping_meta::bitflags a, log_housekeeping_meta::bitflags b) {
    return log_housekeeping_meta::bitflags(
      std::underlying_type_t<log_housekeeping_meta::bitflags>(a)
      | std::underlying_type_t<log_housekeeping_meta::bitflags>(b));
}

inline void operator|=(
  log_housekeeping_meta::bitflags& a, log_housekeeping_meta::bitflags b) {
    a = (a | b);
}

inline log_housekeeping_meta::bitflags
operator~(log_housekeeping_meta::bitflags a) {
    return log_housekeeping_meta::bitflags(
      ~std::underlying_type_t<log_housekeeping_meta::bitflags>(a));
}

inline log_housekeeping_meta::bitflags operator&(
  log_housekeeping_meta::bitflags a, log_housekeeping_meta::bitflags b) {
    return log_housekeeping_meta::bitflags(
      std::underlying_type_t<log_housekeeping_meta::bitflags>(a)
      & std::underlying_type_t<log_housekeeping_meta::bitflags>(b));
}

inline void operator&=(
  log_housekeeping_meta::bitflags& a, log_housekeeping_meta::bitflags b) {
    a = (a & b);
}

} // namespace storage
