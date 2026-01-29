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

#include "lsm/core/internal/files.h"

#include "absl/strings/numbers.h"
#include "absl/strings/str_split.h"

#include <seastar/core/format.hh>

namespace lsm::internal {

ss::sstring sst_file_name(file_handle handle) noexcept {
    return ss::format("{:020}-{:020}.sst", handle.id(), handle.epoch);
}

std::optional<file_handle>
parse_sst_file_name(std::string_view filename) noexcept {
    size_t pos = filename.find_last_of('.');
    if (pos == std::string_view::npos) {
        return std::nullopt;
    }
    std::string_view stem = filename.substr(0, pos);
    std::string_view ext = filename.substr(pos);
    if (ext != ".sst") {
        return std::nullopt;
    }
    std::array<std::string_view, 2> split = absl::StrSplit(
      stem, absl::MaxSplits('-', 1));
    auto [str_id, str_epoch] = split;
    uint64_t raw_id = 0;
    if (!absl::SimpleAtoi(str_id, &raw_id)) {
        return std::nullopt;
    }
    uint64_t raw_epoch = 0;
    if (!absl::SimpleAtoi(str_epoch, &raw_epoch)) {
        return std::nullopt;
    }
    return file_handle{
      .id = file_id{raw_id},
      .epoch = database_epoch{raw_epoch},
    };
}

fmt::iterator file_handle::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{}", sst_file_name(*this));
}

} // namespace lsm::internal
