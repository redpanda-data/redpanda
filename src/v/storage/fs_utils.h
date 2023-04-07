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
#include "model/fundamental.h"
#include "seastarx.h"
#include "storage/ntp_config.h"
#include "storage/version.h"

#include <seastar/core/print.hh>
#include <seastar/core/sstring.hh>

#include <boost/lexical_cast.hpp>

#include <filesystem>
#include <regex>

namespace storage {

struct segment_path {
    struct metadata {
        model::offset base_offset;
        model::term_id term;
        record_version_type version;
    };

    /// Construct an absolute path to a segment file.
    static std::filesystem::path make_segment_path(
      const ntp_config& ntp_config,
      const model::offset& base_offset,
      const model::term_id& term,
      const record_version_type& version) {
        return std::filesystem::path(ss::format(
          "{}/{}-{}-{}.log",
          ntp_config.work_directory(),
          base_offset(),
          term(),
          to_string(version)));
    }

    /// Parse metadata from a segment filename.
    static std::optional<metadata>
    parse_segment_filename(const ss::sstring& name) {
        const std::regex re(R"(^(\d+)-(\d+)-([\x00-\x7F]+).log$)");
        std::cmatch match;
        if (!std::regex_match(name.c_str(), match, re)) {
            return std::nullopt;
        }
        return metadata{
          .base_offset = model::offset(
            boost::lexical_cast<uint64_t>(match[1].str())),
          .term = model::term_id(boost::lexical_cast<int64_t>(match[2].str())),
          .version = from_string(match[3].str()),
        };
    }
};
struct ntp_directory_path {
    struct metadata {
        model::partition_id partition_id;
        model::revision_id revision_id;
    };

    /// Parse ntp directory name
    static std::optional<metadata>
    parse_partition_directory(const ss::sstring& name) {
        const std::regex re(R"(^(\d+)_(\d+)$)");
        std::cmatch match;
        if (!std::regex_match(name.c_str(), match, re)) {
            return std::nullopt;
        }
        return metadata{
          .partition_id = model::partition_id(
            boost::lexical_cast<uint64_t>(match[1].str())),
          .revision_id = model::revision_id(
            boost::lexical_cast<uint64_t>(match[2].str()))};
    }
};

} // namespace storage
