#pragma once
#include "model/fundamental.h"
#include "seastarx.h"
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
        int64_t term;
        record_version_type version;
    };

    /// Construct an absolute path to a segment file.
    static std::filesystem::path make_segment_path(
      const ss::sstring& base,
      const model::ntp& ntp,
      const model::offset& base_offset,
      const model::term_id& term,
      const record_version_type& version) {
        return std::filesystem::path(format(
          "{}/{}/{}-{}-{}.log",
          base,
          ntp.path(),
          base_offset(),
          term(),
          to_string(version)));
    }

    /// Parse metadata from a segment filename.
    static std::optional<metadata>
    parse_segment_filename(const ss::sstring& name) {
        const std::regex re("^(\\d+)-(\\d+)-([\\x00-\\x7F]+).log$");
        std::cmatch match;
        if (!std::regex_match(name.c_str(), match, re)) {
            return std::nullopt;
        }
        return metadata{
          .base_offset = model::offset(
            boost::lexical_cast<uint64_t>(match[1].str())),
          .term = boost::lexical_cast<int64_t>(match[2].str()),
          .version = from_string(match[3].str()),
        };
    }
};

} // namespace storage
