#include "filesystem/wal_name_extractor_utils.h"

#include <seastar/core/sstring.hh>

#include <regex>

static constexpr const char* epoch_extractor_pattern
  = "([\\x00-\\x7F]+[^\\d]+)?(\\d+)\\.(\\d+)\\.wal$";
static constexpr const char* partition_extractor_pattern
  = "([\\x00-\\x7F]+[^\\d]+)?(\\d+)$";

std::pair<int64_t, int64_t>
wal_name_extractor_utils::wal_segment_extract_epoch_term(
  const seastar::sstring& filename) {
    int64_t epoch = -1;
    int64_t term = -1;
    const std::regex re(epoch_extractor_pattern);
    std::cmatch base_match;

    if (std::regex_match(filename.c_str(), base_match, re)) {
        // The first sub_match is the whole string; the next
        // sub_match is the first parenthesized expression.
        if (base_match.size() == 4) {
            epoch = std::stol(base_match[2].str());
            term = std::stol(base_match[3].str());
        }
    }
    return {epoch, term};
}
bool wal_name_extractor_utils::is_wal_segment(
  const seastar::sstring& filename) {
    auto [e, t] = wal_name_extractor_utils::wal_segment_extract_epoch_term(
      filename);
    return e >= 0 && t >= 0;
}
bool wal_name_extractor_utils::is_valid_ns_topic_name(const char* filename) {
    // cannot include '.' in the regex to simplify life
    const std::regex re("^[\\x00-\\x7F]+$");
    return std::regex_match(filename, re);
}

bool wal_name_extractor_utils::is_valid_partition_name(
  const seastar::sstring& filename) {
    return wal_name_extractor_utils::wal_partition_dir_extract(filename) >= 0;
}

int32_t wal_name_extractor_utils::wal_partition_dir_extract(
  const seastar::sstring& filename) {
    int32_t retval = -1;
    const std::regex re(partition_extractor_pattern);
    std::cmatch base_match;
    if (std::regex_match(filename.c_str(), base_match, re)) {
        // The first sub_match is the whole string; the next
        // sub_match is the first parenthesized expression.
        if (base_match.size() == 3) {
            retval = std::stol(base_match[2].str());
        }
    }
    return retval;
}
