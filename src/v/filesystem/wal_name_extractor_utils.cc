#include "wal_name_extractor_utils.h"

#include <re2/re2.h>
#include <seastar/core/sstring.hh>


static const re2::RE2
  kEpochExtractor("([[:ascii:]]+[^\\d]+)?(\\d+)\\.(\\d+)\\.wal$");
static const re2::RE2 kPartitionExtractor("([[:ascii:]]+[^\\d]+)?(\\d+)$");

std::pair<int64_t, int64_t>
wal_name_extractor_utils::wal_segment_extract_epoch_term(
  const seastar::sstring &filename) {
  int64_t epoch = -1;
  int64_t term = -1;
  re2::RE2::FullMatch(filename.c_str(), kEpochExtractor, (void *)nullptr,
                      &epoch, &term);
  return {epoch, term};
}
bool
wal_name_extractor_utils::is_wal_segment(const seastar::sstring &filename) {
  return re2::RE2::FullMatch(filename.c_str(), kEpochExtractor, (void *)nullptr,
                             (void *)nullptr);
}
bool
wal_name_extractor_utils::is_valid_ns_topic_name(const char *filename) {
  // cannot include '.' in the regex to simplify life
  //
  static const re2::RE2 kWalName("^[[:ascii:]]+$");
  return re2::RE2::FullMatch(filename, kWalName);
}

bool
wal_name_extractor_utils::is_valid_partition_name(
  const seastar::sstring &filename) {
  return re2::RE2::FullMatch(filename.c_str(), kPartitionExtractor,
                             (void *)nullptr, (void *)nullptr);
}

int32_t
wal_name_extractor_utils::wal_partition_dir_extract(
  const seastar::sstring &filename) {
  int32_t retval = 0;
  re2::RE2::FullMatch(filename.c_str(), kPartitionExtractor, (void *)nullptr,
                      &retval);
  return retval;
}
