#pragma once

#include <utility>  // std::pair

#include <seastar/core/sstring.hh>

struct wal_name_extractor_utils {
  /// \brief extract a int64_t epoch out of a write ahead log name
  static std::pair<int64_t, int64_t>
  wal_segment_extract_epoch_term(const seastar::sstring &filename);

  /// \brief determines if is a wal_file or not
  static bool is_wal_segment(const seastar::sstring &filename);

  /// \brief determines if is a wal_file or not
  /// we use char* so we can use both for validation as well as
  /// for parsing filesystem
  static bool is_valid_ns_topic_name(const char *filename);

  /// \brief determines if is a wal_file or not
  static bool is_valid_partition_name(const seastar::sstring &filename);

  /// \brief extracts the partition out of the directory name
  static int32_t wal_partition_dir_extract(const seastar::sstring &filename);
};

