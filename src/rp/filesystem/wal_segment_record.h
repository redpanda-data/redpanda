#pragma once

#include <memory>

#include <seastar/core/sstring.hh>

#include "wal_generated.h"

namespace rp {

struct wal_segment_record {
  /// \brief writes into `wal_binary_recordT`
  /// [header + key + value] where the value might be compressed
  ///
  static std::unique_ptr<wal_binary_recordT>
  coalesce(const char *key, int32_t key_size, const char *val, int32_t val_size,
           wal_compression_type ctype =
             wal_compression_type::wal_compression_type_none);

  /// \brief return the size of a record
  static inline int32_t
  record_size(const wal_header &hdr) {
    return hdr.key_size() + hdr.value_size();
  }

  /// \brief return the checksum of the payload as int32_t
  /// internally we use xxhash64 & INT_MAX for speed
  ///
  static int32_t checksum(const char *p, std::size_t s);

  /// \brief create a header for a write
  static wal_header
  header_for(const char *key, int32_t key_size, const char *val,
             int32_t val_size,
             wal_compression_type ctype =
               wal_compression_type::wal_compression_type_none);

  /// \brief extracts the k=value pairs making a copy of them
  /// if the payload is compressed, it will decompress it correctly
  ///
  /// Note: this does no validation of the header
  ///       Most importantly it doesn't do a checksum of the payload.
  ///       If you care about that, please use the checksum function
  ///       We expect a pre-validated payload.
  ///
  static std::pair<seastar::temporary_buffer<char>,
                   seastar::temporary_buffer<char>>
  extract_from_bin(const char *begin, int32_t sz);
};
}  // namespace rp
