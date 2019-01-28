#pragma once

#include <cstdint>

#include <bytell_hash_map.hpp>
#include <seastar/core/sstring.hh>

#include "filesystem/wal_generated.h"

namespace v {
namespace api {

struct client_opts {
  enum class offset_strategy { stored_offset, begining, latest };

  explicit client_opts(seastar::sstring _topic_namespace,
                       seastar::sstring _topic, int64_t _consumer_group_id,
                       int64_t _producer_id);
  ~client_opts() = default;

  /// \brief namespace for topic; immutable after creation
  seastar::sstring topic_namespace;
  int64_t ns_id;

  /// \brief topic name; immutable after creation
  seastar::sstring topic;
  int64_t topic_id;

  bool server_side_verify_payload = true;

  /// \brief numer or partitions per topic; immutable after creation
  int32_t topic_partitions = 16;

  /// \brief can be regular or compaction topic; immutable after creation
  v::wal_topic_type topic_type = v::wal_topic_type::wal_topic_type_regular;

  /// \brief use this compression type on every record after the
  /// @record_compression_threshold
  v::wal_compression_type record_compression_type =
    v::wal_compression_type::wal_compression_type_lz4;

  /// \brief after this threshold we use the @record_compression_type
  /// Only applies to the value. The key is always uncompressed
  int32_t record_compression_value_threshold = 512;

  /// \brief properties for the topic, immutable after creation
  ska::bytell_hash_map<seastar::sstring, seastar::sstring> topic_props{};

  /// \brief falls back on begining if nothing present
  offset_strategy strategy = offset_strategy::stored_offset;

  /// \brief consumer group - allows a load balancer server side for a
  /// particular namespace/topic; up to a max of # of partitions
  /// - typically use xxhash("Some Human Readable Name")
  int64_t consumer_group_id;

  /// \brief 1MB.
  int32_t consumer_max_read_bytes = 1024 * 1024;

  /// \brief producer id - allows to dedup requests in case of failure-retry
  /// - typically use xxhash("Some Human Readable Name")
  int64_t producer_id;

  /// \brief - expensive - mostly useful for benchmarking / debugging.
  /// it uses about 185KB of memory to store an HDR Histogram instance
  /// and measure every RPC latency
  bool enable_detailed_latency_metrics = false;

  // XXX - missing TLS opts. No need to be part of the props
};

}  // namespace api
}  // namespace v
