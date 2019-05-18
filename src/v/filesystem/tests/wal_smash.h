#pragma once

#include "filesystem/write_ahead_log.h"

#include <smf/random.h>

#include <map>

struct wal_smash_opts {
    seastar::sstring topic_namespace;
    int64_t ns_id;
    seastar::sstring topic;
    int64_t topic_id;
    int32_t topic_partitions = 16;
    wal_topic_type topic_type = wal_topic_type::wal_topic_type_regular;
    wal_compression_type record_compression_type
      = wal_compression_type::wal_compression_type_lz4;
    /// \brief after this threshold we use the @record_compression_type
    /// Only applies to the value. The key is always uncompressed
    int32_t record_compression_value_threshold = 512;
    /// \brief properties for the topic, immutable after creation
    std::unordered_map<seastar::sstring, seastar::sstring> topic_props{};
    /// Set default to 2MB
    int32_t consumer_max_read_bytes = 2048 * 1024;
    int32_t random_key_bytes = 20;
    int32_t random_val_bytes = 180;
    int32_t write_batch_size = 10;
};

/// \brief like HULK_SMASH for for the write ahead log
/// Only performs ops on this core-local write ahead log
///
class wal_smash {
public:
    struct wal_smash_stats {
        uint64_t bytes_written{0};
        uint64_t bytes_read{0};
        uint32_t reads{0};
        uint32_t writes{0};
    };
    explicit wal_smash(
      wal_smash_opts opt, seastar::distributed<write_ahead_log>* w);
    ~wal_smash() = default;

    seastar::future<> stop();

    seastar::future<std::unique_ptr<wal_create_reply>>
    create(std::vector<int32_t> partitions);

    seastar::future<std::unique_ptr<wal_write_reply>>
    write_one(int32_t partition);

    seastar::future<std::unique_ptr<wal_read_reply>>
    read_one(int32_t partition);

    inline const wal_smash_stats& stats() const {
        return _stats;
    }
    const wal_smash_opts& opts() const {
        return _opts;
    }
    const std::map<int32_t, std::vector<int32_t>>& core2partitions() const {
        return core_to_partitions_;
    }

private:
    wal_smash_opts _opts;
    seastar::distributed<write_ahead_log>* _wal;
    std::map<int32_t, std::vector<int32_t>> core_to_partitions_;
    struct offset_meta_idx {
        int64_t offset{0};
        seastar::semaphore lock{1};
    };
    // must be ordered.
    // key = partition
    // value = offsets & lock
    std::map<int32_t, offset_meta_idx> partition_offsets_{};
    smf::random _rand;
    wal_smash_stats _stats;
};
