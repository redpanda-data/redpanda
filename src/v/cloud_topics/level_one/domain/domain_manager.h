/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "cloud_topics/level_one/metastore/lsm/write_batch_row.h"
#include "cloud_topics/level_one/metastore/partition_validator.h"
#include "cloud_topics/level_one/metastore/rpc_types.h"
#include "container/chunked_vector.h"

#include <cstddef>
#include <cstdint>
#include <string>

namespace cloud_topics::l1 {

// Summary information about a single SST file in the LSM tree.
struct lsm_file_info {
    uint64_t epoch;
    uint64_t id;
    uint64_t size_bytes;
    std::string smallest_key_info;
    std::string largest_key_info;
};

// Information about a single level in the LSM tree.
struct lsm_level_info {
    int32_t level_number;
    chunked_vector<lsm_file_info> files;
};

struct database_stats {
    size_t active_memtable_bytes{0};
    size_t immutable_memtable_bytes{0};
    size_t total_size_bytes{0};
    std::vector<lsm_level_info> levels;
};

// Abstract base class for domain managers.
// Defines the interface used by leader_router to interact with domain managers.
class domain_manager {
public:
    domain_manager() = default;
    domain_manager(const domain_manager&) = delete;
    domain_manager(domain_manager&&) = delete;
    domain_manager& operator=(const domain_manager&) = delete;
    domain_manager& operator=(domain_manager&&) = delete;
    virtual ~domain_manager() = default;

    virtual void start() = 0;
    virtual ss::future<> stop_and_wait() = 0;

    virtual ss::future<rpc::add_objects_reply>
      add_objects(rpc::add_objects_request) = 0;

    virtual ss::future<rpc::replace_objects_reply>
      replace_objects(rpc::replace_objects_request) = 0;

    virtual ss::future<rpc::get_first_offset_ge_reply>
      get_first_offset_ge(rpc::get_first_offset_ge_request) = 0;

    virtual ss::future<rpc::get_first_timestamp_ge_reply>
      get_first_timestamp_ge(rpc::get_first_timestamp_ge_request) = 0;

    virtual ss::future<rpc::get_first_offset_for_bytes_reply>
      get_first_offset_for_bytes(rpc::get_first_offset_for_bytes_request) = 0;

    virtual ss::future<rpc::get_offsets_reply>
      get_offsets(rpc::get_offsets_request) = 0;

    virtual ss::future<rpc::get_size_reply> get_size(rpc::get_size_request) = 0;

    virtual ss::future<rpc::get_compaction_info_reply>
      get_compaction_info(rpc::get_compaction_info_request) = 0;

    virtual ss::future<rpc::get_term_for_offset_reply>
      get_term_for_offset(rpc::get_term_for_offset_request) = 0;

    virtual ss::future<rpc::get_end_offset_for_term_reply>
      get_end_offset_for_term(rpc::get_end_offset_for_term_request) = 0;

    virtual ss::future<rpc::set_start_offset_reply>
      set_start_offset(rpc::set_start_offset_request) = 0;

    virtual ss::future<rpc::remove_topics_reply>
      remove_topics(rpc::remove_topics_request) = 0;

    virtual ss::future<rpc::get_compaction_infos_reply>
      get_compaction_infos(rpc::get_compaction_infos_request) = 0;

    virtual ss::future<rpc::get_extent_metadata_reply>
      get_extent_metadata(rpc::get_extent_metadata_request) = 0;

    virtual ss::future<rpc::flush_domain_reply>
      flush_domain(rpc::flush_domain_request) = 0;

    virtual ss::future<rpc::restore_domain_reply>
      restore_domain(rpc::restore_domain_request) = 0;

    virtual ss::future<std::expected<database_stats, rpc::errc>>
    get_database_stats() = 0;

    virtual ss::future<rpc::preregister_objects_reply>
      preregister_objects(rpc::preregister_objects_request) = 0;

    virtual ss::future<std::expected<void, rpc::errc>>
      write_debug_rows(chunked_vector<write_batch_row>) = 0;

    struct read_debug_rows_result {
        chunked_vector<write_batch_row> rows;
        std::optional<ss::sstring> next_key;
    };

    virtual ss::future<std::expected<read_debug_rows_result, rpc::errc>>
    read_debug_rows(
      std::optional<ss::sstring> seek_key,
      std::optional<ss::sstring> last_key,
      uint32_t max_rows) = 0;

    virtual ss::future<
      std::expected<partition_validation_result, partition_validator::error>>
      validate_partition(validate_partition_options) = 0;
};

} // namespace cloud_topics::l1
