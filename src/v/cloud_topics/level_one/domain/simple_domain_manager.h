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

#include "cloud_topics/level_one/domain/domain_manager.h"
#include "cloud_topics/level_one/metastore/simple_stm.h"

namespace cloud_topics::l1 {
class io;

// Encapsulates management of a given L1 metastore domain by wrapping a STM.
// Expected to be running on the leader replicas of the partition that backs
// the STM.
//
// The underlying STM itself focuses solely on persisting deterministic
// updates, while this:
// 1. wrangles additional aspects of these updates like concurrency, and
// 2. TODO: reconciles the STM state with cloud-recoverable state.
class simple_domain_manager final : public domain_manager {
public:
    explicit simple_domain_manager(ss::shared_ptr<simple_stm> stm, io* io);

    void start() override;
    ss::future<> stop_and_wait() override;

    ss::future<rpc::add_objects_reply>
      add_objects(rpc::add_objects_request) override;

    ss::future<rpc::replace_objects_reply>
      replace_objects(rpc::replace_objects_request) override;

    ss::future<rpc::get_first_offset_ge_reply>
      get_first_offset_ge(rpc::get_first_offset_ge_request) override;

    ss::future<rpc::get_first_timestamp_ge_reply>
      get_first_timestamp_ge(rpc::get_first_timestamp_ge_request) override;

    ss::future<rpc::get_first_offset_for_bytes_reply>
      get_first_offset_for_bytes(
        rpc::get_first_offset_for_bytes_request) override;

    ss::future<rpc::get_offsets_reply>
      get_offsets(rpc::get_offsets_request) override;

    ss::future<rpc::get_size_reply> get_size(rpc::get_size_request) override;

    ss::future<rpc::get_compaction_info_reply>
      get_compaction_info(rpc::get_compaction_info_request) override;

    ss::future<rpc::get_term_for_offset_reply>
      get_term_for_offset(rpc::get_term_for_offset_request) override;

    ss::future<rpc::get_end_offset_for_term_reply>
      get_end_offset_for_term(rpc::get_end_offset_for_term_request) override;

    ss::future<rpc::set_start_offset_reply>
      set_start_offset(rpc::set_start_offset_request) override;

    ss::future<rpc::remove_topics_reply>
      remove_topics(rpc::remove_topics_request) override;

    ss::future<rpc::get_compaction_infos_reply>
      get_compaction_infos(rpc::get_compaction_infos_request) override;

    ss::future<rpc::get_extent_metadata_reply>
      get_extent_metadata(rpc::get_extent_metadata_request) override;

    ss::future<rpc::flush_domain_reply>
      flush_domain(rpc::flush_domain_request) override;

    ss::future<rpc::restore_domain_reply>
      restore_domain(rpc::restore_domain_request) override;

    ss::future<std::expected<database_stats, rpc::errc>>
    get_database_stats() override;

    ss::future<rpc::preregister_objects_reply>
      preregister_objects(rpc::preregister_objects_request) override;

    ss::future<std::expected<void, rpc::errc>>
      write_debug_rows(chunked_vector<write_batch_row>) override;

    ss::future<std::expected<read_debug_rows_result, rpc::errc>>
    read_debug_rows(
      std::optional<ss::sstring> seek_key,
      std::optional<ss::sstring> last_key,
      uint32_t max_rows) override;

    ss::future<
      std::expected<partition_validation_result, partition_validator::error>>
      validate_partition(validate_partition_options) override;

private:
    std::optional<ss::gate::holder> maybe_gate();
    ss::future<> gc_loop();

    rpc::get_compaction_info_reply
    do_get_compaction_info(const state&, rpc::get_compaction_info_request);

    config::binding<std::chrono::milliseconds> gc_interval_;
    // This semaphore is used as a way to signal a change to
    // `cloud_topics_long_term_garbage_collection_interval` during the `wait()`
    // operation in the main garbage collection loop.
    ssx::semaphore sem_{0, "simple_domain_manager::gc_loop"};

    ss::gate gate_;
    ss::abort_source as_;
    ss::shared_ptr<simple_stm> stm_;
    io* object_io_;
};

} // namespace cloud_topics::l1
