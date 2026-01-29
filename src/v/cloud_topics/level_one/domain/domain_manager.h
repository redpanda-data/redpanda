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

#include "cloud_topics/level_one/metastore/rpc_types.h"

namespace cloud_topics::l1 {

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
};

} // namespace cloud_topics::l1
