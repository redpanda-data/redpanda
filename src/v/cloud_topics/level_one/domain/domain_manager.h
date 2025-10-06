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
class domain_manager {
public:
    explicit domain_manager(ss::shared_ptr<simple_stm> stm, io* io);

    void start();
    ss::future<> stop_and_wait();

    ss::future<rpc::add_objects_reply> add_objects(rpc::add_objects_request);

    ss::future<rpc::replace_objects_reply>
      replace_objects(rpc::replace_objects_request);

    ss::future<rpc::get_first_offset_ge_reply>
      get_first_offset_ge(rpc::get_first_offset_ge_request);

    ss::future<rpc::get_first_timestamp_ge_reply>
      get_first_timestamp_ge(rpc::get_first_timestamp_ge_request);

    ss::future<rpc::get_first_offset_for_bytes_reply>
      get_first_offset_for_bytes(rpc::get_first_offset_for_bytes_request);

    ss::future<rpc::get_offsets_reply> get_offsets(rpc::get_offsets_request);

    ss::future<rpc::get_compaction_offsets_reply>
      get_compaction_offsets(rpc::get_compaction_offsets_request);

    ss::future<rpc::get_term_for_offset_reply>
      get_term_for_offset(rpc::get_term_for_offset_request);

    ss::future<rpc::get_end_offset_for_term_reply>
      get_end_offset_for_term(rpc::get_end_offset_for_term_request);

    ss::future<rpc::set_start_offset_reply>
      set_start_offset(rpc::set_start_offset_request);

private:
    std::optional<ss::gate::holder> maybe_gate();
    ss::future<> gc_loop();

    ss::gate gate_;
    ss::abort_source as_;
    ss::shared_ptr<simple_stm> stm_;
    io* object_io_;
};

} // namespace cloud_topics::l1
