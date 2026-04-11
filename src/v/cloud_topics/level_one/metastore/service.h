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

#include "base/seastarx.h"
#include "cloud_topics/level_one/metastore/rpc_service.h"
#include "cloud_topics/level_one/metastore/rpc_types.h"

#include <seastar/core/future.hh>

namespace cloud_topics::l1 {
class leader_router;
} // namespace cloud_topics::l1

namespace cloud_topics::l1::rpc {
class service final : public impl::l1_rpc_service {
public:
    service(
      ss::scheduling_group, ss::smp_service_group, ss::sharded<leader_router>*);

    ss::future<add_objects_reply>
    add_objects(add_objects_request, ::rpc::streaming_context&) override;

    ss::future<replace_objects_reply> replace_objects(
      replace_objects_request, ::rpc::streaming_context&) override;

    ss::future<set_start_offset_reply> set_start_offset(
      set_start_offset_request, ::rpc::streaming_context&) override;

    ss::future<remove_topics_reply>
    remove_topics(remove_topics_request, ::rpc::streaming_context&) override;

    ss::future<get_first_offset_ge_reply> get_first_offset_ge(
      get_first_offset_ge_request, ::rpc::streaming_context&) override;

    ss::future<get_first_timestamp_ge_reply> get_first_timestamp_ge(
      get_first_timestamp_ge_request, ::rpc::streaming_context&) override;

    ss::future<get_first_offset_for_bytes_reply> get_first_offset_for_bytes(
      get_first_offset_for_bytes_request, ::rpc::streaming_context&) override;

    ss::future<get_offsets_reply>
    get_offsets(get_offsets_request, ::rpc::streaming_context&) override;

    ss::future<get_size_reply>
    get_size(get_size_request, ::rpc::streaming_context&) override;

    ss::future<get_end_offset_for_term_reply> get_end_offset_for_term(
      get_end_offset_for_term_request, ::rpc::streaming_context&) override;

    ss::future<get_term_for_offset_reply> get_term_for_offset(
      get_term_for_offset_request, ::rpc::streaming_context&) override;

    ss::future<get_compaction_info_reply> get_compaction_info(
      get_compaction_info_request, ::rpc::streaming_context&) override;

    ss::future<get_compaction_infos_reply> get_compaction_infos(
      get_compaction_infos_request, ::rpc::streaming_context&) override;

    ss::future<get_extent_metadata_reply> get_extent_metadata(
      get_extent_metadata_request, ::rpc::streaming_context&) override;

    ss::future<flush_domain_reply>
    flush_domain(flush_domain_request, ::rpc::streaming_context&) override;

    ss::future<restore_domain_reply>
    restore_domain(restore_domain_request, ::rpc::streaming_context&) override;

    ss::future<preregister_objects_reply> preregister_objects(
      preregister_objects_request, ::rpc::streaming_context&) override;

private:
    ss::sharded<leader_router>* _leader_router;
};
} // namespace cloud_topics::l1::rpc
