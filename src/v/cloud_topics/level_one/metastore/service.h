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

namespace experimental::cloud_topics::l1 {
class frontend;
} // namespace experimental::cloud_topics::l1

namespace experimental::cloud_topics::l1::rpc {
class service final : public impl::l1_rpc_service {
public:
    service(
      ss::scheduling_group, ss::smp_service_group, ss::sharded<frontend>*);

    ss::future<add_objects_reply>
    add_objects(add_objects_request, ::rpc::streaming_context&) override;

    ss::future<replace_objects_reply> replace_objects(
      replace_objects_request, ::rpc::streaming_context&) override;

    ss::future<get_first_offset_ge_reply> get_first_offset_ge(
      get_first_offset_ge_request, ::rpc::streaming_context&) override;

    ss::future<get_first_timestamp_ge_reply> get_first_timestamp_ge(
      get_first_timestamp_ge_request, ::rpc::streaming_context&) override;

    ss::future<get_offsets_reply>
    get_offsets(get_offsets_request, ::rpc::streaming_context&) override;

    ss::future<get_compaction_offsets_reply> get_compaction_offsets(
      get_compaction_offsets_request, ::rpc::streaming_context&) override;

private:
    ss::sharded<frontend>* _frontend;
};
} // namespace experimental::cloud_topics::l1::rpc
