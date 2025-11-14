/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "redpanda/admin/services/internal/metastore.h"

#include "serde/protobuf/rpc.h"

#include <seastar/core/coroutine.hh>

namespace admin {

namespace {

[[noreturn]] void check_errc(cloud_topics::l1::metastore::errc ec) {
    switch (ec) {
    case cloud_topics::l1::metastore::errc::missing_ntp:
        throw serde::pb::rpc::not_found_exception("missing ntp");
    case cloud_topics::l1::metastore::errc::invalid_request:
        throw serde::pb::rpc::invalid_argument_exception();
    case cloud_topics::l1::metastore::errc::out_of_range:
        throw serde::pb::rpc::out_of_range_exception();
    case cloud_topics::l1::metastore::errc::transport_error:
        throw serde::pb::rpc::unavailable_exception("transport error");
    }
    throw serde::pb::rpc::unknown_exception();
}

} // namespace

seastar::future<proto::admin::metastore::get_offsets_response>
metastore_service_impl::get_offsets(
  serde::pb::rpc::context, proto::admin::metastore::get_offsets_request req) {
    const auto& topic_metadata = _topic_table->local().get_topic_metadata_ref(
      model::topic_namespace{
        model::kafka_namespace, model::topic{req.get_partition().get_topic()}});
    if (!topic_metadata) {
        throw serde::pb::rpc::not_found_exception("topic not found");
    }
    auto topic_id = topic_metadata->get().get_configuration().tp_id;
    if (!topic_id) {
        throw serde::pb::rpc::not_found_exception("topic missing id");
    }
    proto::admin::metastore::get_offsets_response response;
    auto result = co_await _metastore->local().get_offsets(
      {topic_id.value(),
       model::partition_id{req.get_partition().get_partition()}});
    if (!result) {
        check_errc(result.error());
    }
    proto::admin::metastore::offsets offsets;
    offsets.set_start_offset(result.value().start_offset());
    offsets.set_next_offset(result.value().next_offset());
    response.set_offsets(std::move(offsets));
    co_return response;
}

} // namespace admin
