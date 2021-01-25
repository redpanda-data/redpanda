// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/requests/requests.h"

#include "kafka/protocol/alter_configs.h"
#include "kafka/protocol/api_versions.h"
#include "kafka/protocol/create_topics.h"
#include "kafka/protocol/delete_topics.h"
#include "kafka/protocol/describe_configs.h"
#include "kafka/protocol/describe_groups.h"
#include "kafka/protocol/fetch.h"
#include "kafka/protocol/find_coordinator.h"
#include "kafka/protocol/heartbeat.h"
#include "kafka/protocol/incremental_alter_configs.h"
#include "kafka/protocol/init_producer_id.h"
#include "kafka/protocol/join_group.h"
#include "kafka/protocol/leave_group.h"
#include "kafka/protocol/list_groups.h"
#include "kafka/protocol/list_offsets.h"
#include "kafka/protocol/metadata.h"
#include "kafka/protocol/offset_commit.h"
#include "kafka/protocol/offset_fetch.h"
#include "kafka/protocol/produce.h"
#include "kafka/requests/request_context.h"
#include "kafka/protocol/sasl_authenticate.h"
#include "kafka/protocol/sasl_handshake.h"
#include "kafka/protocol/sync_group.h"
#include "utils/to_string.h"
#include "vlog.h"

#include <seastar/core/print.hh>
#include <seastar/util/log.hh>

namespace kafka {

/**
 * Dispatch request with version bounds checking.
 */
template<typename Request>
CONCEPT(requires(KafkaRequest<Request>))
struct process_dispatch {
    static ss::future<response_ptr>
    process(request_context&& ctx, ss::smp_service_group g) {
        if (
          ctx.header().version < Request::min_supported
          || ctx.header().version > Request::max_supported) {
            return ss::make_exception_future<response_ptr>(
              std::runtime_error(fmt::format(
                "Unsupported version {} for {} API",
                ctx.header().version,
                Request::name)));
        }
        return Request::process(std::move(ctx), g);
    }
};

/**
 * Dispatch API versions request without version checks.
 *
 * The version bounds checks are not applied to this request because the client
 * does not yet know what versions this server supports. The api versions
 * request is used by a client to query this information.
 */
template<>
struct process_dispatch<api_versions_api> {
    static ss::future<response_ptr>
    process(request_context&& ctx, ss::smp_service_group g) {
        return api_versions_api::process(std::move(ctx), g);
    }
};

template<typename Request>
CONCEPT(requires(KafkaRequest<Request>))
ss::future<response_ptr> do_process(
  request_context&& ctx, ss::smp_service_group g) {
    vlog(
      klog.trace,
      "Processing name:{}, key:{}, version:{} for {}",
      Request::name,
      ctx.header().key,
      ctx.header().version,
      ctx.header().client_id.value_or(std::string_view("unset-client-id")));

    return process_dispatch<Request>::process(std::move(ctx), g);
}

ss::future<response_ptr>
process_request(request_context&& ctx, ss::smp_service_group g) {
    switch (ctx.header().key) {
    case api_versions_api::key:
        return do_process<api_versions_api>(std::move(ctx), g);
    case metadata_api::key:
        return do_process<metadata_api>(std::move(ctx), g);
    case list_groups_api::key:
        return do_process<list_groups_api>(std::move(ctx), g);
    case find_coordinator_api::key:
        return do_process<find_coordinator_api>(std::move(ctx), g);
    case offset_fetch_api::key:
        return do_process<offset_fetch_api>(std::move(ctx), g);
    case produce_api::key:
        return do_process<produce_api>(std::move(ctx), g);
    case list_offsets_api::key:
        return do_process<list_offsets_api>(std::move(ctx), g);
    case offset_commit_api::key:
        return do_process<offset_commit_api>(std::move(ctx), g);
    case fetch_api::key:
        return do_process<fetch_api>(std::move(ctx), g);
    case join_group_api::key:
        return do_process<join_group_api>(std::move(ctx), g);
    case heartbeat_api::key:
        return do_process<heartbeat_api>(std::move(ctx), g);
    case leave_group_api::key:
        return do_process<leave_group_api>(std::move(ctx), g);
    case sync_group_api::key:
        return do_process<sync_group_api>(std::move(ctx), g);
    case create_topics_api::key:
        return do_process<create_topics_api>(std::move(ctx), g);
    case describe_configs_api::key:
        return do_process<describe_configs_api>(std::move(ctx), g);
    case alter_configs_api::key:
        return do_process<alter_configs_api>(std::move(ctx), g);
    case delete_topics_api::key:
        return do_process<delete_topics_api>(std::move(ctx), g);
    case describe_groups_api::key:
        return do_process<describe_groups_api>(std::move(ctx), g);
    case sasl_handshake_api::key:
        return do_process<sasl_handshake_api>(std::move(ctx), g);
    case sasl_authenticate_api::key:
        return do_process<sasl_authenticate_api>(std::move(ctx), g);
    case init_producer_id_api::key:
        return do_process<init_producer_id_api>(std::move(ctx), g);
    case incremental_alter_configs_api::key:
        return do_process<incremental_alter_configs_api>(std::move(ctx), g);
    };
    return ss::make_exception_future<response_ptr>(
      std::runtime_error(fmt::format("Unsupported API {}", ctx.header().key)));
}

std::ostream& operator<<(std::ostream& os, const request_header& header) {
    fmt::print(
      os,
      "{{key:{}, version:{}, correlation_id:{}, client_id:{}}}",
      header.key,
      header.version,
      header.correlation,
      header.client_id.value_or(std::string_view("nullopt")));
    return os;
}

std::ostream& operator<<(std::ostream& os, config_resource_type t) {
    switch (t) {
    case config_resource_type::topic:
        return os << "{topic}";
    case config_resource_type::broker:
        [[fallthrough]];
    case config_resource_type::broker_logger:
        break;
    }
    return os << "{unknown type}";
}

std::ostream& operator<<(std::ostream& os, describe_configs_source s) {
    switch (s) {
    case describe_configs_source::topic:
        return os << "{topic}";
    }
    return os << "{unknown type}";
}

} // namespace kafka
