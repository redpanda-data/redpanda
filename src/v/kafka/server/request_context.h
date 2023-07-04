/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "bytes/iobuf.h"
#include "cluster/security_frontend.h"
#include "kafka/protocol/fetch.h"
#include "kafka/protocol/fwd.h"
#include "kafka/protocol/types.h"
#include "kafka/protocol/wire.h"
#include "kafka/server/connection_context.h"
#include "kafka/server/fetch_session_cache.h"
#include "kafka/server/handlers/fetch/replica_selector.h"
#include "kafka/server/logger.h"
#include "kafka/server/response.h"
#include "kafka/server/server.h"
#include "kafka/server/usage_manager.h"
#include "kafka/types.h"
#include "pandaproxy/schema_registry/fwd.h"
#include "seastarx.h"
#include "security/fwd.h"
#include "ssx/abort_source.h"
#include "vlog.h"

#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/unaligned.hh>
#include <seastar/util/log.hh>

#include <memory>
#include <type_traits>

namespace kafka {

constexpr auto request_header_size = sizeof(int16_t) + sizeof(int16_t)
                                     + sizeof(correlation_id::type)
                                     + sizeof(int16_t);

struct request_header {
    api_key key;
    api_version version;
    correlation_id correlation;
    ss::temporary_buffer<char> client_id_buffer;
    std::optional<std::string_view> client_id;

    // value of std::nullopt indicates v0 request was parsed, 0 tag bytes will
    // be parsed. If this is non-null a v1 (flex) request header is parsed in
    // which the min number of bytes parsed must be at least 1.
    std::optional<tagged_fields> tags;
    size_t tags_size_bytes{0};
    bool is_flexible() const { return tags_size_bytes > 0; }

    friend std::ostream& operator<<(std::ostream&, const request_header&);
};

template<typename T>
concept has_throttle_time_ms = requires(T a) {
    { a.data.throttle_time_ms };
};

class request_context {
public:
    request_context(
      ss::lw_shared_ptr<connection_context> conn,
      request_header&& header,
      iobuf&& request,
      ss::lowres_clock::duration throttle_delay) noexcept
      : _conn(std::move(conn))
      , _request_size(request.size_bytes())
      , _header(std::move(header))
      , _reader(std::move(request))
      , _throttle_delay(throttle_delay) {}

    request_context(const request_context&) = delete;
    request_context& operator=(const request_context&) = delete;
    request_context(request_context&& o) noexcept = default;
    request_context& operator=(request_context&& o) noexcept = default;
    ~request_context() noexcept = default;

    const request_header& header() const { return _header; }

    ss::lw_shared_ptr<connection_context> connection() { return _conn; }

    ssx::sharded_abort_source& abort_source() { return _conn->abort_source(); }
    bool abort_requested() const { return _conn->abort_requested(); }

    protocol::decoder& reader() { return _reader; }

    latency_probe& probe() { return _conn->server().latency_probe(); }

    kafka::usage_manager& usage_mgr() const {
        return _conn->server().usage_mgr();
    }

    const cluster::metadata_cache& metadata_cache() const {
        return _conn->server().metadata_cache();
    }

    cluster::metadata_cache& metadata_cache() {
        return _conn->server().metadata_cache();
    }

    cluster::topics_frontend& topics_frontend() const {
        return _conn->server().topics_frontend();
    }

    quota_manager& quota_mgr() { return _conn->server().quota_mgr(); }

    ss::sharded<cluster::config_frontend>& config_frontend() const {
        return _conn->server().config_frontend();
    }

    ss::sharded<features::feature_table>& feature_table() const {
        return _conn->server().feature_table();
    }

    cluster::id_allocator_frontend& id_allocator_frontend() const {
        return _conn->server().id_allocator_frontend();
    }

    bool is_idempotence_enabled() {
        return _conn->server().is_idempotence_enabled();
    }

    bool are_transactions_enabled() {
        return _conn->server().are_transactions_enabled();
    }

    cluster::tx_gateway_frontend& tx_gateway_frontend() const {
        return _conn->server().tx_gateway_frontend();
    }

    const std::unique_ptr<pandaproxy::schema_registry::api>&
    schema_registry() const {
        return _conn->server().schema_registry();
    }

    std::chrono::milliseconds throttle_delay_ms() const {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
          _throttle_delay);
    }

    kafka::group_router& groups() { return _conn->server().group_router(); }

    cluster::shard_table& shards() { return _conn->server().shard_table(); }

    ss::sharded<cluster::partition_manager>& partition_manager() {
        return _conn->server().partition_manager();
    }

    fetch_session_cache& fetch_sessions() {
        return _conn->server().fetch_sessions_cache();
    }

    fetch_metadata_cache& get_fetch_metadata_cache() {
        return _conn->server().get_fetch_metadata_cache();
    }

    template<typename ResponseType>
    requires requires(
      ResponseType r, protocol::encoder& writer, api_version version) {
        { r.encode(writer, version) } -> std::same_as<void>;
    }
    ss::future<response_ptr> respond(ResponseType r) {
        /// Many responses contain a throttle_time_ms field, to prevent each
        /// handler from manually having to set this value, it can be done in
        /// one place here, with this concept check
        if constexpr (has_throttle_time_ms<ResponseType>) {
            /// Allow request handlers to override the throttle response, if
            /// multiple throttles detected, choose larger of the two
            r.data.throttle_time_ms = std::max(
              r.data.throttle_time_ms, throttle_delay_ms());
        }

        vlog(
          klog.trace,
          "[{}:{}] sending {}:{} for {}, response {}",
          _conn->client_host(),
          _conn->client_port(),
          ResponseType::api_type::key,
          ResponseType::api_type::name,
          _header.client_id,
          r);
        /// KIP-511 bumps api_versions_request/response to 3, past the first
        /// supported flex version for this API, and makes an exception
        /// that there will be no tags in the response header.
        auto is_flexible = flex_enabled(header().is_flexible());
        api_version version = header().version;
        if constexpr (std::is_same_v<ResponseType, api_versions_response>) {
            is_flexible = flex_enabled::no;
            if (r.data.error_code == kafka::error_code::unsupported_version) {
                /// Furthermore if the client has made an api_versions_request
                /// outside of the max supported version, any assumptions about
                /// its ability to understand a response at a given version
                /// cannot be made. In this case return api_versions_response at
                /// version 0.
                version = api_version(0);
            }
        }

        auto resp = std::make_unique<response>(is_flexible);
        r.encode(resp->writer(), version);
        update_usage_stats(r, resp->buf().size_bytes());
        return ss::make_ready_future<response_ptr>(std::move(resp));
    }

    coordinator_ntp_mapper& coordinator_mapper() {
        return _conn->server().coordinator_mapper();
    }

    cluster::tx_registry_frontend& tx_registry_frontend() {
        return _conn->server().tx_registry_frontend();
    }

    const ss::sstring& listener() const { return _conn->listener(); }
    std::optional<security::sasl_server>& sasl() { return _conn->sasl(); }
    security::credential_store& credentials() {
        return _conn->server().credentials();
    }

    template<typename T>
    bool authorized(
      security::acl_operation operation,
      const T& name,
      authz_quiet quiet = authz_quiet{false}) {
        return _conn->authorized(operation, name, quiet);
    }

    cluster::security_frontend& security_frontend() const {
        return _conn->server().security_frontend();
    }

    security::authorizer& authorizer() { return _conn->server().authorizer(); }

    cluster::controller_api& controller_api() {
        return _conn->server().controller_api();
    }

    ss::sharded<server>& server() { return _conn->server().container(); }

private:
    template<typename ResponseType>
    void update_usage_stats(const ResponseType& r, size_t response_size) {
        size_t internal_bytes_recv = 0;
        size_t internal_bytes_sent = 0;
        if constexpr (std::is_same_v<ResponseType, produce_response>) {
            internal_bytes_recv = r.internal_topic_bytes;
        } else if constexpr (std::is_same_v<ResponseType, fetch_response>) {
            internal_bytes_sent = r.internal_topic_bytes;
        }
        /// Bytes recieved by redpanda
        vassert(
          _request_size >= internal_bytes_recv,
          "Observed bigger internal bytes accounting then entire request size");
        usage_mgr().add_bytes_recv(_request_size - internal_bytes_recv);

        /// Bytes sent to redpanda
        vassert(
          response_size >= internal_bytes_sent,
          "Observed bigger internal bytes accounting then entire response "
          "size");
        usage_mgr().add_bytes_sent(response_size - internal_bytes_sent);
    }

private:
    ss::lw_shared_ptr<connection_context> _conn;
    size_t _request_size;
    request_header _header;
    protocol::decoder _reader;
    ss::lowres_clock::duration _throttle_delay;
};

// Executes the API call identified by the specified request_context.
process_result_stages process_request(
  request_context&&, ss::smp_service_group, const session_resources&);

bool track_latency(api_key);

} // namespace kafka
