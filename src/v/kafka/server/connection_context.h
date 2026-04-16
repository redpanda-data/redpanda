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
#include "absl/container/flat_hash_map.h"
#include "absl/container/node_hash_map.h"
#include "base/seastarx.h"
#include "config/property.h"
#include "container/chunked_hash_map.h"
#include "kafka/protocol/types.h"
#include "kafka/server/fwd.h"
#include "kafka/server/handlers/details/security.h"
#include "kafka/server/handlers/handler_probe.h"
#include "kafka/server/logger.h"
#include "kafka/server/response.h"
#include "net/connection.h"
#include "net/server_probe.h"
#include "proto/redpanda/core/admin/v2/kafka_connections.proto.h"
#include "security/acl.h"
#include "security/authorizer.h"
#include "security/mtls.h"
#include "security/sasl_authentication.h"
#include "ssx/abort_source.h"
#include "ssx/mutex.h"
#include "ssx/semaphore.h"
#include "utils/log_hist.h"
#include "utils/named_type.h"
#include "utils/windowed_sum_tracker.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/net/socket_defs.hh>

#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <vector>

namespace kafka {

using closed_connections_t
  = std::deque<ss::lw_shared_ptr<const proto::admin::kafka_connection>>;

class kafka_api_version_not_supported_exception : public std::runtime_error {
public:
    explicit kafka_api_version_not_supported_exception(const std::string& m)
      : std::runtime_error(m) {}
};

class sasl_session_expired_exception : public std::runtime_error {
public:
    explicit sasl_session_expired_exception(const std::string& m)
      : std::runtime_error(m) {}
};

struct request_header;
class request_context;

// used to track number of pending requests
class request_tracker {
public:
    explicit request_tracker(
      net::server_probe& probe, handler_probe& h_probe) noexcept
      : _probe(probe)
      , _h_probe(h_probe) {
        _probe.request_received();
        _h_probe.request_started();
    }
    request_tracker(const request_tracker&) = delete;
    request_tracker(request_tracker&&) = delete;
    request_tracker& operator=(const request_tracker&) = delete;
    request_tracker& operator=(request_tracker&&) = delete;

    void mark_errored() { _errored = true; }

    ~request_tracker() noexcept {
        _probe.request_completed();
        if (_errored) {
            _h_probe.request_errored();
            _probe.service_error();
        } else {
            _h_probe.request_completed();
        }
    }

private:
    net::server_probe& _probe;
    handler_probe& _h_probe;
    bool _errored{false};
};

struct request_data {
    api_key request_key;
    std::optional<ss::sstring> client_id;
};

// Used to hold resources associated with a given request until
// the response has been send, as well as to track some statistics
// about the request.
//
// The resources in particular should be not be destroyed until
// the request is complete (e.g., all the information written to
// the socket so that no userspace buffers remain).
struct request_resources {
    using pointer = ss::lw_shared_ptr<request_resources>;

    ss::lowres_clock::duration backpressure_delay;
    ssx::semaphore_units memlocks;
    ssx::semaphore_units queue_units;
    std::unique_ptr<log_hist_internal::measurement> method_latency;
    std::unique_ptr<handler_probe::hist_t::measurement> handler_latency;
    std::unique_ptr<request_tracker> tracker;
    request_data request_data;
    ss::deleter response_resource_deleter;
};
using vcluster_connection_id
  = named_type<uint32_t, struct vcluster_connection_id_tag>;
/**
 * Struct representing virtual connection identifier. Each virtual cluster may
 * have multiple connections identified with connection_id.
 */
struct virtual_connection_id {
    xid virtual_cluster_id;
    vcluster_connection_id connection_id;

    template<typename H>
    friend H AbslHashValue(H h, const virtual_connection_id& id) {
        return H::combine(
          std::move(h), id.virtual_cluster_id, id.connection_id);
    }
    friend bool operator==(
      const virtual_connection_id&, const virtual_connection_id&) = default;

    friend std::ostream&
    operator<<(std::ostream& o, const virtual_connection_id& id);
};

class last_value {
public:
    void update(std::optional<std::string_view> new_value);
    template<typename Tag>
    void update(const named_type<ss::sstring, Tag>& new_value) {
        update(new_value());
    }
    template<typename Tag>
    void update(const std::optional<named_type<ss::sstring, Tag>>& new_value) {
        if (new_value) {
            update((*new_value)());
        }
    }
    const std::optional<ss::sstring>& get() const { return value; }

private:
    std::optional<ss::sstring> value{std::nullopt};
};

struct connection_attributes {
    using sys_clock = ss::lowres_system_clock;
    struct request_state {
        constexpr static auto bucket_count = size_t{4};
        constexpr static auto window_ns = std::chrono::minutes(1)
                                          / std::chrono::nanoseconds(1);
        using tracker_t = windowed_sum_tracker<bucket_count, window_ns>;
        tracker_t recent_stat{};
        uint64_t total_stat{0};

        void record(uint64_t val) {
            total_stat += val;
            recent_stat.record(val);
        }
    };

    class in_flight_request_tracker {
    public:
        using clock = ss::lowres_clock;
        constexpr static auto max_count = size_t{5};

        explicit in_flight_request_tracker(
          std::optional<clock::time_point> idle_since = clock::now())
          : _idle_since(idle_since) {}

        void record_begin_request(api_key key);
        void record_end_request();

        proto::admin::in_flight_requests to_proto(clock::time_point now) const;
        clock::duration get_idle_duration(clock::time_point now) const {
            return _idle_since ? (now - *_idle_since) : clock::duration::zero();
        }

    private:
        struct in_flight_request {
            api_key key;
            clock::time_point recv_time;
        };
        using queue_t = std::deque<in_flight_request>;

        queue_t _in_flight_request_samples{};
        size_t _total_in_flight_count{0};
        std::optional<clock::time_point> _idle_since;
    };

    void record_api_version(api_key key, api_version);

    request_state request_count;
    request_state produce_bytes;
    request_state produce_batch_count;
    request_state fetch_bytes;

    uuid_t connection_id{uuid_t::create()};
    sys_clock::time_point open_time{sys_clock::now()};
    last_value last_client_id{};
    last_value last_client_software_name{};
    last_value last_client_software_version{};
    last_value last_transactional_id{};
    last_value last_group_id{};
    last_value last_group_instance_id{};
    last_value last_group_member_id{};

    chunked_hash_map<api_key, api_version> api_versions{};
    in_flight_request_tracker in_flight_requests;
};

class connection_context final
  : public ss::enable_lw_shared_from_this<connection_context>
  , public boost::intrusive::list_base_hook<> {
public:
    connection_context(
      std::optional<std::reference_wrapper<
        boost::intrusive::list<connection_context>>> hook,
      std::optional<std::reference_wrapper<closed_connections_t>> closed_list,
      server& s,
      ss::lw_shared_ptr<net::connection> conn,
      std::optional<security::sasl_server> sasl,
      bool enable_authorizer,
      std::optional<security::tls::mtls_state> mtls_state,
      config::binding<uint32_t> max_request_size,
      config::conversion_binding<std::vector<bool>, std::vector<ss::sstring>>
        kafka_throughput_controlled_api_keys) noexcept;
    ~connection_context() noexcept;

    connection_context(const connection_context&) = delete;
    connection_context(connection_context&&) = delete;
    connection_context& operator=(const connection_context&) = delete;
    connection_context& operator=(connection_context&&) = delete;

    ss::future<> start();
    ss::future<> stop();

    /// The instance of \ref kafka::server on the shard serving the connection
    server& server() { return _server; }

    const class server& server() const { return _server; }
    ssx::sharded_abort_source& abort_source() { return _as; }
    bool abort_requested() const { return _as.abort_requested(); }
    const ss::sstring& listener() const { return conn->name(); }
    std::optional<security::sasl_server>& sasl() { return _sasl; }
    ss::future<> revoke_credentials(std::string_view name);

    template<typename T>
    security::auth_result authorized(
      security::acl_operation operation,
      const T& name,
      authz_quiet quiet,
      superuser_required superuser_required);

    bool authorized_auditor() const {
        return get_principal() == security::audit_principal;
    }

    // Returns true if the user is a superuser or if authz is disabled
    bool has_superuser_access() const;

    ss::future<> process();
    ss::future<> process_one_request();
    ss::net::inet_address client_host() const { return _client_addr; }
    uint16_t client_port() const { return conn ? conn->addr.port() : 0; }
    ss::socket_address local_address() const noexcept {
        return conn ? conn->local_address() : ss::socket_address{};
    }

    bool tls_enabled() const { return conn->tls_enabled(); }

    proto::admin::kafka_connection to_proto() const;

    connection_attributes& attributes() { return _attributes; }

    security::acl_principal get_principal() const {
        if (_mtls_state) {
            return _mtls_state->principal();
        } else if (_sasl && _sasl->complete()) {
            return _sasl->principal();
        }
        // anonymous user
        return security::acl_principal{security::principal_type::user, {}};
    }

private:
    template<typename T>
    security::auth_result authorized_user(
      security::acl_principal principal,
      security::acl_operation operation,
      const T& name,
      authz_quiet quiet,
      superuser_required superuser_required,
      const chunked_vector<security::acl_principal>& groups);

    const chunked_vector<security::acl_principal>& get_groups() const;

    bool is_finished_parsing() const;

    // Reserve units from memory from the memory semaphore in proportion
    // to the number of bytes the request procesisng is expected to
    // take.
    ss::future<ssx::semaphore_units>
    reserve_request_units(api_key key, size_t size);

    /// Calculated throttle delay pair.
    /// \p request is the primary throttle delay that should be applied now.
    /// In Kafka 2.0 compliant behaviour, it is only reported to the clients in
    /// the throttle_ms field, so that they can do the throttling on client
    /// side.
    /// \p enforce is the delay value that has not been implemented by the
    /// client on the last response, and has to be implemented here in the
    /// broker.
    struct delay_t {
        using clock = ss::lowres_clock;
        clock::duration request{};
        clock::duration enforce{};
    };

    /// Update throughput trackers (per-client, per-shard, and whatever are
    /// going to emerge) on ingress traffic and claculate aggregated throttle
    /// delays from all of them.
    ss::future<delay_t>
    record_tp_and_calculate_throttle(request_data r_data, size_t request_size);

    // Apply backpressure sequence, where the request processing may be
    // delayed for various reasons, including throttling but also because
    // too few server resources are available to accomodate the request
    // currently.
    // When the returned future resolves, the throttling period is over and
    // the associated resouces have been obtained and are tracked by the
    // contained request_resources object.
    ss::future<request_resources>
    throttle_request(request_data r_data, size_t sz);

    ss::future<> do_process(request_context);

    ss::future<> handle_auth_v0(size_t);

private:
    /**
     * Bundles together a response and its associated resources.
     */
    struct response_and_resources {
        response_ptr response;
        request_resources::pointer resources;
    };

    using sequence_id = named_type<uint64_t, struct kafka_protocol_sequence>;
    using map_t = chunked_hash_map<sequence_id, response_and_resources>;

    /*
     * dispatch_method_once is the first stage processing of a request and
     * handles work synchronously such as sequencing data off the connection.
     * handle_response waits for the response in the background as a second
     * stage of processing and allows for some request handling overlap.
     */
    ss::future<> dispatch_method_once(request_header, size_t sz);
    bool is_first_request() const {
        return _protocol_state.is_first_request() && _virtual_states.empty();
    }

    // Returns handler specific connection override if available.
    std::optional<ss::scheduling_group>
      get_scheduling_group_override(api_key) const;

    proto::admin::kafka_connection to_closed_proto() const;

    class ctx_log {
    public:
        ctx_log(const ss::net::inet_address& addr, uint16_t port)
          : _client_addr(addr)
          , _client_port(port) {}

        template<typename... Args>
        void error(const char* format, Args&&... args) {
            log(ss::log_level::error, format, std::forward<Args>(args)...);
        }
        template<typename... Args>
        void warn(const char* format, Args&&... args) {
            log(ss::log_level::warn, format, std::forward<Args>(args)...);
        }

        template<typename... Args>
        void info(const char* format, Args&&... args) {
            log(ss::log_level::info, format, std::forward<Args>(args)...);
        }

        template<typename... Args>
        void debug(const char* format, Args&&... args) {
            log(ss::log_level::debug, format, std::forward<Args>(args)...);
        }

        template<typename... Args>
        void trace(const char* format, Args&&... args) {
            log(ss::log_level::trace, format, std::forward<Args>(args)...);
        }

        template<typename... Args>
        void log(ss::log_level lvl, const char* format, Args&&... args) {
            if (kauthzlog.is_enabled(lvl)) {
                auto line_fmt = ss::sstring("{}:{} failed authorization - ")
                                + format;
                kauthzlog.log(
                  lvl,
                  line_fmt.c_str(),
                  _client_addr,
                  _client_port,
                  std::forward<Args>(args)...);
            }
        }

    private:
        // connection_context owns the original client_addr
        const ss::net::inet_address& _client_addr;
        uint16_t _client_port;
    };
    /**
     * Class aggregating a state of protocol per client. During normal operation
     * a connection context contains a single protocol state.
     */
    class client_protocol_state {
    public:
        /**
         * Standard process request.
         *
         * In this case the first phase of the request
         * processing is done in foreground (the first phase is the one that is
         * supposed to guarantee ordering) the second phase is handled in the
         * background allowing multiple concurrent produce/offset_commit
         * requests pending per client
         */
        ss::future<> process_request(
          ss::lw_shared_ptr<connection_context>,
          request_context,
          ss::lw_shared_ptr<request_resources>);

        /**
         * Checks if the request that currently is being processed is the first
         * request processed in the context this virtual connection
         */
        bool is_first_request() const {
            return _next_response == sequence_id(0)
                   && _seq_idx == sequence_id(0);
        }

    private:
        /**
         * Process zero or more ready responses in request order.
         *
         * The future<> returned by this method resolves when all ready *and*
         * in-order responses have been processed, which is not the same as all
         * ready responses. In particular, responses which are ready may not be
         * processed if there are earlier (lower sequence number) responses
         * which are not yet ready: they will be processed by a future
         * invocation.
         */
        ss::future<>
          maybe_process_responses(ss::lw_shared_ptr<connection_context>);
        ss::future<ss::stop_iteration>
          do_process_responses(ss::lw_shared_ptr<connection_context>);

        ss::future<> handle_response(
          ss::lw_shared_ptr<connection_context>,
          ss::future<response_ptr>,
          ss::lw_shared_ptr<request_resources>,
          sequence_id,
          correlation_id);

        sequence_id _next_response;
        sequence_id _seq_idx;
        ssx::semaphore _resp_sem{1, "k/conn_ctx/response"};
        map_t _responses;
    };

    /**
     * Class representing state of virtualized connection. It allow us to track
     * multiple sequences of request and responses in withing a single standard
     * connection context.
     *
     * This is the place to extend if any additional context will be required to
     * store per virtual connection. Currently only the only part that is kept
     * per virtual connection is a protocol state i.e. sequence and response
     * tracking.
     */
    class virtual_connection_state {
    public:
        /**
         * In this case the first phase of the request
         * processing is done in background if there is no request being
         * processed by the same client in the same time. This way from the
         * perspective of a physical connection, requests may be processed out
         * of order but for each client only one request may be processed at the
         * time.
         *
         * NOTE: to propagate backpressure and being able to control the
         * resource usage, only one first phase of a request at a time is
         * allowed to be processed by a client. If a previously dispatched
         * request first phase didn't finished the next request dispatch will
         * block processing requests for the whole connection.
         */
        ss::future<> process_request(
          ss::lw_shared_ptr<connection_context>,
          request_context,
          ss::lw_shared_ptr<request_resources>);

    private:
        client_protocol_state _state;
        /**
         * Mutex is used to control concurrency per virtual connection.
         */
        ssx::mutex _lock{"virtual_connection_state::lock"};
        ss::lowres_clock::time_point _last_request_timestamp;
    };

    class throttling_state {
    public:
        ss::lowres_clock::duration update_fetch_delay(
          ss::lowres_clock::duration new_delay,
          ss::lowres_clock::time_point now) {
            auto result_enforced = fetch_throttled_until - now;
            fetch_throttled_until = now + new_delay;
            return result_enforced;
        }

        ss::lowres_clock::duration update_produce_delay(
          ss::lowres_clock::duration new_delay,
          ss::lowres_clock::time_point now) {
            auto result_enforced = produce_throttled_until - now;
            produce_throttled_until = now + new_delay;
            return result_enforced;
        }

        ss::lowres_clock::duration update_snc_delay(
          ss::lowres_clock::duration new_delay,
          ss::lowres_clock::time_point now) {
            auto result_enforced = snc_throttled_until - now;
            snc_throttled_until = now + new_delay;
            return result_enforced;
        }

    private:
        ss::lowres_clock::time_point snc_throttled_until;
        ss::lowres_clock::time_point produce_throttled_until;
        ss::lowres_clock::time_point fetch_throttled_until;
    };

    std::optional<
      std::reference_wrapper<boost::intrusive::list<connection_context>>>
      _hook;
    std::optional<std::reference_wrapper<closed_connections_t>> _closed_list;
    class server& _server;
    ss::lw_shared_ptr<net::connection> conn;

    /**
     * We keep a separate instance of a protocol state not to lookup for the
     * state if we are not using virtualized connection. This may seem like an
     * overhead but considering the size of protocol state is a low price to pay
     * to prevent map lookups on each request.
     */
    client_protocol_state _protocol_state;
    /**
     * A map keeping virtual connection states, during default operation the map
     * is empty
     */
    chunked_hash_map<
      virtual_connection_id,
      ss::lw_shared_ptr<virtual_connection_state>>
      _virtual_states;

    ss::gate _gate;
    ssx::sharded_abort_source _as;
    std::optional<security::sasl_server> _sasl;
    const ss::net::inet_address _client_addr;
    const bool _enable_authorizer;
    ctx_log _authlog;
    std::optional<security::tls::mtls_state> _mtls_state;
    config::binding<uint32_t> _max_request_size;
    config::conversion_binding<std::vector<bool>, std::vector<ss::sstring>>
      _kafka_throughput_controlled_api_keys;
    std::unique_ptr<snc_quota_context> _snc_quota_context;
    ss::promise<> _wait_input_shutdown;

    bool _is_virtualized_connection = false;

    /// What time the client on this conection should be throttled until
    /// Used to enforce client quotas and ingress/egress quotas broker-side
    /// if the client does not obey the ThrottleTimeMs in the response
    throttling_state _throttling_state;

    connection_attributes _attributes;
};

} // namespace kafka
