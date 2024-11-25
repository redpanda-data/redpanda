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
#include "config/property.h"
#include "kafka/server/fwd.h"
#include "kafka/server/handlers/handler_probe.h"
#include "kafka/server/logger.h"
#include "kafka/types.h"
#include "net/server.h"
#include "seastarx.h"
#include "security/acl.h"
#include "security/authorizer.h"
#include "security/mtls.h"
#include "security/sasl_authentication.h"
#include "ssx/abort_source.h"
#include "ssx/future-util.h"
#include "ssx/semaphore.h"
#include "utils/log_hist.h"
#include "utils/named_type.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/net/socket_defs.hh>

#include <absl/container/flat_hash_map.h>

#include <functional>
#include <memory>
#include <system_error>
#include <vector>

namespace kafka {

using response_ptr = ss::foreign_ptr<std::unique_ptr<response>>;

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

/*
 * authz failures should be quiet or logged at a reduced severity level.
 */
using authz_quiet = ss::bool_class<struct authz_quiet_tag>;

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
    ss::sstring client_id;
};

// Used to hold resources associated with a given request until
// the response has been send, as well as to track some statistics
// about the request.
//
// The resources in particular should be not be destroyed until
// the request is complete (e.g., all the information written to
// the socket so that no userspace buffers remain).
struct session_resources {
    using pointer = ss::lw_shared_ptr<session_resources>;

    ss::lowres_clock::duration backpressure_delay;
    ssx::semaphore_units memlocks;
    ssx::semaphore_units queue_units;
    std::unique_ptr<log_hist_internal::measurement> method_latency;
    std::unique_ptr<handler_probe::hist_t::measurement> handler_latency;
    std::unique_ptr<request_tracker> tracker;
    request_data request_data;
};

class connection_context final
  : public ss::enable_lw_shared_from_this<connection_context>
  , public boost::intrusive::list_base_hook<> {
public:
    connection_context(
      std::optional<std::reference_wrapper<
        boost::intrusive::list<connection_context>>> hook,
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
    ssx::sharded_abort_source& abort_source() { return _as; }
    bool abort_requested() const { return _as.abort_requested(); }
    const ss::sstring& listener() const { return conn->name(); }
    std::optional<security::sasl_server>& sasl() { return _sasl; }
    ss::future<> revoke_credentials(std::string_view name);

    template<typename T>
    security::auth_result authorized(
      security::acl_operation operation, const T& name, authz_quiet quiet);

    bool authorized_auditor() const {
        return get_principal() == security::audit_principal;
    }

    ss::future<> process();
    ss::future<> process_one_request();
    ss::net::inet_address client_host() const { return _client_addr; }
    uint16_t client_port() const { return conn ? conn->addr.port() : 0; }
    ss::socket_address local_address() const noexcept {
        return conn ? conn->local_address() : ss::socket_address{};
    }

    bool tls_enabled() const { return conn->tls_enabled(); }

private:
    template<typename T>
    security::auth_result authorized_user(
      security::acl_principal principal,
      security::acl_operation operation,
      const T& name,
      authz_quiet quiet);

    security::acl_principal get_principal() const {
        if (_mtls_state) {
            return _mtls_state->principal();
        } else if (_sasl) {
            return _sasl->principal();
        }
        // anonymous user
        return security::acl_principal{security::principal_type::user, {}};
    }

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
    delay_t record_tp_and_calculate_throttle(
      const request_header& hdr, size_t request_size);

    // Apply backpressure sequence, where the request processing may be
    // delayed for various reasons, including throttling but also because
    // too few server resources are available to accomodate the request
    // currently.
    // When the returned future resolves, the throttling period is over and
    // the associated resouces have been obtained and are tracked by the
    // contained session_resources object.
    ss::future<session_resources>
    throttle_request(const request_header&, size_t sz);

    /**
     * Process zero or more ready responses in request order.
     *
     * The future<> returned by this method resolves when all ready *and*
     * in-order responses have been processed, which is not the same as all
     * ready responses. In particular, responses which are ready may not be
     * processed if there are earlier (lower sequence number) responses
     * which are not yet ready: they will be processed by a future
     * invocation.
     *
     * @return ss::future<> a future which as described above.
     */
    ss::future<> maybe_process_responses();
    ss::future<> do_process(request_context);

    ss::future<> handle_auth_v0(size_t);

private:
    /**
     * Bundles together a response and its associated resources.
     */
    struct response_and_resources {
        response_ptr response;
        session_resources::pointer resources;
    };

    using sequence_id = named_type<uint64_t, struct kafka_protocol_sequence>;
    using map_t = absl::flat_hash_map<sequence_id, response_and_resources>;

    /*
     * dispatch_method_once is the first stage processing of a request and
     * handles work synchronously such as sequencing data off the connection.
     * handle_response waits for the response in the background as a second
     * stage of processing and allows for some request handling overlap.
     */
    ss::future<> dispatch_method_once(request_header, size_t sz);
    ss::future<> handle_response(
      ss::lw_shared_ptr<connection_context>,
      ss::future<response_ptr>,
      ss::lw_shared_ptr<session_resources>,
      sequence_id,
      correlation_id);

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
            if (klog.is_enabled(lvl)) {
                auto line_fmt = ss::sstring("{}:{} failed authorization - ")
                                + format;
                klog.log(
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
    class server& _server;
    ss::lw_shared_ptr<net::connection> conn;
    sequence_id _next_response;
    sequence_id _seq_idx;
    map_t _responses;
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

    /// What time the client on this conection should be throttled until
    /// Used to enforce client quotas and ingress/egress quotas broker-side
    /// if the client does not obey the ThrottleTimeMs in the response
    throttling_state _throttling_state;
};

} // namespace kafka
