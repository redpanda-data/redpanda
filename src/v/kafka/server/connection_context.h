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
#include "kafka/server/response.h"
#include "kafka/server/server.h"
#include "kafka/types.h"
#include "net/server.h"
#include "seastarx.h"
#include "security/acl.h"
#include "security/authorizer.h"
#include "security/mtls.h"
#include "security/sasl_authentication.h"
#include "ssx/semaphore.h"
#include "utils/hdr_hist.h"
#include "utils/named_type.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>

#include <absl/container/flat_hash_map.h>

#include <memory>

namespace kafka {

class kafka_api_version_not_supported_exception : public std::runtime_error {
public:
    explicit kafka_api_version_not_supported_exception(const std::string& m)
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
    explicit request_tracker(net::server_probe& probe) noexcept
      : _probe(probe) {
        _probe.request_received();
    }
    request_tracker(const request_tracker&) = delete;
    request_tracker(request_tracker&&) = delete;
    request_tracker& operator=(const request_tracker&) = delete;
    request_tracker& operator=(request_tracker&&) = delete;

    ~request_tracker() noexcept { _probe.request_completed(); }

private:
    net::server_probe& _probe;
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
    std::unique_ptr<hdr_hist::measurement> method_latency;
    std::unique_ptr<request_tracker> tracker;
    request_data request_data;
};

class connection_context final
  : public ss::enable_lw_shared_from_this<connection_context> {
public:
    connection_context(
      server& s,
      ss::lw_shared_ptr<net::connection> conn,
      std::optional<security::sasl_server> sasl,
      bool enable_authorizer,
      std::optional<security::tls::mtls_state> mtls_state,
      config::binding<uint32_t> max_request_size) noexcept
      : _server(s)
      , conn(conn)
      , _sasl(std::move(sasl))
      // tests may build a context without a live connection
      , _client_addr(conn ? conn->addr.addr() : ss::net::inet_address{})
      , _enable_authorizer(enable_authorizer)
      , _authlog(_client_addr, client_port())
      , _mtls_state(std::move(mtls_state))
      , _max_request_size(std::move(max_request_size)) {}

    ~connection_context() noexcept = default;
    connection_context(const connection_context&) = delete;
    connection_context(connection_context&&) = delete;
    connection_context& operator=(const connection_context&) = delete;
    connection_context& operator=(connection_context&&) = delete;

    server& server() { return _server; }
    const ss::sstring& listener() const { return conn->name(); }
    std::optional<security::sasl_server>& sasl() { return _sasl; }

    template<typename T>
    bool authorized(
      security::acl_operation operation, const T& name, authz_quiet quiet) {
        // authorization disabled?
        if (!_enable_authorizer) {
            return true;
        }

        auto get_principal = [this]() {
            if (_mtls_state) {
                return _mtls_state->principal();
            } else if (_sasl) {
                return _sasl->principal();
            }
            // anonymous user
            return security::acl_principal{security::principal_type::user, {}};
        };
        return authorized_user(get_principal(), operation, name, quiet);
    }

    template<typename T>
    bool authorized_user(
      security::acl_principal principal,
      security::acl_operation operation,
      const T& name,
      authz_quiet quiet) {
        bool authorized = _server.authorizer().authorized(
          name, operation, principal, security::acl_host(_client_addr));

        if (!authorized) {
            if (_sasl) {
                if (quiet) {
                    vlog(
                      _authlog.debug,
                      "proto: {}, sasl state: {}, acl op: {}, principal: {}, "
                      "resource: {}",
                      _server.name(),
                      security::sasl_state_to_str(_sasl->state()),
                      operation,
                      principal,
                      name);
                } else {
                    vlog(
                      _authlog.info,
                      "proto: {}, sasl state: {}, acl op: {}, principal: {}, "
                      "resource: {}",
                      _server.name(),
                      security::sasl_state_to_str(_sasl->state()),
                      operation,
                      principal,
                      name);
                }
            } else {
                if (quiet) {
                    vlog(
                      _authlog.debug,
                      "proto: {}, acl op: {}, principal: {}, resource: {}",
                      _server.name(),
                      operation,
                      principal,
                      name);
                } else {
                    vlog(
                      _authlog.info,
                      "proto: {}, acl op: {}, principal: {}, resource: {}",
                      _server.name(),
                      operation,
                      principal,
                      name);
                }
            }
        }

        return authorized;
    }

    ss::future<> process();
    ss::future<> process_one_request();
    ss::net::inet_address client_host() const { return _client_addr; }
    uint16_t client_port() const { return conn ? conn->addr.port() : 0; }

private:
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

    ss::future<> dispatch_method_once(request_header, size_t sz);

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

    class server& _server;
    ss::lw_shared_ptr<net::connection> conn;
    sequence_id _next_response;
    sequence_id _seq_idx;
    map_t _responses;
    std::optional<security::sasl_server> _sasl;
    const ss::net::inet_address _client_addr;
    const bool _enable_authorizer;
    ctx_log _authlog;
    std::optional<security::tls::mtls_state> _mtls_state;
    config::binding<uint32_t> _max_request_size;
    ss::lowres_clock::time_point _throttled_until;
};

} // namespace kafka
