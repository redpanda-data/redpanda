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
#include "kafka/server/protocol.h"
#include "kafka/server/response.h"
#include "net/server.h"
#include "seastarx.h"
#include "security/acl.h"
#include "security/sasl_authentication.h"
#include "utils/hdr_hist.h"
#include "utils/named_type.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>

#include <absl/container/flat_hash_map.h>

#include <memory>

namespace kafka {

/*
 * authz failures should be quiet or logged at a reduced severity level.
 */
using authz_quiet = ss::bool_class<struct authz_quiet_tag>;

struct request_header;
class request_context;

class connection_context final
  : public ss::enable_lw_shared_from_this<connection_context> {
public:
    connection_context(
      protocol& p,
      net::server::resources&& r,
      security::sasl_server sasl,
      bool enable_authorizer,
      bool use_mtls) noexcept
      : _proto(p)
      , _rs(std::move(r))
      , _sasl(std::move(sasl))
      // tests may build a context without a live connection
      , _client_addr(_rs.conn ? _rs.conn->addr.addr() : ss::net::inet_address{})
      , _enable_authorizer(enable_authorizer)
      , _authlog(_client_addr, client_port())
      , _use_mtls(use_mtls) {}

    ~connection_context() noexcept = default;
    connection_context(const connection_context&) = delete;
    connection_context(connection_context&&) = delete;
    connection_context& operator=(const connection_context&) = delete;
    connection_context& operator=(connection_context&&) = delete;

    protocol& server() { return _proto; }
    const ss::sstring& listener() const { return _rs.conn->name(); }
    security::sasl_server& sasl() { return _sasl; }

    template<typename T>
    bool authorized(
      security::acl_operation operation, const T& name, authz_quiet quiet) {
        // mtls configured?
        if (_use_mtls) {
            if (_mtls_principal.has_value()) {
                return authorized_user(
                  _mtls_principal.value(), operation, name, quiet);
            }
            return false;
        }
        // sasl configured?
        if (!_enable_authorizer) {
            return true;
        }
        auto user = sasl().principal();
        return authorized_user(std::move(user), operation, name, quiet);
    }

    template<typename T>
    bool authorized_user(
      ss::sstring user,
      security::acl_operation operation,
      const T& name,
      authz_quiet quiet) {
        security::acl_principal principal(
          security::principal_type::user, std::move(user));

        bool authorized = _proto.authorizer().authorized(
          name, operation, principal, security::acl_host(_client_addr));

        if (!authorized) {
            if (quiet) {
                vlog(
                  _authlog.debug,
                  "proto: {}, sasl state: {}, acl op: {}, principal: {}, "
                  "resource: {}",
                  _proto.name(),
                  security::sasl_state_to_str(_sasl.state()),
                  operation,
                  principal,
                  name);
            } else {
                vlog(
                  _authlog.info,
                  "proto: {}, sasl state: {}, acl op: {}, principal: {}, "
                  "resource: {}",
                  _proto.name(),
                  security::sasl_state_to_str(_sasl.state()),
                  operation,
                  principal,
                  name);
            }
        }

        return authorized;
    }

    ss::future<> process_one_request();
    bool is_finished_parsing() const;
    ss::net::inet_address client_host() const { return _client_addr; }
    uint16_t client_port() const {
        return _rs.conn ? _rs.conn->addr.port() : 0;
    }

private:
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
    // used to pass around some internal state
    struct session_resources {
        ss::lowres_clock::duration backpressure_delay;
        ss::semaphore_units<> memlocks;
        ss::semaphore_units<> queue_units;
        std::unique_ptr<hdr_hist::measurement> method_latency;
        std::unique_ptr<request_tracker> tracker;
    };

    /// called by throttle_request
    ss::future<ss::semaphore_units<>> reserve_request_units(size_t size);

    /// apply correct backpressure sequence
    ss::future<session_resources>
    throttle_request(const request_header&, size_t sz);

    ss::future<> handle_mtls_auth();
    ss::future<> dispatch_method_once(request_header, size_t sz);
    ss::future<> process_next_response();
    ss::future<> do_process(request_context);

    ss::future<> handle_auth_v0(size_t);

private:
    using sequence_id = named_type<uint64_t, struct kafka_protocol_sequence>;
    using map_t = absl::flat_hash_map<sequence_id, response_ptr>;

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

    protocol& _proto;
    net::server::resources _rs;
    sequence_id _next_response;
    sequence_id _seq_idx;
    map_t _responses;
    security::sasl_server _sasl;
    const ss::net::inet_address _client_addr;
    const bool _enable_authorizer;
    ctx_log _authlog;
    bool _response_loop_running{false};
    bool _use_mtls{false};
    std::optional<ss::sstring> _mtls_principal;
};

} // namespace kafka
