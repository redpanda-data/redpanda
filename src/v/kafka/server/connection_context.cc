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
#include "kafka/server/connection_context.h"

#include "base/likely.h"
#include "base/units.h"
#include "base/vlog.h"
#include "bytes/iobuf.h"
#include "bytes/iostream.h"
#include "bytes/scattered_message.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "kafka/protocol/sasl_authenticate.h"
#include "kafka/server/handlers/fetch.h"
#include "kafka/server/handlers/handler_interface.h"
#include "kafka/server/handlers/produce.h"
#include "kafka/server/logger.h"
#include "kafka/server/protocol_utils.h"
#include "kafka/server/quota_manager.h"
#include "kafka/server/request_context.h"
#include "kafka/server/response.h"
#include "kafka/server/sasl_probe.h"
#include "kafka/server/server.h"
#include "kafka/server/snc_quota_manager.h"
#include "net/exceptions.h"
#include "security/exceptions.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/scattered_message.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/coroutine/as_future.hh>

#include <chrono>
#include <cstdint>
#include <exception>
#include <memory>

using namespace std::chrono_literals;

namespace kafka {

namespace {
static constexpr std::string_view
  multi_proxy_initial_client_id("__redpanda_mpx");
/**
 * Exception thrown when virtual connection id provided in header client_id
 * field is not valid
 */
class invalid_virtual_connection_id : public std::exception {
public:
    explicit invalid_virtual_connection_id(ss::sstring msg)
      : _msg(std::move(msg)) {}

    const char* what() const noexcept final { return _msg.c_str(); }

private:
    ss::sstring _msg;
};

// Tuple containing virtual connection id and client id. It is returned after
// parsing parts of virtual connection id.
struct virtual_connection_client_id {
    virtual_connection_id v_connection_id;
    std::optional<std::string_view> client_id;
};

const std::regex hex_characters_regexp{R"REGEX(^[a-f0-9A-F]{8}$)REGEX"};

vcluster_connection_id
parse_vcluster_connection_id(const std::string& hex_str) {
    std::smatch matches;
    auto match = std::regex_match(
      hex_str.cbegin(), hex_str.cend(), matches, hex_characters_regexp);
    if (!match) {
        throw invalid_virtual_connection_id(fmt::format(
          "virtual cluster connection id '{}' is not a hexadecimal integer",
          hex_str));
    }

    vcluster_connection_id cid;

    std::stringstream sstream(hex_str);
    sstream >> std::hex >> cid;
    return cid;
}

/**
 * Virtual connection id is encoded as with the following structure:
 *
 * [vcluster_id][connection_id][actual client id]
 *
 * vcluster_id - is a string encoded XID representing virtual cluster (20
 *               characters)
 * connection_id - is a hex encoded 32 bit integer representing virtual
 *                 connection id (8 characters)
 *
 * client_id - standard protocol defined client id
 */
virtual_connection_client_id
parse_virtual_connection_id(const kafka::request_header& header) {
    static constexpr size_t connection_id_str_size
      = sizeof(vcluster_connection_id::type) * 2;
    static constexpr size_t v_connection_id_size = xid::str_size
                                                   + connection_id_str_size;
    if (header.client_id_buffer.empty()) {
        throw invalid_virtual_connection_id(
          "virtual connection client id can not be empty");
    }

    if (header.client_id->size() < v_connection_id_size) {
        throw invalid_virtual_connection_id(fmt::format(
          "virtual connection client id size must contain at least {} "
          "characters. Current size: {}",
          v_connection_id_size,
          header.client_id_buffer.size()));
    }
    try {
        virtual_connection_id connection_id{
          .virtual_cluster_id = xid::from_string(
            std::string_view(header.client_id->begin(), xid::str_size)),
          .connection_id = parse_vcluster_connection_id(std::string(
            std::next(header.client_id_buffer.begin(), xid::str_size),
            connection_id_str_size))};

        return virtual_connection_client_id{
          .v_connection_id = connection_id,
          // a reminder of client id buffer is used as a standard protocol
          // client_id.
          .client_id
          = header.client_id_buffer.size() == v_connection_id_size
              ? std::nullopt
              : std::make_optional<std::string_view>(
                  std::next(
                    header.client_id_buffer.begin(), v_connection_id_size),
                  header.client_id_buffer.size() - v_connection_id_size),
        };
    } catch (const invalid_xid& e) {
        throw invalid_virtual_connection_id(e.what());
    }
}
} // namespace
connection_context::connection_context(
  std::optional<
    std::reference_wrapper<boost::intrusive::list<connection_context>>> hook,
  class server& s,
  ss::lw_shared_ptr<net::connection> conn,
  std::optional<security::sasl_server> sasl,
  bool enable_authorizer,
  std::optional<security::tls::mtls_state> mtls_state,
  config::binding<uint32_t> max_request_size,
  config::conversion_binding<std::vector<bool>, std::vector<ss::sstring>>
    kafka_throughput_controlled_api_keys) noexcept
  : _hook(hook)
  , _server(s)
  , conn(conn)
  , _protocol_state()
  , _as()
  , _sasl(std::move(sasl))
  // tests may build a context without a live connection
  , _client_addr(conn ? conn->addr.addr() : ss::net::inet_address{})
  , _enable_authorizer(enable_authorizer)
  , _authlog(_client_addr, client_port())
  , _mtls_state(std::move(mtls_state))
  , _max_request_size(std::move(max_request_size))
  , _kafka_throughput_controlled_api_keys(
      std::move(kafka_throughput_controlled_api_keys)) {}

connection_context::~connection_context() noexcept = default;

ss::future<> connection_context::start() {
    co_await _as.start(_server.abort_source());
    if (conn) {
        ssx::background
          = conn->wait_for_input_shutdown()
              .finally([this]() {
                  vlog(
                    klog.debug,
                    "Connection input_shutdown; aborting operations for {}",
                    conn->addr);
                  return _as.request_abort_ex(std::system_error(
                    std::make_error_code(std::errc::connection_aborted)));
              })
              .finally([this]() { _wait_input_shutdown.set_value(); });
    } else {
        _wait_input_shutdown.set_value();
    }
    if (_hook) {
        _hook.value().get().push_back(*this);
    }
}

ss::future<> connection_context::stop() {
    if (_hook && is_linked()) {
        _hook.value().get().erase(_hook.value().get().iterator_to(*this));
    }
    if (conn) {
        vlog(klog.trace, "stopping connection context for {}", conn->addr);
        conn->shutdown_input();
    }
    co_await _wait_input_shutdown.get_future();
    co_await _as.request_abort_ex(ssx::connection_aborted_exception{});
    co_await _gate.close();
    co_await _as.stop();

    if (conn) {
        vlog(klog.trace, "stopped connection context for {}", conn->addr);
    }
}

template<typename T>
security::auth_result connection_context::authorized(
  security::acl_operation operation, const T& name, authz_quiet quiet) {
    // authorization disabled?
    if (!_enable_authorizer) {
        return security::auth_result::authz_disabled(
          get_principal(), security::acl_host(_client_addr), operation, name);
    }

    return authorized_user(get_principal(), operation, name, quiet);
}

template security::auth_result connection_context::authorized<model::topic>(
  security::acl_operation operation,
  const model::topic& name,
  authz_quiet quiet);

template security::auth_result connection_context::authorized<kafka::group_id>(
  security::acl_operation operation,
  const kafka::group_id& name,
  authz_quiet quiet);

template security::auth_result
connection_context::authorized<kafka::transactional_id>(
  security::acl_operation operation,
  const kafka::transactional_id& name,
  authz_quiet quiet);

template security::auth_result
connection_context::authorized<security::acl_cluster_name>(
  security::acl_operation operation,
  const security::acl_cluster_name& name,
  authz_quiet quiet);

template<typename T>
security::auth_result connection_context::authorized_user(
  security::acl_principal principal,
  security::acl_operation operation,
  const T& name,
  authz_quiet quiet) {
    auto authorized = _server.authorizer().authorized(
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

template security::auth_result
connection_context::authorized_user<model::topic>(
  security::acl_principal principal,
  security::acl_operation operation,
  const model::topic& name,
  authz_quiet quiet);

template security::auth_result
connection_context::authorized_user<kafka::group_id>(
  security::acl_principal principal,
  security::acl_operation operation,
  const kafka::group_id& name,
  authz_quiet quiet);

template security::auth_result
connection_context::authorized_user<kafka::transactional_id>(
  security::acl_principal principal,
  security::acl_operation operation,
  const kafka::transactional_id& name,
  authz_quiet quiet);

template security::auth_result
connection_context::authorized_user<security::acl_cluster_name>(
  security::acl_principal principal,
  security::acl_operation operation,
  const security::acl_cluster_name& name,
  authz_quiet quiet);

ss::future<> connection_context::revoke_credentials(std::string_view name) {
    if (
      !_sasl.has_value() || !_sasl->has_mechanism()
      || _sasl->mechanism().mechanism_name() != name) {
        return ss::now();
    }
    auto msg = _sasl->mechanism().complete()
                 ? fmt::format(
                     "Session for principal '{}' revoked", _sasl->principal())
                 : "Session for unknown client revoked";
    vlog(klog.info, "{}", msg);
    _server.sasl_probe().session_revoked();
    conn->shutdown_input();
    return ss::now();
}

ss::future<> connection_context::process() {
    while (true) {
        if (is_finished_parsing()) {
            break;
        }
        co_await process_one_request();
    }
}

ss::future<> connection_context::process_one_request() {
    auto sz = co_await protocol::parse_size(conn->input());
    if (!sz.has_value()) {
        co_return;
    }

    if (sz.value() > _max_request_size()) {
        throw net::invalid_request_error(fmt::format(
          "request size {} is larger than the configured max {}",
          sz,
          _max_request_size()));
    }

    /*
     * Intercept the wire protocol when:
     *
     * 1. sasl is enabled (implied by 2)
     * 2. during auth phase
     * 3. handshake was v0
     */
    if (unlikely(
          sasl()
          && sasl()->state() == security::sasl_server::sasl_state::authenticate
          && sasl()->handshake_v0())) {
        try {
            co_return co_await handle_auth_v0(*sz);
        } catch (...) {
            vlog(
              klog.info,
              "Detected error processing request: {}",
              std::current_exception());
            conn->shutdown_input();
        }
    }

    auto h = co_await parse_header(conn->input());
    _server.probe().add_bytes_received(sz.value());
    if (!h) {
        vlog(klog.debug, "could not parse header from client: {}", conn->addr);
        _server.probe().header_corrupted();
        co_return;
    }
    _server.handler_probe(h->key).add_bytes_received(sz.value());
    /**
     * An entry point for the MPX serverless extensions. If the first request
     * for a given connection has a special client_id then MPX extensions are
     * enabled for this physical connection.
     */
    if (_server.enable_mpx_extensions()) {
        if (unlikely(
              is_first_request()
              && h->client_id == multi_proxy_initial_client_id)) {
            vlog(
              klog.debug, "enabling virtualized connections on {}", conn->addr);
            _is_virtualized_connection = true;
        }
    }

    try {
        co_return co_await dispatch_method_once(
          std::move(h.value()), sz.value());
    } catch (const kafka_api_version_not_supported_exception& e) {
        vlog(
          klog.warn,
          "Error while processing request from {} - {}",
          conn->addr,
          e.what());
        conn->shutdown_input();
    } catch (const sasl_session_expired_exception& e) {
        vlog(
          klog.warn, "SASL session expired for {} - {}", conn->addr, e.what());
        _server.sasl_probe().session_expired();
        conn->shutdown_input();
    } catch (const std::bad_alloc&) {
        // In general, dispatch_method_once does not throw, but bad_allocs are
        // an exception. Log it cleanly to avoid this bubbling up as an
        // unhandled exceptional future.
        vlog(
          klog.error,
          "Request from {} failed on memory exhaustion (std::bad_alloc)",
          conn->addr);
    } catch (const invalid_virtual_connection_id& e) {
        // shutdown connection if it doesn't adhere to virtual connections
        // protocol
        vlog(
          klog.warn,
          "Request from {} contains invalid virtual connection id - {}",
          conn->addr,
          e.what());
        conn->shutdown_input();
    } catch (const ss::sleep_aborted&) {
        // shutdown started while force-throttling
    }
}

/*
 * The SASL authentication flow for a client using version 0 of SASL handshake
 * doesn't use an envelope request for tokens. This method intercepts the
 * authentication phase and builds an envelope request so that all of the normal
 * request processing can be re-used.
 *
 * Even though we build and decode a request/response, the payload is a small
 * authentication string. https://github.com/redpanda-data/redpanda/issues/1315.
 * When this ticket is complete we'll be able to easily remove this extra
 * serialization step and and easily operate on non-encoded requests/responses.
 */
ss::future<> connection_context::handle_auth_v0(const size_t size) {
    vlog(klog.debug, "Processing simulated SASL authentication request");
    vassert(sasl().has_value(), "sasl muct be enabled in order to handle sasl");

    /*
     * very generous upper bound for some added safety. generally the size is
     * small and corresponds to the representation of hashes being exchanged but
     * there is some flexibility as usernames, nonces, etc... have no strict
     * limits. future non-SCRAM mechanisms may have other size requirements.
     */
    if (unlikely(size > 256_KiB)) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format, "Auth (handshake_v0) message too large: {}", size));
    }

    const api_version version(0);
    iobuf request_buf;
    {
        auto data = co_await read_iobuf_exactly(conn->input(), size);
        sasl_authenticate_request request;
        request.data.auth_bytes = iobuf_to_bytes(data);
        protocol::encoder writer(request_buf);
        request.encode(writer, version);
    }

    sasl_authenticate_response response;
    {
        auto ctx = request_context(
          shared_from_this(),
          request_header{
            .key = sasl_authenticate_api::key,
            .version = version,
          },
          std::move(request_buf),
          0s);
        auto sres = session_resources{};
        auto resp = co_await kafka::process_request(
                      std::move(ctx), _server.smp_group(), sres)
                      .response;
        auto data = std::move(*resp).release();
        response.decode(std::move(data), version);
    }

    if (response.data.error_code != error_code::none) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "Auth (handshake v0) error {}: {}",
          response.data.error_code,
          response.data.error_message));
    }

    if (sasl()->state() == security::sasl_server::sasl_state::failed) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format, "Auth (handshake v0) failed with unknown error"));
    }

    iobuf data;
    protocol::encoder writer(data);
    writer.write(response.data.auth_bytes);
    auto msg = iobuf_as_scattered(std::move(data));
    co_await conn->write(std::move(msg));
}

bool connection_context::is_finished_parsing() const {
    return conn->input().eof() || abort_requested();
}

ss::future<connection_context::delay_t>
connection_context::record_tp_and_calculate_throttle(
  request_data r_data, const size_t request_size) {
    using clock = quota_manager::clock;
    static_assert(std::is_same_v<clock, delay_t::clock>);
    const auto now = clock::now();

    // Throttle on client based quotas
    connection_context::delay_t client_quota_delay{};
    if (r_data.request_key == fetch_api::key) {
        auto fetch_delay = co_await _server.quota_mgr().throttle_fetch_tp(
          r_data.client_id, now);
        auto fetch_enforced = _throttling_state.update_fetch_delay(
          fetch_delay, now);
        client_quota_delay = delay_t{
          .request = fetch_delay,
          .enforce = fetch_enforced,
        };
    } else if (r_data.request_key == produce_api::key) {
        auto produce_delay
          = co_await _server.quota_mgr().record_produce_tp_and_throttle(
            r_data.client_id, request_size, now);
        auto produce_enforced = _throttling_state.update_produce_delay(
          produce_delay, now);
        client_quota_delay = delay_t{
          .request = produce_delay,
          .enforce = produce_enforced,
        };
    }

    // Throttle on shard wide quotas
    connection_context::delay_t snc_delay;
    if (_kafka_throughput_controlled_api_keys().at(r_data.request_key)) {
        _server.snc_quota_mgr().get_or_create_quota_context(
          _snc_quota_context, r_data.client_id);
        _server.snc_quota_mgr().record_request_receive(
          *_snc_quota_context, request_size, now);
        auto shard_delays = _server.snc_quota_mgr().get_shard_delays(
          *_snc_quota_context);
        auto snc_enforced = _throttling_state.update_snc_delay(
          shard_delays.request, now);
        snc_delay = delay_t{
          .request = shard_delays.request,
          .enforce = snc_enforced,
        };
    }

    // Sum up
    const clock::duration delay_enforce = std::max(
      {snc_delay.enforce, client_quota_delay.enforce, clock::duration::zero()});
    const clock::duration delay_request = std::max(
      {snc_delay.request, client_quota_delay.request, clock::duration::zero()});
    if (
      delay_enforce != clock::duration::zero()
      || delay_request != clock::duration::zero()) {
        vlog(
          client_quota_log.trace,
          "[{}:{}] throttle request:{{snc:{}, client:{}}}, "
          "enforce:{{snc:{}, client:{}}}, key:{}, request_size:{}",
          _client_addr,
          client_port(),
          snc_delay.request,
          client_quota_delay.request,
          snc_delay.enforce,
          client_quota_delay.enforce,
          r_data.request_key,
          request_size);
    }
    co_return delay_t{.request = delay_request, .enforce = delay_enforce};
}

ss::future<session_resources> connection_context::throttle_request(
  const request_data r_data, size_t request_size) {
    // note that when throttling is first determined, the request is
    // allowed to pass through, and only subsequent requests are
    // delayed. this is a similar strategy used by kafka 2.0: the
    // response is important because it allows clients to
    // distinguish throttling delays from real delays. delays
    // applied to subsequent messages allow backpressure to take
    // affect.
    const delay_t delay = co_await record_tp_and_calculate_throttle(
      r_data, request_size);

    if (delay.enforce > delay_t::clock::duration::zero()) {
        vlog(
          klog.trace,
          "[{}:{}] enforcing throttling delay of {}",
          _client_addr,
          client_port(),
          delay.enforce);
        co_await ss::sleep_abortable(delay.enforce, abort_source().local());
    }

    auto mem_units = co_await reserve_request_units(
      r_data.request_key, request_size);

    auto qd_units = co_await server().get_request_unit();

    auto& h_probe = _server.handler_probe(r_data.request_key);
    auto tracker = std::make_unique<request_tracker>(_server.probe(), h_probe);
    auto track = track_latency(r_data.request_key);
    session_resources r{
      .backpressure_delay = delay.request,
      .memlocks = std::move(mem_units),
      .queue_units = std::move(qd_units),
      .tracker = std::move(tracker),
      .request_data = std::move(r_data)};
    if (track) {
        r.method_latency = _server.hist().auto_measure();
    }
    r.handler_latency = h_probe.auto_latency_measurement();
    co_return r;
}

ss::future<ssx::semaphore_units>
connection_context::reserve_request_units(api_key key, size_t size) {
    // Defer to the handler for the request type for the memory estimate, but
    // if the request isn't found, use the default estimate (although in that
    // case the request is likely for an API we don't support or malformed, so
    // it is likely to fail shortly anyway).
    auto handler = handler_for_key(key);
    auto mem_estimate = handler ? (*handler)->memory_estimate(size, *this)
                                : default_memory_estimate(size);
    if (unlikely(mem_estimate >= (size_t)std::numeric_limits<int32_t>::max())) {
        // TODO: Create error response using the specific API?
        throw std::runtime_error(fmt::format(
          "request too large > 1GB (size: {}, estimate: {}, API: {})",
          size,
          mem_estimate,
          handler ? (*handler)->name() : "<bad key>"));
    }
    auto fut = ss::get_units(_server.memory(), mem_estimate);
    if (_server.memory().waiters()) {
        _server.probe().waiting_for_available_memory();
    }
    return fut;
}

ss::future<>
connection_context::dispatch_method_once(request_header hdr, size_t size) {
    auto r_data = request_data{
      .request_key = hdr.key,
      .client_id = hdr.client_id
                     ? std::make_optional<ss::sstring>(*hdr.client_id)
                     : std::nullopt,
    };
    auto sres_in = co_await throttle_request(std::move(r_data), size);
    if (abort_requested()) {
        // protect against shutdown behavior
        co_return;
    }
    if (_kafka_throughput_controlled_api_keys().at(hdr.key)) {
        // Normally we can only get here after a prior call to
        // snc_quota_mgr().get_or_create_quota_context() in
        // record_tp_and_calculate_throttle(), but there is possibility
        // that the changing configuration could still take us into this
        // branch with unmatching (and even null) _snc_quota_context.
        // Simply an unmatching _snc_quota_context is no big deal because
        // it is a one off event, but we need protection from it being
        // nullptr
        if (likely(_snc_quota_context)) {
            _server.snc_quota_mgr().record_request_intake(
              *_snc_quota_context, size);
        }
    }

    auto sres = ss::make_lw_shared(std::move(sres_in));

    auto remaining = size - request_header_size - hdr.client_id_buffer.size()
                     - hdr.tags_size_bytes;
    auto buf = co_await read_iobuf_exactly(conn->input(), remaining);
    if (abort_requested()) {
        // _server._cntrl etc might not be alive
        co_return;
    }
    auto self = shared_from_this();
    auto rctx = request_context(
      self, std::move(hdr), std::move(buf), sres->backpressure_delay);

    /**
     * Not virtualized connection, simply forward to protocol state for request
     * processing.
     */
    if (
      !_is_virtualized_connection
      || rctx.header().client_id == multi_proxy_initial_client_id) {
        co_return co_await _protocol_state.process_request(
          shared_from_this(), std::move(rctx), sres);
    }
    auto client_connection_id = parse_virtual_connection_id(rctx.header());
    rctx.override_client_id(client_connection_id.client_id);
    vlog(
      klog.trace,
      "request from virtual connection {}, client id: {}",
      client_connection_id.v_connection_id,
      client_connection_id.client_id);

    auto it = _virtual_states.find(client_connection_id.v_connection_id);
    if (it == _virtual_states.end()) {
        auto p = _virtual_states.emplace(
          client_connection_id.v_connection_id,
          ss::make_lw_shared<virtual_connection_state>());
        it = p.first;
    }

    co_await it->second->process_request(
      shared_from_this(), std::move(rctx), sres);
}

ss::future<> connection_context::virtual_connection_state::process_request(
  ss::lw_shared_ptr<connection_context> connection_ctx,
  request_context rctx,
  ss::lw_shared_ptr<session_resources> sres) {
    auto u = co_await _lock.get_units();
    ssx::spawn_with_gate(
      connection_ctx->_gate,
      [this,
       rctx = std::move(rctx),
       sres,
       u = std::move(u),
       connection_ctx]() mutable {
          _last_request_timestamp = ss::lowres_clock::now();
          return _state
            .process_request(std::move(connection_ctx), std::move(rctx), sres)
            .finally([u = std::move(u)] {});
      });
}

ss::future<> connection_context::client_protocol_state::process_request(
  ss::lw_shared_ptr<connection_context> connection_ctx,
  request_context rctx,
  ss::lw_shared_ptr<session_resources> sres) {
    /*
     * we process requests in order since all subsequent requests
     * are dependent on authentication having completed.
     *
     * the other important reason for disabling pipeling is because
     * when a sasl handshake with version=0 is processed, the next
     * data on the wire is _not_ another request: it is a
     * size-prefixed authentication payload without a request
     * envelope, and requires special handling.
     *
     * a well behaved client should implicitly provide a data stream
     * that invokes this behavior in the server: that is, it won't
     * send auth data (or any other requests) until handshake or the
     * full auth-process completes, etc... but representing these
     * nuances of the protocol _explicitly_ in the server makes its
     * behavior easier to understand and avoids misbehaving clients
     * creating server-side errors that will appear as a corrupted
     * stream at best and at worst some odd behavior.
     */
    const auto correlation = rctx.header().correlation;
    const sequence_id seq = _seq_idx;
    _seq_idx = _seq_idx + sequence_id(1);
    auto res = kafka::process_request(
      std::move(rctx), connection_ctx->server().smp_group(), *sres);

    /*
     * first stage processed in a foreground.
     *
     * if the dispatch/first stage failed, then we need to
     * need to consume the second stage since it might be
     * an exceptional future.
     */
    auto dispatch = co_await ss::coroutine::as_future(
      std::move(res.dispatched));
    if (dispatch.failed()) {
        vlog(
          klog.info,
          "Detected error dispatching request: {}",
          dispatch.get_exception());
        try {
            co_await std::move(res.response);
        } catch (...) {
            vlog(
              klog.info,
              "Discarding second stage failure {}",
              std::current_exception());
        }
        connection_ctx->conn->shutdown_input();
        sres->tracker->mark_errored();
        co_return;
    }

    /**
     * second stage processed in background.
     */
    ssx::spawn_with_gate(
      connection_ctx->_server.conn_gate(),
      [this,
       f = std::move(res.response),
       sres,
       seq,
       correlation,
       cctx = connection_ctx]() mutable {
          return handle_response(
            std::move(cctx), std::move(f), sres, seq, correlation);
      });
}

ss::future<> connection_context::client_protocol_state::handle_response(
  ss::lw_shared_ptr<connection_context> connection_ctx,
  ss::future<response_ptr> f,
  ss::lw_shared_ptr<session_resources> sres,
  sequence_id seq,
  correlation_id correlation) {
    std::exception_ptr e;
    try {
        auto r = co_await std::move(f);
        r->set_correlation(correlation);
        response_and_resources randr{std::move(r), sres};
        _responses.insert({seq, std::move(randr)});
        co_return co_await maybe_process_responses(connection_ctx);
    } catch (...) {
        e = std::current_exception();
    }

    // on shutdown we don't bother to call shutdown_input on the connection, so
    // rely on any future reader to check the abort source before considering
    // reading the connection.
    if (ssx::is_shutdown_exception(e)) {
        vlog(klog.debug, "Shutdown error processing request: {}", e);
        co_return;
    }

    auto disconnected = net::is_disconnect_exception(e);
    if (disconnected) {
        vlog(
          klog.info,
          "Disconnected {} ({})",
          connection_ctx->conn->addr,
          disconnected.value());
    } else {
        vlog(klog.warn, "Error processing request: {}", e);
    }

    sres->tracker->mark_errored();
    connection_ctx->conn->shutdown_input();
}

/**
 * This method is called repeatedly to process the next request from the
 * connection until there are no more requests to process. The requests are
 * processed in request order. Since we proces the second stage asynchronously
 * within a given connection, reponses may become ready out of order, but Kafka
 * clients expect responses exactly in request order.
 *
 * The _responses queue handles that: responses are enqueued there in completion
 * order, but only sent to the client in response order. So this method, called
 * after every response is ready, may end up sending zero, one or more requests,
 * depending on the completion order.
 */
ss::future<ss::stop_iteration>
connection_context::client_protocol_state::do_process_responses(
  ss::lw_shared_ptr<connection_context> connection_ctx) {
    auto it = _responses.find(_next_response);
    if (it == _responses.end()) {
        co_return ss::stop_iteration::yes;
    }
    // found one; increment counter
    _next_response = _next_response + sequence_id(1);

    auto resp_and_res = std::move(it->second);

    _responses.erase(it);

    if (resp_and_res.response->is_noop()) {
        co_return ss::stop_iteration::no;
    }

    auto msg = response_as_scattered(std::move(resp_and_res.response));
    if (resp_and_res.resources->request_data.request_key == fetch_api::key) {
        co_await connection_ctx->_server.quota_mgr().record_fetch_tp(
          resp_and_res.resources->request_data.client_id, msg.size());
    }
    // Respose sizes only take effect on throttling at the next
    // request processing. The better way was to measure throttle
    // delay right here and apply it to the immediate response, but
    // that would require drastic changes to kafka message
    // processing framework - because throttle_ms has been
    // serialized long ago already. With the current approach,
    // egress token bucket level will always be an extra burst into
    // the negative while under pressure.
    auto response_size = msg.size();
    auto request_key = resp_and_res.resources->request_data.request_key;
    if (connection_ctx->_kafka_throughput_controlled_api_keys().at(
          request_key)) {
        // see the comment in dispatch_method_once()
        if (likely(connection_ctx->_snc_quota_context)) {
            connection_ctx->_server.snc_quota_mgr().record_response(
              *connection_ctx->_snc_quota_context, response_size);
        }
    }
    connection_ctx->_server.handler_probe(request_key)
      .add_bytes_sent(response_size);
    try {
        co_await connection_ctx->conn->write(std::move(msg));
    } catch (...) {
        resp_and_res.resources->tracker->mark_errored();
        vlog(
          klog.debug,
          "Failed to process request: {}",
          std::current_exception());
    }
    co_return ss::stop_iteration::no;
}

/**
 * This method processes as many responses as possible from the connection by
 * calling do_process_responses repeatedly until it returns stop_iteration::yes.
 */
ss::future<> connection_context::client_protocol_state::maybe_process_responses(
  ss::lw_shared_ptr<connection_context> connection_ctx) {
    return ss::repeat([this, connection_ctx]() {
        return do_process_responses(connection_ctx);
    });
}

std::ostream& operator<<(std::ostream& o, const virtual_connection_id& id) {
    fmt::print(
      o,
      "{{virtual_cluster_id: {}, connection_id: {}}}",
      id.virtual_cluster_id,
      id.connection_id);
    return o;
}

} // namespace kafka
