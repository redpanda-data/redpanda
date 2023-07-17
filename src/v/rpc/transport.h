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

#include "net/transport.h"
#include "outcome.h"
#include "reflection/async_adl.h"
#include "rpc/errc.h"
#include "rpc/parse_utils.h"
#include "rpc/response_handler.h"
#include "rpc/types.h"
#include "seastarx.h"
#include "utils/named_type.h"

#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/net/api.hh>
#include <seastar/net/tls.hh>

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>
#include <bits/stdint-uintn.h>

#include <concepts>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>

class rpc_integration_fixture_oc_ns_adl_serde_no_upgrade;
class rpc_integration_fixture_oc_ns_adl_only_no_upgrade;

namespace rpc {
struct client_context_impl;

class transport final : public net::base_transport {
public:
    explicit transport(
      transport_configuration c,
      std::optional<ss::sstring> service_name = std::nullopt);
    ~transport() override;
    transport(transport&&) noexcept = default;
    // semaphore is not move assignable
    transport& operator=(transport&&) noexcept = delete;
    transport(const transport&) = delete;
    transport& operator=(const transport&) = delete;

    ss::future<> connect(clock_type::time_point) final;
    ss::future<> connect(clock_type::duration);
    ss::future<result<std::unique_ptr<streaming_context>>>
      send(netbuf, rpc::client_opts);

    template<typename Input, typename Output>
    ss::future<result<client_context<Output>>>
      send_typed(Input, uint32_t, rpc::client_opts);

    template<typename Input, typename Output>
    ss::future<result<result_context<Output>>> send_typed_versioned(
      Input, uint32_t, rpc::client_opts, transport_version);

    void reset_state() final;

    transport_version version() const { return _version; }

private:
    using sequence_t = named_type<uint64_t, struct sequence_tag>;
    struct entry {
        std::unique_ptr<netbuf> buffer;
        client_opts::resource_units_t resource_units;
    };
    using requests_queue_t
      = absl::btree_map<sequence_t, std::unique_ptr<entry>>;
    friend client_context_impl;
    ss::future<> do_reads();
    ss::future<> dispatch(header);
    void fail_outstanding_futures() noexcept final;
    void setup_metrics(const std::optional<ss::sstring>&);

    ss::future<result<std::unique_ptr<streaming_context>>>
      do_send(sequence_t, netbuf, rpc::client_opts);
    void dispatch_send();
    ss::future<> do_dispatch_send();

    ss::future<result<std::unique_ptr<streaming_context>>>
    make_response_handler(netbuf&, const rpc::client_opts&, sequence_t);

    ssx::semaphore _memory;
    absl::flat_hash_map<uint32_t, std::unique_ptr<internal::response_handler>>
      _correlations;
    uint32_t _correlation_idx{0};
    ss::metrics::metric_groups _metrics;
    /**
     * ordered map containing in-flight requests. The map preserves order of
     * calling send_typed function. It is fine to use btree_map in here as it
     * ususally contains only few elements.
     */
    requests_queue_t _requests_queue;
    sequence_t _seq;
    sequence_t _last_seq;

    /*
     * version level used when dispatching requests. this value may change
     * during the lifetime of the transport. for example the version may be
     * upgraded if it is discovered that a server supports a newer version.
     *
     * reset to v1 in reset_state() to support reconnect_transport.
     */
    transport_version _version{transport_version::v1};

    friend class ::rpc_integration_fixture_oc_ns_adl_serde_no_upgrade;
    friend class ::rpc_integration_fixture_oc_ns_adl_only_no_upgrade;
    void set_version(transport_version v) { _version = v; }

    friend std::ostream& operator<<(std::ostream&, const transport&);
};

namespace internal {

inline errc map_server_error(status status) {
    switch (status) {
    case status::success:
        return errc::success;
    case status::request_timeout:
        return errc::client_request_timeout;
    case rpc::status::server_error:
        return errc::service_error;
    case status::method_not_found:
        return errc::method_not_found;
    case status::version_not_supported:
        return errc::version_not_supported;
    };
};

template<typename T>
ss::future<result<rpc::client_context<T>>> parse_result(
  ss::input_stream<char>& in,
  std::unique_ptr<streaming_context> sctx,
  transport_version req_ver) {
    using ret_t = result<rpc::client_context<T>>;

    const auto st = static_cast<status>(sctx->get_header().meta);
    const auto rep_ver = sctx->get_header().version;

    /*
     * the reply version should always be the same as the request version,
     * otherwise this is non-compliant behavior. the exception to this
     * rule is a v0 reply to a v1 request (ie talking to old v0 server).
     */
    const auto protocol_violation
      = rep_ver != req_ver
        && (req_ver != transport_version::v1 || rep_ver != transport_version::v0);

    if (unlikely(st != status::success || protocol_violation)) {
        if (st == status::version_not_supported) {
            /*
             * let version_not_supported take precedence over error handling for
             * protocol violations because the protocol violation may be due to
             * the unsupported version scenario.
             */
            sctx->signal_body_parse();
            return ss::make_ready_future<ret_t>(map_server_error(st));
        }
        if (protocol_violation) {
            auto msg = fmt::format(
              "Protocol violation: request version {} incompatible with "
              "reply version {} status {} reply type {}",
              req_ver,
              rep_ver,
              st,
              serde::type_str<T>());
            vlog(rpclog.error, "{}", msg);
            auto ex = std::make_exception_ptr(std::runtime_error(msg));
            sctx->body_parse_exception(ex);
            return ss::make_exception_future<ret_t>(ex);
        }
        sctx->signal_body_parse();
        return ss::make_ready_future<ret_t>(map_server_error(st));
    }

    return parse_type<T, default_message_codec>(in, sctx->get_header())
      .then_wrapped([sctx = std::move(sctx)](ss::future<T> data_fut) {
          if (data_fut.failed()) {
              const auto ex = data_fut.get_exception();
              sctx->body_parse_exception(ex);
              /**
               * we want to throw an exception when body parsing failed.
               * this will invalidate the connection since it may not be
               * valid any more.
               */
              std::rethrow_exception(ex);
          }
          sctx->signal_body_parse();
          return ret_t(rpc::client_context<T>(
            sctx->get_header(), std::move(data_fut.get())));
      });
}

} // namespace internal

template<typename Input, typename Output>
inline ss::future<result<client_context<Output>>>
transport::send_typed(Input r, uint32_t method_id, rpc::client_opts opts) {
    using ret_t = result<client_context<Output>>;
    return send_typed_versioned<Input, Output>(
             std::move(r), method_id, std::move(opts), _version)
      .then([](result<result_context<Output>> res) {
          if (!res) {
              return ss::make_ready_future<ret_t>(res.error());
          }
          return ss::make_ready_future<ret_t>(std::move(res.value().ctx));
      });
}

template<typename Input, typename Output>
inline ss::future<result<result_context<Output>>>
transport::send_typed_versioned(
  Input r,
  uint32_t method_id,
  rpc::client_opts opts,
  transport_version version) {
    using ret_t = result<result_context<Output>>;
    using ctx_t = result<std::unique_ptr<streaming_context>>;
    _probe.request();

    auto b = std::make_unique<rpc::netbuf>();
    b->set_compression(opts.compression);
    b->set_min_compression_bytes(opts.min_compression_bytes);
    auto raw_b = b.get();
    raw_b->set_service_method_id(method_id);

    auto& target_buffer = raw_b->buffer();
    auto seq = ++_seq;
    return encode_for_version(target_buffer, std::move(r), version)
      .then([this, version, b = std::move(b), seq, opts = std::move(opts)](
              transport_version effective_version) mutable {
          /*
           * enforce the rule that a transport configured as v0 behaves like
           * a v0 client transport and sends v0 messages.
           */
          vassert(
            version != transport_version::v0
              || effective_version == transport_version::v0,
            "Request type {} cannot be encoded at version {} (effective {}).",
            typeid(Input).name(),
            version,
            effective_version);
          b->set_version(effective_version);
          return do_send(seq, std::move(*b.get()), std::move(opts))
            .then([effective_version](ctx_t ctx) {
                return std::make_tuple(std::move(ctx), effective_version);
            });
      })
      .then_unpack([this](ctx_t sctx, transport_version req_ver) {
          if (!sctx) {
              return ss::make_ready_future<ret_t>(sctx.error());
          }
          const auto version = sctx.value()->get_header().version;
          return internal::parse_result<Output>(
                   _in, std::move(sctx.value()), req_ver)
            .then([this, version](result<client_context<Output>> r) {
                /*
                 * upgrade transport to v2 when:
                 * - at version v1 (do not upgrade from v0 -- for testing)
                 * - the response was handled/contains no errors
                 * - the response is v1,v2 (from a new server)
                 */
                if (
                  _version == transport_version::v1 && r.has_value()
                  && (version == transport_version::v1 || version == transport_version::v2)) {
                    vlog(rpclog.debug, "Upgrading connection from v1 to v2");
                    _version = transport_version::v2;
                }
                return ret_t(result_context<Output>{version, std::move(r)});
            });
      });
}

template<typename Protocol>
concept RpcClientProtocol = std::constructible_from<Protocol, rpc::transport&>;

template<typename... Protocol>
requires(RpcClientProtocol<Protocol>&&...) class client : public Protocol... {
public:
    explicit client(transport_configuration cfg)
      : Protocol(_transport)...
      , _transport(std::move(cfg)) {}

    ss::future<> connect(rpc::clock_type::time_point connection_timeout) {
        return _transport.connect(connection_timeout);
    }
    ss::future<> stop() { return _transport.stop(); };
    void shutdown() { _transport.shutdown(); }

    [[gnu::always_inline]] bool is_valid() const {
        return _transport.is_valid();
    }

    const net::unresolved_address& server_address() const {
        return _transport.server_address();
    }

private:
    rpc::transport _transport;
};

} // namespace rpc
