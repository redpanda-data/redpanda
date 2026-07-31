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

#include "absl/container/btree_map.h"
#include "absl/container/flat_hash_map.h"
#include "base/format_to.h"
#include "base/outcome.h"
#include "base/seastarx.h"
#include "container/chunked_hash_map.h"
#include "metrics/metrics.h"
#include "model/metadata.h"
#include "net/transport.h"
#include "reflection/async_adl.h"
#include "rpc/errc.h"
#include "rpc/parse_utils.h"
#include "rpc/response_handler.h"
#include "rpc/types.h"
#include "utils/named_type.h"

#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/net/api.hh>
#include <seastar/net/tls.hh>

#include <bits/stdint-uintn.h>

#include <concepts>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <utility>

class rpc_integration_fixture_oc_ns_adl_serde_no_upgrade;
class rpc_integration_fixture_oc_ns_adl_only_no_upgrade;

namespace rpc {
struct client_context_impl;

/**
 * @brief A structure for tracking various points in time associated with a
 * specific RPC request, for more detailed diagnosis when an RPC times out.
 */
struct timing_info {
    using clock_type = rpc::clock_type;
    using time_point = clock_type::time_point;

    constexpr static time_point unset = time_point::min();

    /**
     * The originally configured timeout, usually created when the client_opts
     * object is constructed.
     */
    timeout_spec timeout = timeout_spec::none;

    /**
     * The moment in time the request was enqueued in the _requests array, i.e.,
     * now waiting in line to be sent. This
     * is often immediately followed by being dispatched, though not always:
     * since we dispatch in-order, any lower-sequence number requests which
     * haven't been dispatched yet will prevent this from being dispatched.
     */
    time_point enqueued_at = unset;

    /**
     * Moment in time the semaphore units needed for request buffer are
     * reserved. The request is not dispatched until the required units are
     * acquired.
     */
    time_point memory_reserved_at = unset;

    /**
     * The moment in time we dispatched the request: that is, it was the next
     * request to be sent and we called .write on the output stream: note that
     * this does not perform the write (since that's an async method), but it
     * means that the task responsible for doing the write has been set up.
     */
    time_point dispatched_at = unset;

    /**
     * The moment in time the future associated with the write to the output
     * stream completes. As this is a buffered_output_stream, it does not
     * necessarily mean the underlying stream has been flushed (as this happens
     * only sometimes), so doesn't even mean the kernel has been notified of the
     * buffers yet.
     */
    time_point written_at = unset;

    /**
     * True if the batched output stream write associated with this request was
     * flushed at the time of writing. That is, the written_at timestamp is set
     * when the write occurs, but the output stream will internally decide
     * whether to flush not depending on concurrent writers
     *
     */
    bool flushed = false;
};

class client_probe : public net::client_probe {
public:
    client_probe() = default;
    client_probe(const client_probe&) = delete;
    client_probe& operator=(const client_probe&) = delete;
    client_probe(client_probe&&) = delete;
    client_probe& operator=(client_probe&&) = delete;
    ~client_probe() = default;

    void request() { ++_requests; }

    void request_completed() { ++_requests_completed; }

    void request_timeout() { ++_request_timeouts; }

    void request_error() { ++_request_errors; }

    void add_bytes_sent(size_t sent) { _out_bytes += sent; }

    void add_bytes_received(size_t recv) { _in_bytes += recv; }

    void read_dispatch_error() { ++_read_dispatch_errors; }

    void header_corrupted() { ++_corrupted_headers; }

    void client_correlation_error() { ++_client_correlation_errors; }

    void server_correlation_error() { ++_server_correlation_errors; }

    void waiting_for_available_memory() { ++_requests_blocked_memory; }

    std::vector<ss::metrics::metric_definition> defs(
      const std::vector<ss::metrics::label_instance>& labels,
      const std::vector<ss::metrics::label>& aggregate_labels,
      std::function<size_t()> pending_count);

private:
    uint64_t _requests = 0;
    uint32_t _request_errors = 0;
    uint64_t _request_timeouts = 0;
    uint64_t _requests_completed = 0;
    uint64_t _in_bytes = 0;
    uint64_t _out_bytes = 0;
    uint32_t _read_dispatch_errors = 0;
    uint32_t _corrupted_headers = 0;
    uint32_t _server_correlation_errors = 0;
    uint32_t _client_correlation_errors = 0;
    uint32_t _requests_blocked_memory = 0;

    friend struct fmt::formatter<client_probe>;

public:
    fmt::iterator format_to(fmt::iterator it) const;
};

/**
 * Transport implementation used for internal RPC traffic.
 *
 * As callers send buffers over the wire, the transport associates each with an
 * an appropriate response handler to use upon getting a response.
 *
 * Once connected, the transport repeatedly reads from the wire until an
 * invalid response is received, or until shut down.
 */
class transport final : public net::base_transport {
public:
    explicit transport(
      transport_configuration c,
      std::optional<connection_cache_label> label = std::nullopt,
      std::optional<model::node_id> node_id = std::nullopt);
    ~transport() override;
    // semaphore is not move assignable
    transport(transport&&) = delete;
    transport& operator=(transport&&) = delete;
    transport(const transport&) = delete;
    transport& operator=(const transport&) = delete;

    ss::future<> connect(clock_type::time_point) final;
    ss::future<> connect(clock_type::duration);
    ss::future<result<std::unique_ptr<streaming_context>>>
      send(netbuf, rpc::client_opts);

    template<typename Input, typename Output>
    ss::future<result<client_context<Output>>>
      send_typed(Input, method_info, rpc::client_opts);

    template<typename Input, typename Output>
    ss::future<result<result_context<Output>>> send_typed_versioned(
      Input, method_info, rpc::client_opts, transport_version);

    void reset_state() final;

    transport_version version() const { return _version; }

    fmt::iterator format_to(fmt::iterator it) const;

private:
    using sequence_t = named_type<uint64_t, struct sequence_tag>;
    struct entry {
        scattered_buffer bufs;
        uint32_t correlation_id;
    };
    using requests_queue_t
      = absl::btree_map<sequence_t, std::unique_ptr<entry>>;
    friend client_context_impl;
    ss::future<> do_reads();
    ss::future<> dispatch(header);
    void fail_outstanding_futures() noexcept final;
    void setup_metrics(
      const std::optional<connection_cache_label>&,
      const std::optional<model::node_id>&);

    ss::future<result<std::unique_ptr<streaming_context>>>
      do_send(sequence_t, netbuf, rpc::client_opts);
    void dispatch_send();
    ss::future<> do_dispatch_send();

    ss::future<result<std::unique_ptr<streaming_context>>>
    make_response_handler(netbuf&, rpc::client_opts&);

    ssx::semaphore _memory;

    /**
     * @brief Get the timing info for the request with the given correlation ID.
     *
     * A pointer to the timing info object embedded in the _correlations map, or
     * nullptr the correlation no longer exists (e.g., because the request has
     * completed).
     *
     * This pointer is only valid until the next suspension point, since the
     * entry may be deleted at any point if the current fiber suspends.
     */
    timing_info* get_timing(uint32_t correlation);

    /**
     * @brief Holds resource units from client_opts, the response handler and
     * timing information for an outstanding request.
     */
    struct response_entry {
        client_opts::resource_units_t resource_units;
        internal::response_handler handler;
        timing_info timing;
    };

    /**
     * Map of correlation IDs to response handlers to use when processing a
     * response read from the wire. We also track the timing info for the
     * request in this map.
     *
     * NOTE: _correlation_idx is unrelated to the sequence type used to define
     * on-wire ordering below.
     *
     * TODO(CORE-12902)
     */
    chunked_hash_map<uint32_t, std::unique_ptr<response_entry>> _correlations;
    uint32_t _correlation_idx{0};

    /**
     * Ordered map containing requests to be sent over the wire. The map
     * preserves order of calling send_typed function. It is fine to use
     * btree_map in here as it ususally contains only few elements.
     */
    requests_queue_t _requests_queue;
    sequence_t _seq;
    sequence_t _last_seq;

    /*
     * version level used when dispatching requests. this value may change
     * during the lifetime of the transport. for example the version may be
     * upgraded if it is discovered that a server supports a newer version.
     */
    transport_version _version;

    /*
     * The initial version for new connections.  If we upgrade to a newer
     * version from negotiation with a peer, _version will be incremented
     * but will reset to _default_version when reset_state() is called.
     */
    transport_version _default_version;

    friend class ::rpc_integration_fixture_oc_ns_adl_serde_no_upgrade;
    friend class ::rpc_integration_fixture_oc_ns_adl_only_no_upgrade;
    void set_version(transport_version v) { _version = v; }

    std::unique_ptr<client_probe> _probe;
};

namespace internal {

inline errc map_server_error(status status) {
    switch (status) {
    case status::success:
        return errc::success;
    case status::request_timeout:
        return errc::client_request_timeout;
    case status::server_error:
        return errc::service_error;
    case status::method_not_found:
        return errc::method_not_found;
    case status::version_not_supported:
        return errc::version_not_supported;
    case status::service_unavailable:
        return errc::service_unavailable;
    default:
        return errc::unknown;
    };
};

template<typename T>
ss::future<result<rpc::client_context<T>>> parse_result(
  ss::input_stream<char>& in,
  std::unique_ptr<streaming_context> sctx,
  transport_version req_ver) {
    const auto st = static_cast<status>(sctx->get_header().meta);
    const auto rep_ver = sctx->get_header().version;

    /*
     * the reply version should always be the same as the request version,
     * otherwise this is non-compliant behavior. the exception to this
     * rule is a v0 reply to a v1 request (ie talking to old v0 server).
     */
    const auto protocol_violation = rep_ver != req_ver;

    if (unlikely(st != status::success || protocol_violation)) {
        if (st == status::version_not_supported) {
            /*
             * let version_not_supported take precedence over error handling for
             * protocol violations because the protocol violation may be due to
             * the unsupported version scenario.
             */
            sctx->signal_body_parse();
            co_return map_server_error(st);
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
            co_return ss::coroutine::exception(ex);
        }
        sctx->signal_body_parse();
        co_return map_server_error(st);
    }

    auto header = sctx->get_header();
    auto parsed = co_await ss::coroutine::as_future(
      parse_type<T, default_message_codec>(in, header));
    if (parsed.failed()) {
        auto exception = parsed.get_exception();
        sctx->body_parse_exception(exception);
        /**
         * we want to propagate an exception when body parsing failed.
         * this will invalidate the connection since it may not be
         * valid any more.
         */
        co_return ss::coroutine::exception(exception);
    }
    sctx->signal_body_parse();
    co_return rpc::client_context<T>(
      sctx->get_header(), std::move(parsed).get());
}

} // namespace internal

template<typename Input, typename Output>
inline ss::future<result<client_context<Output>>>
transport::send_typed(Input r, method_info method, rpc::client_opts opts) {
    auto result = co_await send_typed_versioned<Input, Output>(
      std::move(r), method, std::move(opts), _version);
    if (!result) {
        co_return result.error();
    }
    co_return std::move(result.value().ctx);
}

template<typename Input, typename Output>
inline ss::future<result<result_context<Output>>>
transport::send_typed_versioned(
  Input r,
  method_info method,
  rpc::client_opts opts,
  transport_version version) {
    using ret_t = result<result_context<Output>>;
    _probe->request();

    auto b = std::make_unique<rpc::netbuf>();
    b->set_compression(opts.compression);
    b->set_min_compression_bytes(opts.min_compression_bytes);
    auto raw_b = b.get();
    raw_b->set_service_method(method);

    auto& target_buffer = raw_b->buffer();
    auto seq = ++_seq;
    auto effective_version = co_await encode_for_version(
      target_buffer, std::move(r), version);
    vassert(
      version >= transport_version::min_supported,
      "Request type {} cannot be encoded at version {} (effective {}).",
      typeid(Input).name(),
      version,
      effective_version);
    b->set_version(effective_version);

    auto context = co_await do_send(seq, std::move(*b), std::move(opts));
    if (!context) {
        co_return context.error();
    }
    const auto response_version = context.value()->get_header().version;
    auto parsed = co_await internal::parse_result<Output>(
      in(), std::move(context.value()), effective_version);
    co_return ret_t(
      result_context<Output>{response_version, std::move(parsed)});
}

} // namespace rpc

template<>
struct fmt::formatter<rpc::client_probe> {
    constexpr auto parse(fmt::format_parse_context& ctx) const {
        return ctx.begin();
    }
    auto format(const rpc::client_probe& v, fmt::format_context& ctx) const {
        return v.format_to(ctx.out());
    }
};

template<>
struct fmt::formatter<rpc::transport> {
    constexpr auto parse(fmt::format_parse_context& ctx) const {
        return ctx.begin();
    }
    auto format(const rpc::transport& v, fmt::format_context& ctx) const {
        return v.format_to(ctx.out());
    }
};
