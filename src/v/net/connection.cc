// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "net/connection.h"

#include "base/seastarx.h"
#include "net/exceptions.h"
#include "net/types.h"
#include "ssx/abort_source.h"

#include <seastar/core/future.hh>
#include <seastar/net/tls.hh>

#include <exception>
#include <system_error>

namespace net {

/**
 * Identify error cases that should be quickly retried, e.g.
 * TCP disconnects, timeouts. Network errors may also show up
 * indirectly as errors from the TLS layer.
 */
bool is_reconnect_error(const std::system_error& e) {
    const auto v = e.code().value();
    static const std::array ss_tls_reconnect_errors{
      ss::tls::ERROR_PUSH,
      ss::tls::ERROR_PULL,
      ss::tls::ERROR_UNEXPECTED_PACKET,
      ss::tls::ERROR_INVALID_SESSION,
      ss::tls::ERROR_UNSUPPORTED_VERSION,
      ss::tls::ERROR_NO_CIPHER_SUITES,
      ss::tls::ERROR_PREMATURE_TERMINATION,
      ss::tls::ERROR_DECRYPTION_FAILED,
      ss::tls::ERROR_MAC_VERIFY_FAILED};

    if (e.code().category() == ss::tls::error_category()) {
        return absl::c_any_of(
          ss_tls_reconnect_errors, [v](int ec) { return v == ec; });
    } else if (
      e.code().category() == std::system_category()
      || e.code().category() == std::generic_category()) {
        switch (v) {
        case ECONNREFUSED:
        case ENETUNREACH:
        case ETIMEDOUT:
        case ECONNRESET:
        case ENOTCONN:
        case ECONNABORTED:
        case EAGAIN:
        case EPIPE:
        case EHOSTUNREACH:
        case EHOSTDOWN:
        case ENETRESET:
        case ENETDOWN:
            return true;
        default:
            return false;
        }
    } else {
        // We don't know what the error category is at this point
        return false;
    }
    __builtin_unreachable();
}

/**
 * If the exception is a "boring" disconnection case, then populate this with
 * the reason.
 *
 * This avoids logging overly alarmist "error" messages for exceptions that
 * are typical in the case of a client or node simply stopping.
 */
std::optional<ss::sstring> is_disconnect_exception(std::exception_ptr e) {
    try {
        std::rethrow_exception(e);
    } catch (const std::system_error& e) {
        if (is_reconnect_error(e)) {
            return e.code().message();
        }
    } catch (const net::batched_output_stream_closed& e) {
        return "stream closed";
    } catch (const std::out_of_range&) {
        // Happens on unclean client disconnect, when io_iterator_consumer
        // gets fewer bytes than it wanted
        return "short read";
    } catch (const net::parsing_exception&) {
        // Happens on unclean client disconnect, typically wrapping
        // an out_of_range
        return "parse error";
    } catch (const invalid_request_error& e) {
        if (std::strlen(e.what())) {
            return fmt::format("invalid request: {}", e.what());
        }
        return "invalid request";
    } catch (const ssx::connection_aborted_exception&) {
        return "connection aborted";
    } catch (const ssx::shutdown_requested_exception&) {
        return "shutdown requested";
    } catch (const ss::nested_exception& e) {
        if (auto err = is_disconnect_exception(e.inner)) {
            return err;
        }
        return is_disconnect_exception(e.outer);
    } catch (...) {
        // Global catch-all prevents stranded/non-handled exceptional futures.
        // In all other non-explicity handled cases, the exception will not be
        // related to disconnect issues, therefore fallthrough to return nullopt
        // is acceptable.
    }

    return std::nullopt;
}

bool is_auth_error(std::exception_ptr e) {
    try {
        std::rethrow_exception(e);
    } catch (const authentication_exception& e) {
        return true;
    } catch (...) {
        return false;
    }

    __builtin_unreachable();
}

connection::connection(
  boost::intrusive::list<connection>& hook,
  ss::sstring name,
  ss::connected_socket f,
  ss::socket_address a,
  server_probe& p,
  std::optional<size_t> in_max_buffer_size,
  bool tls_enabled,
  ss::logger* log)
  : addr(a)
  , _hook(hook)
  , _name(std::move(name))
  , _fd(std::move(f))
  , _local_addr(_fd.local_address())
  , _in(_fd.input())
  , _out(_fd.output())
  , _probe(p)
  , _tls_enabled(tls_enabled)
  , _log(log) {
    if (in_max_buffer_size.has_value()) {
        auto in_config = ss::connected_socket_input_stream_config{};
        in_config.max_buffer_size = in_max_buffer_size.value();
        _in = _fd.input(in_config);
    } else {
        _in = _fd.input();
    }

    _hook.push_back(*this);
    _probe.connection_established();
}

connection::~connection() noexcept { _hook.erase(_hook.iterator_to(*this)); }

void connection::shutdown_input() {
    try {
        _fd.shutdown_input();
    } catch (...) {
        _probe.connection_close_error();
        _log->debug(
          "Failed to shutdown connection: {}", std::current_exception());
    }
}

ss::future<> connection::shutdown() {
    _probe.connection_closed();
    return _out.stop();
}

ss::future<> connection::write(ss::scattered_message<char> msg) {
    _probe.add_bytes_sent(msg.size());
    return _out.write(std::move(msg)).discard_result();
}

} // namespace net
