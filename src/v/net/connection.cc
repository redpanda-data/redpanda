// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "net/connection.h"

#include "rpc/service.h"

namespace net {

/**
 * If the exception is a "boring" disconnection case, then populate this with
 * the reason.
 *
 * This avoids logging overly alarmist "error" messages for exceptions that
 * are typical in the case of a client or node simply stopping.
 */
std::optional<ss::sstring> is_disconnect_exception(std::exception_ptr e) {
    try {
        rethrow_exception(e);
    } catch (std::system_error& e) {
        if (
          e.code() == std::errc::broken_pipe
          || e.code() == std::errc::connection_reset
          || e.code() == std::errc::connection_aborted) {
            return e.code().message();
        }
    } catch (const net::batched_output_stream_closed& e) {
        return "stream closed";
    } catch (const std::out_of_range&) {
        // Happens on unclean client disconnect, when io_iterator_consumer
        // gets fewer bytes than it wanted
        return "short read";
    } catch (const rpc::rpc_internal_body_parsing_exception&) {
        // Happens on unclean client disconnect, typically wrapping
        // an out_of_range
        return "parse error";
    }

    return std::nullopt;
}

connection::connection(
  boost::intrusive::list<connection>& hook,
  ss::sstring name,
  ss::connected_socket f,
  ss::socket_address a,
  server_probe& p,
  std::optional<size_t> in_max_buffer_size,
  std::optional<security::tls::principal_mapper> tls_pm)
  : addr(a)
  , _hook(hook)
  , _name(std::move(name))
  , _fd(std::move(f))
  , _in(_fd.input())
  , _out(_fd.output())
  , _probe(p)
  , _tls_pm(std::move(tls_pm)) {
    if (in_max_buffer_size.has_value()) {
        auto in_config = ss::connected_socket_input_stream_config{};
        in_config.max_buffer_size = in_max_buffer_size.value();
        _in = _fd.input(std::move(in_config));
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
        rpc::rpclog.debug(
          "Failed to shutdown connection: {}", std::current_exception());
    }
}

ss::future<> connection::shutdown() {
    _probe.connection_closed();
    return _out.stop();
}

ss::future<> connection::write(ss::scattered_message<char> msg) {
    _probe.add_bytes_sent(msg.size());
    return _out.write(std::move(msg));
}

} // namespace net
