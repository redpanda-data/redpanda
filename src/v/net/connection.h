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

#include "base/seastarx.h"
#include "net/batched_output_stream.h"
#include "net/server_probe.h"

#include <seastar/core/iostream.hh>
#include <seastar/net/api.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/net/tls.hh>
#include <seastar/util/log.hh>

#include <boost/intrusive/list.hpp>

/*
 * FIXME:
 *  - server_probe contains bits from simple_protocol
 */
namespace net {

bool is_reconnect_error(const std::system_error& e);
std::optional<ss::sstring> is_disconnect_exception(std::exception_ptr);

bool is_auth_error(std::exception_ptr);

class connection : public boost::intrusive::list_base_hook<> {
public:
    connection(
      boost::intrusive::list<connection>& hook,
      ss::sstring name,
      ss::connected_socket f,
      ss::socket_address a,
      server_probe& p,
      std::optional<size_t> in_max_buffer_size,
      bool tls_enabled,
      ss::logger*);
    ~connection() noexcept;
    connection(const connection&) = delete;
    connection& operator=(const connection&) = delete;
    connection(connection&&) noexcept = default;
    connection& operator=(connection&&) noexcept = delete;

    const ss::sstring& name() const { return _name; }
    ss::input_stream<char>& input() { return _in; }
    ss::future<> write(ss::scattered_message<char> msg);
    ss::future<> shutdown();
    void shutdown_input();
    const ss::socket_address& local_address() const noexcept {
        return _local_addr;
    }

    // NOLINTNEXTLINE
    const ss::socket_address addr; // remote addr

    /// Returns DN from client certificate
    ///
    /// The value can only be returned by the server socket and
    /// only in case if the client authentication is enabled.
    ss::future<std::optional<ss::session_dn>> get_distinguished_name() {
        return ss::tls::get_dn_information(_fd);
    }

    ss::future<> wait_for_input_shutdown() {
        return _fd ? _fd.wait_input_shutdown() : ss::make_ready_future<>();
    }

    bool tls_enabled() const { return _tls_enabled; }

private:
    boost::intrusive::list<connection>& _hook;
    ss::sstring _name;
    ss::connected_socket _fd;
    ss::socket_address _local_addr;
    ss::input_stream<char> _in;
    net::batched_output_stream _out;
    server_probe& _probe;
    bool _tls_enabled;
    ss::logger* _log;
};

} // namespace net
