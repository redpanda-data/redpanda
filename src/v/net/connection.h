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

#include "net/batched_output_stream.h"
#include "net/server_probe.h"
#include "seastarx.h"

#include <seastar/core/iostream.hh>
#include <seastar/net/api.hh>
#include <seastar/net/socket_defs.hh>

#include <boost/intrusive/list.hpp>

/*
 * FIXME:
 *  - server_probe contains bits from simple_protocol
 */
namespace net {

class connection : public boost::intrusive::list_base_hook<> {
public:
    connection(
      boost::intrusive::list<connection>& hook,
      ss::sstring name,
      ss::connected_socket f,
      ss::socket_address a,
      server_probe& p);
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

    // NOLINTNEXTLINE
    const ss::socket_address addr;

private:
    boost::intrusive::list<connection>& _hook;
    ss::sstring _name;
    ss::connected_socket _fd;
    ss::input_stream<char> _in;
    net::batched_output_stream _out;
    server_probe& _probe;
};

} // namespace net
