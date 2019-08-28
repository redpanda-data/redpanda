#pragma once

#include "rpc/logger.h"

#include <seastar/core/iostream.hh>
#include <seastar/net/api.hh>
#include <seastar/net/socket_defs.hh>

#include <boost/intrusive/list.hpp>

namespace rpc {

class connection : public boost::intrusive::list_base_hook<> {
public:
    connection(
      boost::intrusive::list<connection>& hook,
      connected_socket f,
      socket_address a);
    ~connection();
    connection(const connection&) = delete;
    input_stream<char>& input() {
        return _in;
    }
    future<> write(scattered_message<char> msg);
    void shutdown();

    const socket_address addr;

private:
    std::reference_wrapper<boost::intrusive::list<connection>> _hook;
    connected_socket _fd;
    input_stream<char> _in;
    output_stream<char> _out;
};
} // namespace rpc
