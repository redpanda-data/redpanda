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
      socket_address a)
      : addr(std::move(a))
      , _hook(hook)
      , _fd(std::move(f))
      , _in(_fd.input())
      , _out(_fd.output()) {
        _hook.get().push_back(*this);
    }
    connection(connection&& o) noexcept
      : addr(std::move(o.addr))
      , _hook(std::move(o._hook))
      , _fd(std::move(o._fd))
      , _in(std::move(o._in))
      , _out(std::move(o._out)) {
    }
    ~connection() {
        _hook.get().erase(_hook.get().iterator_to(*this));
    }
    connection(const connection&) = delete;
    input_stream<char>& input() {
        return _in;
    }
    output_stream<char>& output() {
        return _out;
    }
    void shutdown() {
        try {
            _fd.shutdown_input();
            _fd.shutdown_output();
        } catch (...) {
            rpclog().debug(
              "Failed to shutdown conneciton: {}", std::current_exception());
        }
    }
    const socket_address addr;

private:
    std::reference_wrapper<boost::intrusive::list<connection>> _hook;
    connected_socket _fd;
    input_stream<char> _in;
    output_stream<char> _out;
};
} // namespace rpc
