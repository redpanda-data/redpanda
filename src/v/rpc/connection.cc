#include "rpc/connection.h"

#include "rpc/logger.h"

namespace rpc {
connection::connection(
  boost::intrusive::list<connection>& hook,
  connected_socket f,
  socket_address a,
  server_probe& p)
  : addr(std::move(a))
  , _hook(hook)
  , _fd(std::move(f))
  , _in(_fd.input())
  , _out(_fd.output())
  , _probe(p) {
    _hook.get().push_back(*this);
    _probe.connection_established();
}
connection::~connection() {
    _hook.get().erase(_hook.get().iterator_to(*this));
}

void connection::shutdown() {
    _probe.connection_closed();
    try {
        _fd.shutdown_input();
        _fd.shutdown_output();
    } catch (...) {
        _probe.connection_close_error();
        rpclog.debug(
          "Failed to shutdown conneciton: {}", std::current_exception());
    }
}
future<> connection::write(scattered_message<char> msg) {
    _probe.add_bytes_sent(msg.size());
    return _out.write(std::move(msg));
}

} // namespace rpc
