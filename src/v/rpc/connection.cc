#include "rpc/connection.h"

#include "rpc/logger.h"

namespace rpc {
connection::connection(
  boost::intrusive::list<connection>& hook,
  ss::connected_socket f,
  ss::socket_address a,
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
connection::~connection() { _hook.get().erase(_hook.get().iterator_to(*this)); }

ss::future<> connection::shutdown() {
    _probe.connection_closed();
    try {
        _fd.shutdown_input();
        _fd.shutdown_output();
    } catch (...) {
        _probe.connection_close_error();
        rpclog.debug(
          "Failed to shutdown connection: {}", std::current_exception());
    }
    return ss::make_ready_future<>();
}
ss::future<> connection::write(ss::scattered_message<char> msg) {
    _probe.add_bytes_sent(msg.size());
    return _out.write(std::move(msg));
}

} // namespace rpc
