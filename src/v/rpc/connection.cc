#include "rpc/connection.h"

#include "rpc/logger.h"

namespace rpc {
connection::connection(
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
connection::~connection() {
    _hook.get().erase(_hook.get().iterator_to(*this));
}

void connection::shutdown() {
    try {
        _fd.shutdown_input();
        _fd.shutdown_output();
    } catch (...) {
        rpclog().debug(
          "Failed to shutdown conneciton: {}", std::current_exception());
    }
}
future<> connection::write(scattered_message<char> msg) {
    return _out.write(std::move(msg));
}

} // namespace rpc
