#include "rpc/batched_output_stream.h"

#include "likely.h"

#include <seastar/core/future.hh>

namespace rpc {
batched_output_stream::batched_output_stream(
  ss::output_stream<char> o, size_t cache)
  : _out(std::move(o))
  , _cache_size(cache)
  , _write_sem(std::make_unique<ss::semaphore>(1)) {}
}

ss::future<> batched_output_stream::write(ss::scattered_message<char> msg) {
    return ss::with_semaphore(
      *_write_sem, 1, [this, v = std::move(msg)]() mutable {
          if (unlikely(_closed)) {
              return already_closed_error(v);
          }
          const size_t vbytes = v.size();
          return _out.write(std::move(v)).then([this, vbytes] {
              _unflushed_bytes += vbytes;
              if (
                _write_sem->waiters() == 0 || _unflushed_bytes >= _cache_size) {
                  return do_flush();
              }
              return ss::make_ready_future<>();
          });
      });
}
ss::future<> batched_output_stream::do_flush() {
    if (_unflushed_bytes == 0) {
        return ss::make_ready_future<>();
    }
    _unflushed_bytes = 0;
    return _out.flush();
}
ss::future<> batched_output_stream::flush() {
    return ss::with_semaphore(*_write_sem, 1, [this] { return do_flush(); });
}
ss::future<> batched_output_stream::stop() {
    if (_closed) {
        return ss::make_ready_future<>();
    }
    _closed = true;
    return ss::with_semaphore(*_write_sem, 1, [this] {
        return do_flush().then([this] { return _out.close(); });
    });
}

} // namespace rpc
