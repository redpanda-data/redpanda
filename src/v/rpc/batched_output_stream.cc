#include "rpc/batched_output_stream.h"

#include "likely.h"

#include <seastar/core/future.hh>

namespace rpc {
batched_output_stream::batched_output_stream(
  ss::output_stream<char> o, size_t cache)
  : _out(std::move(o))
  , _cache_size(cache) {}
batched_output_stream::batched_output_stream(batched_output_stream&& o) noexcept
  : _out(std::move(o._out))
  , _cache_size(o._cache_size)
  , _write_sem(std::move(o._write_sem))
  , _unflushed_bytes(o._unflushed_bytes) {}
batched_output_stream&
batched_output_stream::operator=(batched_output_stream&& o) noexcept {
    if (this != &o) {
        this->~batched_output_stream();
        new (this) batched_output_stream(std::move(o));
    }
    return *this;
}

ss::future<> batched_output_stream::write(ss::scattered_message<char> msg) {
    return with_semaphore(_write_sem, 1, [this, v = std::move(msg)]() mutable {
        if (unlikely(_closed)) {
            // skip dispatching writes as stream is already closed
            return ss::make_ready_future<>();
        }
        const size_t vbytes = v.size();
        return _out.write(std::move(v)).then([this, vbytes] {
            _unflushed_bytes += vbytes;
            if (_write_sem.waiters() == 0 || _unflushed_bytes >= _cache_size) {
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
    return with_semaphore(_write_sem, 1, [this] { return do_flush(); });
}
ss::future<> batched_output_stream::stop() {
    _closed = true;
    return with_semaphore(_write_sem, 1, [this] {
        return do_flush().then([this] { return _out.close(); });
    });
}

} // namespace rpc
