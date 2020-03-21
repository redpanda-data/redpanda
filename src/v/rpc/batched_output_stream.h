#pragma once

#include "seastarx.h"

#include <seastar/core/iostream.hh>
#include <seastar/core/semaphore.hh>

#include <cstdint>

namespace rpc {

/// \brief batch operations for zero copy interface of an output_stream<char>
class batched_output_stream {
public:
    static constexpr size_t default_max_unflushed_bytes = 1024 * 1024;

    batched_output_stream() = default;
    explicit batched_output_stream(
      ss::output_stream<char>, size_t cache = default_max_unflushed_bytes);
    ~batched_output_stream() noexcept = default;
    // NOTE: explicitly defined for a gcc
    batched_output_stream(batched_output_stream&& o) noexcept
      : _out(std::move(o._out))
      , _write_sem(std::move(o._write_sem))
      , _cache_size(o._cache_size)
      , _unflushed_bytes(o._unflushed_bytes)
      , _closed(o._closed) {}
    batched_output_stream& operator=(batched_output_stream&& o) noexcept {
        if (this != &o) {
            this->~batched_output_stream();
            new (this) batched_output_stream(std::move(o));
        }
        return *this;
    }
    batched_output_stream(const batched_output_stream&) = delete;
    batched_output_stream& operator=(const batched_output_stream&) = delete;

    ss::future<> write(ss::scattered_message<char> msg);
    ss::future<> flush();

    /// \brief calls output_stream<char>::close()
    /// do not use `_fd.shutdown_output();` on connected_sockets
    ss::future<> stop();

private:
    ss::future<> do_flush();

    ss::output_stream<char> _out;
    std::unique_ptr<ss::semaphore> _write_sem;
    size_t _cache_size{0};
    size_t _unflushed_bytes{0};
    bool _closed = false;
};
} // namespace rpc
