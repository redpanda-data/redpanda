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
    batched_output_stream(batched_output_stream&&) noexcept;
    batched_output_stream& operator=(batched_output_stream&&) noexcept;
    ss::future<> write(ss::scattered_message<char> msg);
    ss::future<> flush();

    /// \brief calls output_stream<char>::close()
    /// do not use `_fd.shutdown_output();` on connected_sockets
    ss::future<> stop();

private:
    ss::future<> do_flush();

    ss::output_stream<char> _out;
    ss::semaphore _write_sem{1};
    size_t _cache_size{0};
    size_t _unflushed_bytes{0};
};
} // namespace rpc
