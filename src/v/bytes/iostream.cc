/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "bytes/iostream.h"

ss::input_stream<char> make_iobuf_input_stream(iobuf io) {
    struct iobuf_input_stream final : ss::data_source_impl {
        explicit iobuf_input_stream(iobuf i)
          : io(std::move(i)) {}
        ss::future<ss::temporary_buffer<char>> skip(uint64_t n) final {
            io.trim_front(n);
            return get();
        }
        ss::future<ss::temporary_buffer<char>> get() final {
            if (io.empty()) {
                return ss::make_ready_future<ss::temporary_buffer<char>>();
            }
            auto buf = io.begin()->share();
            io.pop_front();
            return ss::make_ready_future<ss::temporary_buffer<char>>(
              std::move(buf));
        }
        iobuf io;
    };
    auto ds = ss::data_source(
      std::make_unique<iobuf_input_stream>(std::move(io)));
    return ss::input_stream<char>(std::move(ds));
}

ss::output_stream<char> make_iobuf_ref_output_stream(iobuf& io) {
    struct iobuf_output_stream final : ss::data_sink_impl {
        explicit iobuf_output_stream(iobuf& i)
          : io(i) {}
        ss::future<> put(scattered_buffer_view data) final {
            for (auto& b : data) {
                io.append(std::move(b));
            }
            return ss::make_ready_future<>();
        }
        ss::future<> flush() final { return ss::make_ready_future<>(); }
        ss::future<> close() final { return ss::make_ready_future<>(); }
        iobuf& io;
    };
    const size_t sz = io.size_bytes();
    return ss::output_stream<char>(
      ss::data_sink(std::make_unique<iobuf_output_stream>(io)), sz);
}

ss::future<iobuf> read_iobuf_exactly(ss::input_stream<char>& in, size_t n) {
    iobuf result;
    while (n > 0) {
        auto buffer = co_await in.read_up_to(n);
        if (buffer.empty()) {
            break;
        }
        n -= buffer.size();
        result.append(std::move(buffer));
    }
    co_return result;
}

ss::future<>
write_iobuf_to_output_stream(iobuf buf, ss::output_stream<char>& output) {
    for (const auto& fragment : buf) {
        co_await output.write(fragment.get(), fragment.size());
    }
}
