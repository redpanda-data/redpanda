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
            if (io.begin() == io.end()) {
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
        ss::future<> put(ss::net::packet data) final {
            auto all = data.release();
            for (auto& b : all) {
                io.append(std::move(b));
            }
            return ss::make_ready_future<>();
        }
        ss::future<> put(std::vector<ss::temporary_buffer<char>> all) final {
            for (auto& b : all) {
                io.append(std::move(b));
            }
            return ss::make_ready_future<>();
        }
        ss::future<> put(ss::temporary_buffer<char> buf) final {
            io.append(std::move(buf));
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
    return ss::do_with(iobuf{}, n, [&in](iobuf& b, size_t& n) {
        return ss::do_until(
                 [&n] { return n == 0; },
                 [&n, &in, &b] {
                     return in.read_up_to(n).then(
                       [&n, &b](ss::temporary_buffer<char> buf) {
                           if (buf.empty()) {
                               n = 0;
                               return;
                           }
                           n -= buf.size();
                           b.append(std::move(buf));
                       });
                 })
          .then([&b] { return std::move(b); });
    });
}

ss::future<>
write_iobuf_to_output_stream(iobuf buf, ss::output_stream<char>& output) {
    return ss::do_with(std::move(buf), [&output](iobuf& buf) {
        return ss::do_for_each(buf, [&output](iobuf::fragment& f) {
            return output.write(f.get(), f.size());
        });
    });
}
