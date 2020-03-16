#include "bytes/iobuf.h"

#include <iostream>

std::ostream& operator<<(std::ostream& o, const iobuf& io) {
    return o << "{bytes=" << io.size_bytes()
             << ", fragments=" << std::distance(io.cbegin(), io.cend()) << "}";
}
ss::scattered_message<char> iobuf_as_scattered(iobuf b) {
    ss::scattered_message<char> msg;
    auto in = iobuf::iterator_consumer(b.cbegin(), b.cend());
    in.consume(b.size_bytes(), [&msg](const char* src, size_t sz) {
        msg.append_static(src, sz);
        return ss::stop_iteration::no;
    });
    msg.on_delete([b = std::move(b)] {});
    return msg;
}
std::vector<iobuf> iobuf_share_foreign_n(iobuf&& og, size_t n) {
    const auto shard = ss::this_shard_id();
    std::vector<iobuf> retval(n);
    for (auto& frag : og) {
        auto tmpbuf = std::move(frag).release();
        char* src = tmpbuf.get_write();
        const size_t sz = tmpbuf.size();
        ss::deleter del = tmpbuf.release();
        for (iobuf& b : retval) {
            ss::deleter del_i = ss::make_deleter(
              [shard, d = del.share()]() mutable {
                  (void)ss::smp::submit_to(shard, [d = std::move(d)] {});
              });
            b.append(ss::temporary_buffer<char>(src, sz, std::move(del_i)));
        }
    }
    return retval;
}
ss::future<iobuf> read_iobuf_exactly(ss::input_stream<char>& in, size_t n) {
    return ss::do_with(iobuf(), n, [&in](iobuf& b, size_t& n) {
        return ss::do_until(
                 [&n] { return n == 0; },
                 [&n, &in, &b] {
                     if (n == 0) {
                         return ss::make_ready_future<>();
                     }
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
          .then([&b] { return ss::make_ready_future<iobuf>(std::move(b)); });
    });
}

ss::output_stream<char> make_iobuf_output_stream(iobuf io) {
    struct iobuf_output_stream final : ss::data_sink_impl {
        explicit iobuf_output_stream(iobuf i)
          : io(std::move(i)) {}
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
        iobuf io;
    };
    const size_t sz = io.size_bytes();
    return ss::output_stream<char>(
      ss::data_sink(std::make_unique<iobuf_output_stream>(std::move(io))), sz);
}
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
