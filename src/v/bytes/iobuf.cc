// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iobuf.h"

#include "bytes/details/io_allocation_size.h"
#include "vassert.h"

#include <seastar/core/bitops.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/smp.hh>

#include <cstddef>
#include <iostream>
#include <limits>

std::ostream& operator<<(std::ostream& o, const iobuf& io) {
    return o << "{bytes=" << io.size_bytes()
             << ", fragments=" << std::distance(io.cbegin(), io.cend()) << "}";
}
ss::scattered_message<char> iobuf_as_scattered(iobuf b) {
    ss::scattered_message<char> msg;
    auto in = iobuf::iterator_consumer(b.cbegin(), b.cend());
    int32_t chunk_no = 0;
    in.consume(
      b.size_bytes(), [&msg, &chunk_no, &b](const char* src, size_t sz) {
          ++chunk_no;
          vassert(
            chunk_no <= std::numeric_limits<int16_t>::max(),
            "Invalid construction of scattered_message. fragment coutn exceeds "
            "max count:{}. Usually a bug with small append() to iobuf. {}",
            chunk_no,
            b);
          msg.append_static(src, sz);
          return ss::stop_iteration::no;
      });
    msg.on_delete([b = std::move(b)] {});
    return msg;
}

ss::future<>
write_iobuf_to_output_stream(iobuf buf, ss::output_stream<char>& output) {
    return ss::do_with(std::move(buf), [&output](iobuf& buf) {
        return ss::do_for_each(buf, [&output](iobuf::fragment& f) {
            return output.write(f.get(), f.size());
        });
    });
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

iobuf iobuf::copy() const {
    auto in = iobuf::iterator_consumer(cbegin(), cend());
    return iobuf_copy(in, _size);
}

iobuf iobuf_copy(iobuf::iterator_consumer& in, size_t len) {
    iobuf ret;

    int bytes_left = len;
    size_t alloc_size = details::io_allocation_size::ss_next_allocation_size(
      bytes_left);

    while (bytes_left) {
        alloc_size = std::min(
          alloc_size,
          details::io_allocation_size::ss_next_allocation_size(bytes_left));

        // Try and allocate a fragment, backing off our alloc size if we can't.
        ss::temporary_buffer<char> buf;
        while (buf.get() == nullptr) {
            try {
                buf = ss::temporary_buffer<char>(alloc_size);
            } catch (std::bad_alloc const& e) {
                if (alloc_size > 0x1000) {
                    // We were asking for more than 4k and couldn't get it,
                    // try asking for smaller fragments.
                    alloc_size >>= 1;
                } else {
                    // We couldn't even get one 4k extent: give up.
                    throw;
                }
            }
        }

        size_t offset = 0;
        in.consume(buf.size(), [&buf, &offset](const char* src, size_t size) {
            // NOLINTNEXTLINE
            std::copy_n(src, size, buf.get_write() + offset);
            offset += size;
            return ss::stop_iteration::no;
        });

        bytes_left -= buf.size();

        auto f = new iobuf::fragment(std::move(buf), iobuf::fragment::full{});
        ret.append_take_ownership(f);
    }

    vassert(bytes_left == 0, "Bytes remaining to be copied");
    return ret;
}

iobuf iobuf::share(size_t pos, size_t len) {
    iobuf ret;
    size_t left = len;
    for (auto& frag : _frags) {
        if (left == 0) {
            break;
        }
        if (pos >= frag.size()) {
            pos -= frag.size();
            continue;
        }
        size_t left_in_frag = frag.size() - pos;
        if (left >= left_in_frag) {
            left -= left_in_frag;
        } else {
            left_in_frag = left;
            left = 0;
        }
        auto f = new fragment(frag.share(pos, left_in_frag), fragment::full{});
        ret.append_take_ownership(f);
        pos = 0;
    }
    return ret;
}

bool iobuf::operator==(const iobuf& o) const {
    if (_size != o._size) {
        return false;
    }
    auto lhs_begin = byte_iterator(cbegin(), cend());
    auto lhs_end = byte_iterator(cend(), cend());
    auto rhs = byte_iterator(o.cbegin(), o.cend());
    auto rhs_end = byte_iterator(o.cend(), o.cend());
    while (lhs_begin != lhs_end && rhs != rhs_end) {
        char l = *lhs_begin;
        char r = *rhs;
        if (l != r) {
            return false;
        }
        ++lhs_begin;
        ++rhs;
    }
    return true;
}

bool iobuf::operator==(std::string_view o) const {
    if (_size != o.size()) {
        return false;
    }
    bool are_equal = true;
    std::string_view::size_type n = 0;
    auto in = iobuf::iterator_consumer(cbegin(), cend());
    (void)in.consume(
      size_bytes(), [&are_equal, &o, &n](const char* src, size_t fg_sz) {
          /// Both strings are equiv in total size, so its safe to assume the
          /// next chunk to compare is the remaining to cmp or the fragment size
          const auto size = std::min((o.size() - n), fg_sz);
          std::string_view a_view(src, size);
          std::string_view b_view(o.cbegin() + n, size);
          n += size;
          are_equal &= (a_view == b_view);
          return !are_equal ? ss::stop_iteration::yes : ss::stop_iteration::no;
      });
    return are_equal;
}
