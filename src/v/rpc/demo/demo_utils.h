#include "rpc/arity.h"
#include "rpc/demo/types.h"
#include "rpc/for_each_field.h"
#include "seastarx.h"
#include "utils/hdr_hist.h"

#include <seastar/core/fstream.hh>
#include <seastar/core/reactor.hh>

inline future<>
force_write_ptr(sstring filename, const char* ptr, std::size_t len) {
    auto flags = open_flags::rw | open_flags::create | open_flags::truncate;
    return open_file_dma(filename, flags).then([ptr, len](file f) mutable {
        auto out = make_lw_shared<output_stream<char>>(
          make_file_output_stream(std::move(f)));
        return out->write(ptr, len)
          .then([out] { return out->flush(); })
          .then([out] { return out->close(); })
          .finally([out] {});
    });
}

inline future<> force_write_buffer(sstring filename, temporary_buffer<char> b) {
    const char* ptr = b.get();
    std::size_t len = b.size();
    return force_write_ptr(std::move(filename), ptr, len)
      .then([b = std::move(b)] {});
}

inline future<> write_histogram(sstring filename, const hdr_hist& h) {
    return force_write_buffer(std::move(filename), h.print_classic());
}

namespace demo {
inline iobuf rand_iobuf(std::size_t chunks, std::size_t chunk_size) {
    iobuf b;
    for (size_t i = 0; i < chunks; ++i) {
        b.append(temporary_buffer<char>(chunk_size));
    }
    return b;
}

inline demo::simple_request
gen_simple_request(size_t data_size, size_t chunk_size) {
    const std::size_t chunks = data_size / chunk_size;
    return demo::simple_request{.data = rand_iobuf(chunks, chunk_size)};
}

inline interspersed_request
gen_interspersed_request(size_t data_size, size_t chunk_size) {
    const std::size_t chunks = data_size / chunk_size / 8;
    return interspersed_request{
      .data = interspersed_request::
        payload{._one = i1{.y = rand_iobuf(chunks, chunk_size)},
                ._two = i2{.x = i1{.y = rand_iobuf(chunks, chunk_size)},
                           .y = rand_iobuf(chunks, chunk_size)},
                ._three = i3{.x = i2{.x = i1{.y = rand_iobuf(
                                               chunks, chunk_size)},
                                     .y = rand_iobuf(chunks, chunk_size)},
                             .y = rand_iobuf(chunks, chunk_size)}},
      .x = rand_iobuf(chunks, chunk_size),
      .y = rand_iobuf(chunks, chunk_size)};
}

} // namespace demo
