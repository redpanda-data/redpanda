#include "rpc/deserialize.h"
#include "rpc/serialize.h"

#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include <seastar/testing/perf_tests.hh>

using namespace rpc; // NOLINT

struct small_t {
    int8_t a = 1;
    // char __a_padding;
    int16_t b = 2;
    int32_t c = 3;
    int64_t d = 4;
};
static_assert(sizeof(small_t) == 16, "one more byte for padding");

PERF_TEST(small, serialize) {
    perf_tests::start_measuring_time();
    auto o = rpc::serialize(small_t{});
    perf_tests::do_not_optimize(o);
    perf_tests::stop_measuring_time();
}
PERF_TEST(small, deserialize) {
    return do_with(
      make_iobuf_input_stream(rpc::serialize(small_t{})),
      [](input_stream<char>& in) {
          return do_with(rpc::source(in), [](rpc::source& s) {
              perf_tests::start_measuring_time();
              return rpc::deserialize<small_t>(s).then(
                [](auto _) { perf_tests::stop_measuring_time(); });
          });
      });
}

struct big_t {
    small_t s;
    iobuf data;
};

inline big_t gen_big(size_t data_size, size_t chunk_size) {
    const size_t chunks = data_size / chunk_size;
    big_t ret{.s = small_t{}};
    for (size_t i = 0; i < chunks; ++i) {
        auto c = temporary_buffer<char>(chunk_size);
        ret.data.append(std::move(c));
    }
    return ret;
}

inline void serialize_big(size_t data_size, size_t chunk_size) {
    big_t b = gen_big(data_size, chunk_size);
    auto o = iobuf();
    perf_tests::start_measuring_time();
    serialize(o, std::move(b));
    perf_tests::do_not_optimize(o);
    perf_tests::stop_measuring_time();
}

inline future<> deserialize_big(size_t data_size, size_t chunk_size) {
    big_t b = gen_big(data_size, chunk_size);
    auto o = iobuf();
    serialize(o, std::move(b));
    return do_with(
      make_iobuf_input_stream(std::move(o)), [](input_stream<char>& in) {
          return do_with(rpc::source(in), [](rpc::source& s) {
              perf_tests::start_measuring_time();
              return rpc::deserialize<big_t>(s).then(
                [](auto _) { perf_tests::stop_measuring_time(); });
          });
      });
}

PERF_TEST(big_1mb, serialize) {
    serialize_big(1 << 20 /*1MB*/, 1 << 15 /*32KB*/);
}

PERF_TEST(big_1mb, deserialize) {
    return deserialize_big(1 << 20 /*1MB*/, 1 << 15 /*32KB*/);
}

PERF_TEST(big_10mb, serialize) {
    serialize_big(10 << 20 /*10MB*/, 1 << 15 /*32KB*/);
}

PERF_TEST(big_10mb, deserialize) {
    return deserialize_big(10 << 20 /*10MB*/, 1 << 15 /*32KB*/);
}
