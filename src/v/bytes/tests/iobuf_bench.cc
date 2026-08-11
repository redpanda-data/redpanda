// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/units.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"

#include <seastar/testing/perf_tests.hh>

namespace {
static constexpr size_t inner_iters = 1000;

// Make a deterministic iobuf for consistent benchmarks instead of randomized
// data.
iobuf make_iobuf(size_t size, bool reverse = false) {
    auto gen = std::views::iota('a', 'z');
    std::string generated;
    generated.reserve(size);
    while (generated.size() < size) {
        for (char c : gen) {
            generated.push_back(c);
            if (generated.size() == size) {
                break;
            }
        }
    }
    if (reverse) {
        std::ranges::reverse(generated);
    }
    return iobuf::from(generated);
}

template<size_t Size>
size_t move_bench() {
    iobuf buffer = make_iobuf(Size);
    perf_tests::start_measuring_time();
    for (auto i = inner_iters; i--;) {
        iobuf moved = std::move(buffer);
        perf_tests::do_not_optimize(moved);
        buffer = std::move(moved);
    }
    perf_tests::stop_measuring_time();
    return inner_iters * 2;
}

// Overloaded make_rhs: convert iobuf to the desired RHS type
template<typename T>
T make_rhs(iobuf&& src);

template<>
iobuf make_rhs<iobuf>(iobuf&& src) {
    return std::move(src);
}

template<>
ss::sstring make_rhs<ss::sstring>(iobuf&& src) {
    return src.linearize_to_string();
}

// Get the view type for comparison (iobuf& or string_view)
const iobuf& as_cmp_arg(const iobuf& b) { return b; }
std::string_view as_cmp_arg(const ss::sstring& b) { return b; }

template<size_t Size, typename cmp_fn, bool same, typename rhs_type = iobuf>
size_t cmp_bench() {
    // LHS: iobuf (single fragment and fragmented variants)
    iobuf lhs = make_iobuf(Size);
    iobuf lhs_fragmented;
    for (const auto& frag : lhs) {
        for (char c : std::string_view(frag.get(), frag.size())) {
            lhs_fragmented.append(&c, 1);
        }
    }

    // RHS: converted to target type (iobuf or ss::sstring)
    auto rhs = make_rhs<rhs_type>(make_iobuf(Size, !same));

    perf_tests::start_measuring_time();
    for (auto i = inner_iters; i--;) {
        perf_tests::do_not_optimize(cmp_fn{}(lhs, as_cmp_arg(rhs)));
        perf_tests::do_not_optimize(cmp_fn{}(lhs_fragmented, as_cmp_arg(rhs)));
    }
    perf_tests::stop_measuring_time();
    return inner_iters * 2;
}

// This microbench mocks common pattern in Redpanda where smaller iobufs will be
// parsed out of a larger iobuf then appendded together. This pattern is heavily
// used in the datalake impl.
template<int Bufs, size_t BufSize>
size_t append_bench() {
    iobuf_parser parser(make_iobuf(Bufs * BufSize));

    std::vector<iobuf> buffers;
    buffers.reserve(Bufs);
    for (int i = 0; i < Bufs; i++) {
        buffers.push_back(parser.share(BufSize));
    }

    iobuf res;
    perf_tests::start_measuring_time();
    for (int i = 0; i < Bufs; i++) {
        res.append(std::move(buffers[i]));
    }
    perf_tests::stop_measuring_time();
    perf_tests::do_not_optimize(res);

    return Bufs;
}
} // namespace

// clang-format off
PERF_TEST(iobuf, cmp_bench_0000) { return cmp_bench<   0, std::less<>, false>(); }
PERF_TEST(iobuf, cmp_bench_0001) { return cmp_bench<   1, std::less<>, false>(); }
PERF_TEST(iobuf, cmp_bench_0002) { return cmp_bench<   2, std::less<>, false>(); }
PERF_TEST(iobuf, cmp_bench_0003) { return cmp_bench<   3, std::less<>, false>(); }
PERF_TEST(iobuf, cmp_bench_0004) { return cmp_bench<   4, std::less<>, false>(); }
PERF_TEST(iobuf, cmp_bench_0016) { return cmp_bench<  16, std::less<>, false>(); }
PERF_TEST(iobuf, cmp_bench_0064) { return cmp_bench<  64, std::less<>, false>(); }
PERF_TEST(iobuf, cmp_bench_0128) { return cmp_bench< 128, std::less<>, false>(); }
PERF_TEST(iobuf, cmp_bench_0256) { return cmp_bench< 256, std::less<>, false>(); }
PERF_TEST(iobuf, cmp_bench_0512) { return cmp_bench< 512, std::less<>, false>(); }
PERF_TEST(iobuf, cmp_bench_1024) { return cmp_bench<1024, std::less<>, false>(); }
PERF_TEST(iobuf, cmp_bench_2048) { return cmp_bench<2048, std::less<>, false>(); }
PERF_TEST(iobuf, cmp_bench_0000_same) { return cmp_bench<   0, std::less<>, true>(); }
PERF_TEST(iobuf, cmp_bench_0001_same) { return cmp_bench<   1, std::less<>, true>(); }
PERF_TEST(iobuf, cmp_bench_0002_same) { return cmp_bench<   2, std::less<>, true>(); }
PERF_TEST(iobuf, cmp_bench_0003_same) { return cmp_bench<   3, std::less<>, true>(); }
PERF_TEST(iobuf, cmp_bench_0004_same) { return cmp_bench<   4, std::less<>, true>(); }
PERF_TEST(iobuf, cmp_bench_0016_same) { return cmp_bench<  16, std::less<>, true>(); }
PERF_TEST(iobuf, cmp_bench_0064_same) { return cmp_bench<  64, std::less<>, true>(); }
PERF_TEST(iobuf, cmp_bench_0128_same) { return cmp_bench< 128, std::less<>, true>(); }
PERF_TEST(iobuf, cmp_bench_0256_same) { return cmp_bench< 256, std::less<>, true>(); }
PERF_TEST(iobuf, cmp_bench_0512_same) { return cmp_bench< 512, std::less<>, true>(); }
PERF_TEST(iobuf, cmp_bench_1024_same) { return cmp_bench<1024, std::less<>, true>(); }
PERF_TEST(iobuf, cmp_bench_2048_same) { return cmp_bench<2048, std::less<>, true>(); }
// clang-format on

PERF_TEST(iobuf, move_bench_small) { return move_bench<71>(); }
PERF_TEST(iobuf, move_bench_medium) { return move_bench<300_KiB>(); }
PERF_TEST(iobuf, move_bench_large) { return move_bench<965_KiB>(); }

PERF_TEST(iobuf, eq_bench_small) {
    return cmp_bench<71, std::equal_to<>, false>();
}
PERF_TEST(iobuf, eq_bench_small_same) {
    return cmp_bench<71, std::equal_to<>, true>();
}
PERF_TEST(iobuf, eq_bench_medium) {
    return cmp_bench<300_KiB, std::equal_to<>, false>();
}
PERF_TEST(iobuf, eq_bench_medium_same) {
    return cmp_bench<300_KiB, std::equal_to<>, true>();
}
PERF_TEST(iobuf, eq_bench_large) {
    return cmp_bench<965_KiB, std::equal_to<>, false>();
}
PERF_TEST(iobuf, eq_bench_large_same) {
    return cmp_bench<965_KiB, std::equal_to<>, true>();
}

PERF_TEST(iobuf, append_bench_small) { return append_bench<1'000, 4>(); }
PERF_TEST(iobuf, append_bench_medium) { return append_bench<1'000, 40_KiB>(); }
PERF_TEST(iobuf, append_bench_large) { return append_bench<1'000, 400_KiB>(); }

// iobuf vs string_view comparisons
// clang-format off
PERF_TEST(iobuf, sv_eq_0000)      { return cmp_bench<   0, std::equal_to<>, false, ss::sstring>(); }
PERF_TEST(iobuf, sv_eq_0001)      { return cmp_bench<   1, std::equal_to<>, false, ss::sstring>(); }
PERF_TEST(iobuf, sv_eq_1024)      { return cmp_bench<1024, std::equal_to<>, false, ss::sstring>(); }
PERF_TEST(iobuf, sv_eq_0000_same) { return cmp_bench<   0, std::equal_to<>, true,  ss::sstring>(); }
PERF_TEST(iobuf, sv_eq_0001_same) { return cmp_bench<   1, std::equal_to<>, true,  ss::sstring>(); }
PERF_TEST(iobuf, sv_eq_1024_same) { return cmp_bench<1024, std::equal_to<>, true,  ss::sstring>(); }

PERF_TEST(iobuf, sv_cmp_0000)      { return cmp_bench<   0, std::less<>, false, ss::sstring>(); }
PERF_TEST(iobuf, sv_cmp_0001)      { return cmp_bench<   1, std::less<>, false, ss::sstring>(); }
PERF_TEST(iobuf, sv_cmp_1024)      { return cmp_bench<1024, std::less<>, false, ss::sstring>(); }
PERF_TEST(iobuf, sv_cmp_0000_same) { return cmp_bench<   0, std::less<>, true,  ss::sstring>(); }
PERF_TEST(iobuf, sv_cmp_0001_same) { return cmp_bench<   1, std::less<>, true,  ss::sstring>(); }
PERF_TEST(iobuf, sv_cmp_1024_same) { return cmp_bench<1024, std::less<>, true,  ss::sstring>(); }
// clang-format on
