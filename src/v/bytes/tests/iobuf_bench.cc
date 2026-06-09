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

// Make an iobuf with many small fragments.
iobuf make_fragmented_iobuf(size_t num_frags, size_t frag_size) {
    iobuf result;
    auto data = std::string(frag_size, 'x');
    for (size_t i = 0; i < num_frags; ++i) {
        iobuf frag;
        frag.append(data.data(), data.size());
        result.append_fragments(std::move(frag));
    }
    return result;
}

// Deep-copy an iobuf preserving its fragment structure. iobuf::copy() reflows
// through the standard allocator, coalescing many small fragments into fewer
// large ones; this helper appends each source fragment as its own fragment in
// the result so fragment count is preserved.
iobuf deep_copy_keep_frags(const iobuf& src) {
    iobuf result;
    for (const auto& frag : src) {
        iobuf tmp;
        tmp.append(frag.get(), frag.size());
        result.append_fragments(std::move(tmp));
    }
    return result;
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

// Microbench for fragment _creation_: each iteration forces a brand new
// fragment to be allocated (control block + backing buffer) and the data
// copied in. This is the path hit when building/growing an iobuf and when
// deep-copying one, which the share-based benches above do not exercise.
template<size_t NumFrags, size_t FragSize>
size_t create_frags_bench() {
    const auto data = std::string(FragSize, 'x');
    perf_tests::start_measuring_time();
    iobuf buf;
    for (size_t i = 0; i < NumFrags; ++i) {
        buf.reserve_memory(FragSize);
        buf.append(data.data(), data.size());
    }
    perf_tests::stop_measuring_time();
    perf_tests::do_not_optimize(buf);
    return NumFrags;
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

PERF_TEST(iobuf, create_frags_bench_128) {
    return create_frags_bench<128, 512>();
}
PERF_TEST(iobuf, create_frags_bench_1k) {
    return create_frags_bench<1'000, 512>();
}

namespace {
// Build n deep copies of source upfront so each iteration consumes a fresh
// iobuf via the destructive as_scattered() &&. inputs and outputs are
// explicitly cleared inside the timed region so any deferred cleanup work
// (fragment disposal, underlying-buffer frees) is included in the
// measurement, regardless of whether the as_scattered() implementation
// performs that work eagerly or defers it to ~iobuf / ~temporary_buffer.
//
// The batch size scales inversely with input size to cap upfront allocation
// at ~100 MiB (10% of the 1 GiB budget allocated to the test).
size_t as_scattered_bench(const iobuf& source) {
    constexpr size_t budget = 100_MiB;
    const size_t per_input = std::max<size_t>(source.size_bytes(), 1);
    const size_t n = std::clamp<size_t>(budget / per_input, 1, inner_iters);

    std::vector<iobuf> inputs;
    inputs.reserve(n);
    for (size_t i = 0; i < n; i++) {
        inputs.push_back(deep_copy_keep_frags(source));
    }
    std::vector<scattered_buffer> outputs;
    outputs.reserve(n);

    perf_tests::start_measuring_time();
    for (auto& buf : inputs) {
        outputs.push_back(std::move(buf).as_scattered());
    }
    perf_tests::do_not_optimize(outputs);
    outputs.clear();
    inputs.clear();
    perf_tests::stop_measuring_time();
    return n;
}

} // namespace

PERF_TEST(iobuf, as_scattered_small) {
    return as_scattered_bench(make_iobuf(71));
}

PERF_TEST(iobuf, as_scattered_large) {
    return as_scattered_bench(make_iobuf(965_KiB));
}

PERF_TEST(iobuf, as_scattered_many_frags) {
    return as_scattered_bench(make_fragmented_iobuf(1000, 10));
}

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
