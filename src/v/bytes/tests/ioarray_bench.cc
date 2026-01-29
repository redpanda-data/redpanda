// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/units.h"
#include "bytes/ioarray.h"

#include <seastar/testing/perf_tests.hh>

namespace {
static constexpr size_t inner_iters = 1000;

// Create a deterministic ioarray for consistent benchmarks.
ioarray make_ioarray(size_t size) {
    ioarray arr(size);
    auto gen = std::views::iota('a', 'z');
    size_t idx = 0;
    while (idx < size) {
        for (char c : gen) {
            arr[idx++] = c;
            if (idx == size) {
                break;
            }
        }
    }
    return arr;
}

// Benchmark read_fixed32 at different positions within the ioarray.
// This tests both aligned and unaligned reads, as well as reads that cross
// chunk boundaries.
template<size_t Size>
size_t read_fixed32_bench() {
    ioarray arr = make_ioarray(Size);
    // Fill with some deterministic pattern for uint32 values
    for (size_t i = 0; i + sizeof(uint32_t) <= Size; i += sizeof(uint32_t)) {
        uint32_t val = static_cast<uint32_t>(i);
        arr[i] = static_cast<char>(val & 0xFF);
        arr[i + 1] = static_cast<char>((val >> 8) & 0xFF);
        arr[i + 2] = static_cast<char>((val >> 16) & 0xFF);
        arr[i + 3] = static_cast<char>((val >> 24) & 0xFF);
    }

    // Benchmark reads at various positions
    size_t num_reads = 0;
    perf_tests::start_measuring_time();
    for (auto iter = inner_iters; iter--;) {
        // Read at different positions throughout the array
        for (size_t pos = 0; pos + sizeof(uint32_t) <= Size;
             pos += 64) { // Step by 64 bytes
            uint32_t val = arr.read_fixed32(pos);
            perf_tests::do_not_optimize(val);
            ++num_reads;
        }
    }
    perf_tests::stop_measuring_time();
    return num_reads;
}

// Benchmark sequential reads of fixed32 values
template<size_t Size>
size_t read_fixed32_sequential_bench() {
    ioarray arr = make_ioarray(Size);
    // Fill with some deterministic pattern
    for (size_t i = 0; i + sizeof(uint32_t) <= Size; i += sizeof(uint32_t)) {
        uint32_t val = static_cast<uint32_t>(i);
        arr[i] = static_cast<char>(val & 0xFF);
        arr[i + 1] = static_cast<char>((val >> 8) & 0xFF);
        arr[i + 2] = static_cast<char>((val >> 16) & 0xFF);
        arr[i + 3] = static_cast<char>((val >> 24) & 0xFF);
    }

    size_t num_reads = 0;
    perf_tests::start_measuring_time();
    for (auto iter = inner_iters; iter--;) {
        // Sequential reads
        for (size_t pos = 0; pos + sizeof(uint32_t) <= Size;
             pos += sizeof(uint32_t)) {
            uint32_t val = arr.read_fixed32(pos);
            perf_tests::do_not_optimize(val);
            ++num_reads;
        }
    }
    perf_tests::stop_measuring_time();
    return num_reads;
}

// Benchmark reads near chunk boundaries (most challenging case)
template<size_t Size>
size_t read_fixed32_boundary_bench() {
    ioarray arr = make_ioarray(Size);
    // Fill with deterministic pattern
    for (size_t i = 0; i < Size; ++i) {
        arr[i] = static_cast<char>(i & 0xFF);
    }

    size_t num_reads = 0;
    perf_tests::start_measuring_time();
    for (auto iter = inner_iters; iter--;) {
        // Read at positions near chunk boundaries
        constexpr size_t chunk_size = ioarray::max_chunk_size;
        for (size_t chunk = 0; chunk < Size; chunk += chunk_size) {
            for (int offset = chunk == 0 ? 0 : -8; offset <= 8; offset += 4) {
                size_t pos = chunk + offset;
                if (pos <= Size - sizeof(uint32_t)) {
                    uint32_t val = arr.read_fixed32(pos);
                    perf_tests::do_not_optimize(val);
                    ++num_reads;
                }
            }
        }
    }
    perf_tests::stop_measuring_time();
    return num_reads;
}
} // namespace

// Small size benchmarks (within a single chunk)
PERF_TEST(ioarray, read_fixed32_small) { return read_fixed32_bench<1024>(); }
PERF_TEST(ioarray, read_fixed32_sequential_small) {
    return read_fixed32_sequential_bench<1024>();
}

// Medium size benchmarks (spans a few chunks)
PERF_TEST(ioarray, read_fixed32_medium) {
    return read_fixed32_bench<256_KiB>();
}
PERF_TEST(ioarray, read_fixed32_sequential_medium) {
    return read_fixed32_sequential_bench<256_KiB>();
}

// Large size benchmarks (spans many chunks)
PERF_TEST(ioarray, read_fixed32_large) { return read_fixed32_bench<1_MiB>(); }
PERF_TEST(ioarray, read_fixed32_sequential_large) {
    return read_fixed32_sequential_bench<1_MiB>();
}

// Boundary crossing benchmarks (most challenging case)
PERF_TEST(ioarray, read_fixed32_boundary_small) {
    return read_fixed32_boundary_bench<256_KiB>();
}
PERF_TEST(ioarray, read_fixed32_boundary_medium) {
    return read_fixed32_boundary_bench<512_KiB>();
}
PERF_TEST(ioarray, read_fixed32_boundary_large) {
    return read_fixed32_boundary_bench<1_MiB>();
}
