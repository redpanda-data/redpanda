// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/seastarx.h"
#include "base/units.h"
#include "pandaproxy/schema_registry/protobuf.h"
#include "pandaproxy/schema_registry/test/protobuf_utils.h"
#include "pandaproxy/schema_registry/test/store_fixture.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/core/memory.hh>
#include <seastar/util/defer.hh>

#include <fmt/core.h>
#include <gtest/gtest.h>

#include <new>

namespace pps = pandaproxy::schema_registry;

struct memory_chunk {
    std::vector<std::byte> buffer;

    explicit memory_chunk(size_t n_bytes)
      : buffer{n_bytes} {}
    memory_chunk(const memory_chunk&) = default;
    memory_chunk(memory_chunk&&) = default;
    memory_chunk& operator=(const memory_chunk&) = default;
    memory_chunk& operator=(memory_chunk&&) = default;
    ~memory_chunk() = default;
};

namespace {
// Fill up the available memory and then release every second
// object to end up with a very fragment memory
std::vector<std::optional<memory_chunk>> fragment_memory() {
    // Chunk size set to the same size as the maximum allocation of the chunked
    // vector
    constexpr auto chunk_size = 128_KiB;

    std::vector<std::optional<memory_chunk>> chunks;
    chunks.reserve(ss::memory::stats().total_memory() / chunk_size);

    while (ss::memory::stats().free_memory() > chunk_size) {
        try {
            chunks.emplace_back(memory_chunk{chunk_size});
        } catch (std::bad_alloc&) {
            // we might run out of memory sooner, due to existing
            // framentation in memory. That's ok. Just exit the loop
            break;
        }
    }

    const auto n_chunks = chunks.size();
    for (size_t i = 0; i < n_chunks; i += 2) {
        chunks[i].reset();
    }
    return chunks;
}

} // namespace

TEST(ProtoLargeAlloc, test_protobuf_memory_stuff) {
#ifdef SEASTAR_DEFAULT_ALLOCATOR
    GTEST_SKIP() << "Default seastar allocator detected. Skipping large alloc "
                    "protobuf test";
#endif
    ss::memory::set_abort_on_allocation_failure(false);
    auto allocation_failure_guard = ss::defer(
      []() { ss::memory::set_abort_on_allocation_failure(true); });

    pps::test_utils::store_fixture s;

    // Size of these schemas is set to around half-megabyte. The exact value is
    // not important, as long as it is significantly larger than the
    // fragmentation interval introduced in fragment_memory
    const pps::subject sub{"test_subject_01"};
    const auto large_schema = pps::test_utils::make_proto_schema(sub, 25'000);
    EXPECT_EQ(large_schema.size(), 552841);

    // Compiles the large_schema and returns the number of fallback
    // allocations needed for this operation
    const auto make_proto = [&s, &sub, &large_schema]() {
        const ss::memory::statistics start_stats = ss::memory::stats();
        auto canocal_schema
          = pps::make_canonical_protobuf_schema(
              s.store(),
              pps::subject_schema{
                sub,
                pps::schema_definition{
                  large_schema, pps::schema_type::protobuf, {}}})
              .get();
        const ss::memory::statistics post_stats = ss::memory::stats();

        return post_stats.fallback_allocations()
               - start_stats.fallback_allocations();
    };

    // When there is enough free contiguous memory, verify that the schema
    // doesn't need any fallback system allocations.
    EXPECT_EQ(make_proto(), 0);

    const std::vector<std::optional<memory_chunk>> chunks = fragment_memory();

    // Since there isn't enough contiguous free memory, any request in proto
    // that cannot be handled using the seastar allocator will fallback to the
    // system allocator. We don't check the exact number of fallback
    // allocations, as this is an implementation detail of how things are being
    // handled in the libproto. Here we only verify that there have been some
    // fallback allocations.
    EXPECT_GT(make_proto(), 0);
}
