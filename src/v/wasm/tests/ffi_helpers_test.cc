/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "bytes/iobuf.h"
#include "bytes/random.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "random/generators.h"
#include "test_utils/test.h"
#include "wasm/ffi.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <limits>
#include <stdexcept>

namespace wasm::ffi {

using ::testing::ElementsAre;

struct flubber {
    int32_t foo = 0;
    std::string bar;
    iobuf baz;
    uint64_t qux = 0;

    friend bool operator==(const flubber&, const flubber&) = default;
};

template<typename T>
void serialize_flubber(const flubber& f, T* output) {
    output->append(f.foo);
    output->append_with_length(f.bar);
    output->append_with_length(f.baz);
    output->append(f.qux);
}

flubber deserialize_flubber(reader* r) {
    flubber f;
    f.foo = int32_t(r->read_varint());
    f.bar = r->read_sized_string();
    f.baz = r->read_sized_iobuf();
    f.qux = r->read_varint();
    return f;
}

TEST(FFIHelpers, CanRoundTrip) {
    flubber input{
      .foo = -1,
      .bar = "asdf",
      .baz = random_generators::make_iobuf(),
      .qux = std::numeric_limits<uint64_t>::max()};
    sizer s;
    serialize_flubber(input, &s);
    size_t serialized_size = s.total();
    std::vector<uint8_t> serialized(serialized_size, 0);
    array<uint8_t> ffi_array(serialized);
    writer w(ffi_array);
    serialize_flubber(input, &w);
    reader r(ffi_array);
    flubber output = deserialize_flubber(&r);
    ASSERT_EQ(input, output);
}

TEST(FFIHelpers, ReaderOverflow) {
    std::vector<uint8_t> data(4, 0);
    array<uint8_t> ffi_array(data);
    reader r(ffi_array);
    ASSERT_THROW(r.read_string(data.size() + 1), std::out_of_range);
}

TEST(FFIHelpers, WriterOverflow) {
    std::vector<uint8_t> data(2, 0);
    array<uint8_t> ffi_array(data);
    writer w(ffi_array);
    ASSERT_THROW(
      w.append(std::numeric_limits<int64_t>::max()), std::out_of_range);
    ASSERT_THROW(w.append("abc"), std::out_of_range);
    ASSERT_NO_THROW(w.append("ab"));
}

TEST(FFIHelpers, ConvertSignature) {
    std::vector<val_type> types;
    ffi::transform_types<
      ffi::memory*,
      ffi::array<int>,
      model::ns,
      int32_t,
      int64_t,
      uint64_t*>(types);
    ASSERT_THAT(
      types,
      ElementsAre(
        // memory is passed out of bound from the runtime
        // the array is a pointer and size
        val_type::i32,
        val_type::i32,
        // the named type is converted to a string which is a pointer and size
        val_type::i32,
        val_type::i32,
        // the int32 type
        val_type::i32,
        // the int64 type
        val_type::i64,
        // the pointer to the uint64 type
        val_type::i32));
}

TEST(FFIHelpers, ExtractParameters) {
    constexpr size_t array_offset = 42;
    constexpr size_t array_len = 2;
    static std::array<int32_t, array_len> guest_array{-1, 1};
    constexpr size_t ns_offset = 99;
    constexpr size_t num_ptr_offset = 65;
    constexpr uint64_t num_value = 654;
    static uint64_t num = num_value;
    class test_memory : public memory {
        // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
        void* translate_raw(ptr guest_ptr, uint32_t len) final {
            switch (guest_ptr) {
            case array_offset: {
                if (len != (array_len * sizeof(int32_t))) {
                    throw std::runtime_error(
                      ss::format("unexpected array len {}", len));
                }
                return guest_array.data();
            }
            case ns_offset:
                if (len != model::kafka_namespace().size()) {
                    throw std::runtime_error(
                      ss::format("unexpected ns len {}", len));
                }
                // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
                return const_cast<char*>(model::kafka_namespace().data());
            case num_ptr_offset:
                if (len != sizeof(int64_t)) {
                    throw std::runtime_error(
                      ss::format("unexpected num len {}", len));
                }
                return &num;
            default:
                throw std::runtime_error(
                  ss::format("unexpected ptr {}", guest_ptr));
            }
        }
    };
    test_memory mem;
    std::vector<uint64_t> ffi_types{
      array_offset,
      array_len,
      ns_offset,
      model::kafka_namespace().size(),
      std::numeric_limits<int32_t>::max(),
      static_cast<uint64_t>(std::numeric_limits<int64_t>::min()),
      num_ptr_offset,
    };
    auto [extracted_mem, array, ns, i32, i64, u64_ptr]
      = ffi::extract_parameters<
        ffi::memory*,
        ffi::array<int32_t>,
        model::ns,
        int32_t,
        int64_t,
        uint64_t*>(&mem, ffi_types, 0);
    ASSERT_EQ(extracted_mem, &mem);
    ASSERT_EQ(array.data(), guest_array.data());
    ASSERT_TRUE(std::equal(
      array.begin(), array.end(), guest_array.begin(), guest_array.end()));
    ASSERT_EQ(ns, model::kafka_namespace);
    ASSERT_EQ(i32, std::numeric_limits<int32_t>::max());
    ASSERT_EQ(i64, std::numeric_limits<int64_t>::min());
    ASSERT_EQ(u64_ptr, &num);
    ASSERT_EQ(*u64_ptr, num_value);
}

} // namespace wasm::ffi
