#include <seastar/core/sstring.hh>

#include <smf/fbs_typed_buf.h>
#include <smf/native_type_utils.h>
#include <smf/random.h>

#include <gtest/gtest.h>

#include <iostream>
#include <utility>

// filesystem
#include "wal_segment_record.h"

/*
  BUGFIX: When flatbuffers reallocs, it copies the bytes incorrectly
  flipping the byte ordering

  ➜  debug git:(feature/correct_writes) ✗
  od --format=x1 --read-bytes=152
  /home/agallego/.redpanda/nemo/rickenbacker/15/0.wal

  0000000 56 6d 6b 6e 55 73 67 6a 50 32 41 68 58 67 73 58
  0000020 63 44 49 72 41 6f 5a 35 48 46 78 66 6f 72 52 61
  0000040 6b 51 34 74 77 48 31 4c 78 65 32 47 62 51 4e 70
  0000060 48 38 58 63 79 71 76 52 67 6e 4f 7a 6a 58 77 6e
  0000100 65 61 38 68 68 64 68 47 5a 76 51 6c 72 42 71 44
  0000120 4c 64 47 77 72 74 35 67 77 6e 46 57 68 4c 6c 32
  0000140 6f 74 4b 63 5a 72 77 79 7a 48 51 67 72 4f 4b 52
  0000160 30 78 46 64 4b 4c 66 61 52 31 35 7a 38 6a 53 4e
  0000200 2d c1 30 1f 00 00 00 00 e3 2b a5 7e 14 00 00 00
  0000220 b4 00 00 00 00 01 fe ca


  This is wrong because the layout should be:

                      'f' 'o' 'o'
  Key:                 66  6f  6f

                      'b' 'a' 'r'
  Value:               62  61  72

  Record Mem:          0  0  0  0  0  0  0  0  f9  aa  85  90  3  0  0  0
                       3  0  0  0  0  1  fe  ca  66  6f  6f  62  61  72

  Record Wrie:         0  0  0  0  0  0  0  0  f9  aa  85  90  3  0  0  0
                       3  0  0  0  0  1  fe  ca  66  6f  6f  62  61  72

 */

void hex_print(const uint8_t* ptr, uint32_t sz) {
    for (auto i = 0u; i < sz; ++i) {
        std::printf(" %x ", ptr[i]);
    }
    std::cout << std::endl;
}

TEST(wal_binary_record_order, small_on_wire_vs_on_mem) {
    seastar::sstring key = "foo";
    seastar::sstring value = "bar";
    auto record_mem = wal_segment_record::coalesce(
      key.data(), key.size(), value.data(), value.size());
    auto body = smf::native_table_as_buffer<wal_binary_record>(
      *record_mem.get());
    auto record_wire = smf::fbs_typed_buf<wal_binary_record>(std::move(body));

    std::cout << "Key:\t\t";
    hex_print((uint8_t*)key.data(), key.size());
    std::cout << "Value:\t\t";
    hex_print((uint8_t*)value.data(), value.size());
    std::cout << "Record Mem:\t\t";
    hex_print(record_mem->data.data(), record_mem->data.size());
    std::cout << "Record Wrie:\t";
    hex_print(record_wire->data()->Data(), record_wire->data()->size());
    ASSERT_TRUE(
      std::memcmp(
        record_mem->data.data(),
        record_wire->data()->Data(),
        record_wire->data()->size())
      == 0);
}

TEST(wal_binary_record_order, large_on_wire_vs_on_mem) {
    smf::random r;
    seastar::sstring key = r.next_alphanum(1024 * 1024);
    seastar::sstring value = r.next_alphanum(1024 * 1024);
    auto record_mem = wal_segment_record::coalesce(
      key.data(), key.size(), value.data(), value.size());
    auto body = smf::native_table_as_buffer<wal_binary_record>(
      *record_mem.get());
    auto record_wire = smf::fbs_typed_buf<wal_binary_record>(std::move(body));

    // std::cout << "Record Mem:\t\t";
    // hex_print(record_mem->data.data(), record_mem->data.size());
    // std::cout << "Record Wrie:\t";
    // hex_print(record_wire->data()->Data(), record_wire->data()->size());
    ASSERT_TRUE(
      std::memcmp(
        record_mem->data.data(),
        record_wire->data()->Data(),
        record_wire->data()->size())
      == 0);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
