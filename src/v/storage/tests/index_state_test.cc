#define BOOST_TEST_MODULE storage
#include "bytes/bytes.h"
#include "random/generators.h"
#include "serde/serde.h"
#include "storage/index_state.h"
#include "storage/index_state_serde_compat.h"

#include <boost/test/unit_test.hpp>

static storage::index_state make_random_index_state() {
    storage::index_state st;
    st.size = random_generators::get_int<uint32_t>();
    st.checksum = random_generators::get_int<uint64_t>();
    st.bitflags = random_generators::get_int<uint32_t>();
    st.base_offset = model::offset(random_generators::get_int<int64_t>());
    st.max_offset = model::offset(random_generators::get_int<int64_t>());
    st.base_timestamp = model::timestamp(random_generators::get_int<int64_t>());
    st.max_timestamp = model::timestamp(random_generators::get_int<int64_t>());

    const auto n = random_generators::get_int(1, 10000);
    for (auto i = 0; i < n; ++i) {
        st.add_entry(
          random_generators::get_int<uint32_t>(),
          random_generators::get_int<uint32_t>(),
          random_generators::get_int<uint64_t>());
        return st;
    }

    return st;
}

static void set_version(iobuf& buf, int8_t version) {
    auto tmp = iobuf_to_bytes(buf);
    buf.clear();
    buf.append((const char*)&version, sizeof(version));
    buf.append(bytes_to_iobuf(tmp.substr(1)));
}

static void set_size(iobuf& buf, uint32_t size) {
    auto tmp = iobuf_to_bytes(buf);
    buf.clear();
    buf.append((const char*)tmp.c_str(), 1); // version
    uint32_t size_le = ss::cpu_to_le(size);
    buf.append((const char*)&size_le, sizeof(size_le));
    buf.append(bytes_to_iobuf(tmp.substr(5)));
}

BOOST_AUTO_TEST_CASE(encode_decode) {
    auto src = make_random_index_state();
    auto src_buf = src.checksum_and_serialize();

    auto dst = storage::index_state::hydrate_from_buffer(src_buf.copy());
    BOOST_REQUIRE(dst);
    BOOST_REQUIRE_EQUAL(src, *dst);

    auto dst_buf = dst->checksum_and_serialize();
    BOOST_REQUIRE_EQUAL(src_buf, dst_buf);
}

BOOST_AUTO_TEST_CASE(encode_decode_clipped) {
    auto src = make_random_index_state();
    auto src_buf = src.checksum_and_serialize();

    // trim off some data from the end
    BOOST_REQUIRE_GT(src_buf.size_bytes(), 10);
    src_buf.trim_back(src_buf.size_bytes() - 10);

    auto dst = storage::index_state::hydrate_from_buffer(src_buf.copy());
    BOOST_REQUIRE(!dst);
}

BOOST_AUTO_TEST_CASE(encode_decode_v0) {
    auto src = make_random_index_state();
    auto src_buf = src.checksum_and_serialize();
    set_version(src_buf, 0);

    // version 0 is fully deprecated
    auto dst = storage::index_state::hydrate_from_buffer(src_buf.copy());
    BOOST_REQUIRE(!dst);
}

BOOST_AUTO_TEST_CASE(encode_decode_v1) {
    auto src = make_random_index_state();
    auto src_buf = src.checksum_and_serialize();
    set_version(src_buf, 1);
    set_size(src_buf, src.size - 4);

    // version 1 is fully deprecated
    auto dst = storage::index_state::hydrate_from_buffer(src_buf.copy());
    BOOST_REQUIRE(!dst);
}

BOOST_AUTO_TEST_CASE(encode_decode_v2) {
    auto src = make_random_index_state();
    auto src_buf = src.checksum_and_serialize();
    set_version(src_buf, 2);

    // version 2 is fully deprecated
    auto dst = storage::index_state::hydrate_from_buffer(src_buf.copy());
    BOOST_REQUIRE(!dst);
}

BOOST_AUTO_TEST_CASE(encode_decode_future_version) {
    auto src = make_random_index_state();
    auto src_buf = src.checksum_and_serialize();
    set_version(
      src_buf, storage::serde_compat::index_state_serde::ondisk_version + 1);

    // cannot decode future version
    auto dst = storage::index_state::hydrate_from_buffer(src_buf.copy());
    BOOST_REQUIRE(!dst);
}

// encode/decode using new serde framework
BOOST_AUTO_TEST_CASE(serde_basic) {
    for (int i = 0; i < 100; ++i) {
        auto input = make_random_index_state();
        const auto input_copy = input.copy();
        BOOST_REQUIRE_EQUAL(input, input_copy);

        // objects are equal
        const auto buf = serde::to_iobuf(std::move(input));
        auto output = serde::from_iobuf<storage::index_state>(buf.copy());
        BOOST_REQUIRE_EQUAL(output, input_copy);

        // round trip back to equal iobufs
        const auto buf2 = serde::to_iobuf(std::move(output));
        BOOST_REQUIRE_EQUAL(buf, buf2);
    }
}

// accept decoding supported old version
BOOST_AUTO_TEST_CASE(serde_supported_deprecated) {
    for (int i = 0; i < 100; ++i) {
        auto input = make_random_index_state();
        const auto output = serde::from_iobuf<storage::index_state>(
          storage::serde_compat::index_state_serde::encode(input));
        BOOST_REQUIRE_EQUAL(output, input);
    }
}

// reject decoding unsupported old versions
BOOST_AUTO_TEST_CASE(serde_unsupported_deprecated) {
    auto test = [](int version) {
        auto input = make_random_index_state();
        auto buf = storage::serde_compat::index_state_serde::encode(input);
        set_version(buf, version);

        BOOST_REQUIRE_EXCEPTION(
          const auto output = serde::from_iobuf<storage::index_state>(
            buf.copy()),
          serde::serde_exception,
          [version](const serde::serde_exception& e) {
              auto s = fmt::format("Unsupported version: {}", version);
              return std::string_view(e.what()).find(s)
                     != std::string_view::npos;
          });
    };
    test(0);
    test(1);
    test(2);
}

// decoding should fail if all the data isn't available
BOOST_AUTO_TEST_CASE(serde_clipped) {
    auto input = make_random_index_state();
    auto buf = serde::to_iobuf(std::move(input));

    // trim off some data from the end
    BOOST_REQUIRE_GT(buf.size_bytes(), 10);
    buf.trim_back(buf.size_bytes() - 10);

    BOOST_REQUIRE_EXCEPTION(
      serde::from_iobuf<storage::index_state>(buf.copy()),
      serde::serde_exception,
      [](const serde::serde_exception& e) {
          return std::string_view(e.what()).find("bytes_left")
                 != std::string_view::npos;
      });
}

// decoding deprecated format should fail if not all data is available
BOOST_AUTO_TEST_CASE(serde_deprecated_clipped) {
    auto input = make_random_index_state();
    auto buf = storage::serde_compat::index_state_serde::encode(input);

    // trim off some data from the end
    BOOST_REQUIRE_GT(buf.size_bytes(), 10);
    buf.trim_back(buf.size_bytes() - 10);

    BOOST_REQUIRE_EXCEPTION(
      serde::from_iobuf<storage::index_state>(buf.copy()),
      serde::serde_exception,
      [](const serde::serde_exception& e) {
          return std::string_view(e.what()).find(
                   "Index size does not match header size")
                 != std::string_view::npos;
      });
}
