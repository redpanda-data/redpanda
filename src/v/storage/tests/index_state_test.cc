#define BOOST_TEST_MODULE storage
#include "bytes/bytes.h"
#include "random/generators.h"
#include "serde/serde.h"
#include "storage/index_state.h"
#include "storage/index_state_serde_compat.h"

#include <boost/test/unit_test.hpp>

static storage::index_state make_random_index_state() {
    storage::index_state st;
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

BOOST_AUTO_TEST_CASE(serde_crc) {
    auto input = make_random_index_state();
    auto good_buf = serde::to_iobuf(std::move(input));

    auto bad_bytes = iobuf_to_bytes(good_buf);
    auto& bad_byte = bad_bytes[bad_bytes.size() / 2];
    bad_byte += 1;
    auto bad_buf = bytes_to_iobuf(bad_bytes);

    BOOST_REQUIRE_EXCEPTION(
      serde::from_iobuf<storage::index_state>(bad_buf.copy()),
      serde::serde_exception,
      [](const serde::serde_exception& e) {
          return std::string_view(e.what()).find("Mismatched checksum")
                 != std::string_view::npos;
      });
}

BOOST_AUTO_TEST_CASE(serde_deprecated_crc) {
    auto input = make_random_index_state();
    auto good_buf = storage::serde_compat::index_state_serde::encode(input);

    auto bad_bytes = iobuf_to_bytes(good_buf);
    auto& bad_byte = bad_bytes[bad_bytes.size() / 2];
    bad_byte += 1;
    auto bad_buf = bytes_to_iobuf(bad_bytes);

    BOOST_REQUIRE_EXCEPTION(
      serde::from_iobuf<storage::index_state>(bad_buf.copy()),
      serde::serde_exception,
      [](const serde::serde_exception& e) {
          return std::string_view(e.what()).find("Invalid checksum for index")
                 != std::string_view::npos;
      });
}
