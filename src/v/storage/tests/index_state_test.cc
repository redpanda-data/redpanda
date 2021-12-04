#define BOOST_TEST_MODULE storage
#include "bytes/bytes.h"
#include "random/generators.h"
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
