/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "model/record.h"
#include "random/generators.h"
#include "reflection/adl.h"
#include "storage/record_batch_builder.h"

#include <seastar/testing/thread_test_case.hh>

#include <absl/container/btree_map.h>
#include <boost/test/tools/old/interface.hpp>

namespace detail {

struct sample_type {
    ss::sstring key;
    ss::sstring value;
    absl::btree_map<ss::sstring, ss::sstring> kv_pairs;

    bool operator==(const sample_type& st) const {
        return key == st.key && value == st.value && kv_pairs == st.kv_pairs;
    }
    bool operator!=(const sample_type& st) const { return !(*this == st); }
};

std::ostream& operator<<(std::ostream& os, const sample_type& st) {
    os << "Key: " << st.key << " Value: " << st.value
       << " Number of headers: " << st.kv_pairs.size() << std::endl;
    return os;
}

void serialize_sample_type(storage::record_batch_builder& rbb, sample_type st) {
    iobuf key, value;
    std::vector<model::record_header> headers;
    reflection::serialize(key, std::move(st.key));
    reflection::serialize(value, std::move(st.value));
    std::transform(
      st.kv_pairs.begin(),
      st.kv_pairs.end(),
      std::back_inserter(headers),
      [](absl::btree_map<ss::sstring, ss::sstring>::value_type& kv_pair) {
          /// adl<ss::string> serializer appends a 4 byte int (size) to the
          /// payload, must account for this in the key & value size
          const auto key_size = kv_pair.first.size() + sizeof(int32_t);
          const auto value_size = kv_pair.second.size() + sizeof(int32_t);
          iobuf hkey, hval;
          reflection::serialize(hkey, kv_pair.first);
          reflection::serialize(hval, std::move(kv_pair.second));
          return model::record_header(
            key_size, std::move(hkey), value_size, std::move(hval));
      });
    rbb.add_raw_kw(std::move(key), std::move(value), std::move(headers));
}

sample_type deserialize_sample_type(model::record& r) {
    iobuf_parser key_parser(r.release_key());
    iobuf_parser val_parser(r.release_value());
    ss::sstring key = reflection::adl<ss::sstring>{}.from(key_parser);
    ss::sstring val = reflection::adl<ss::sstring>{}.from(val_parser);
    absl::btree_map<ss::sstring, ss::sstring> kv_pairs;
    for (auto& rh : r.headers()) {
        iobuf_parser key_parser(rh.release_key());
        iobuf_parser val_parser(rh.release_value());
        kv_pairs.emplace(
          reflection::adl<ss::sstring>{}.from(key_parser),
          reflection::adl<ss::sstring>{}.from(val_parser));
    }
    return sample_type{
      .key = std::move(key),
      .value = std::move(val),
      .kv_pairs = std::move(kv_pairs)};
}

} // namespace detail

SEASTAR_THREAD_TEST_CASE(empty_builder) {
    // positive case
    {
        storage::record_batch_builder rbb(
          model::record_batch_type::raft_data, model::offset(0));

        BOOST_REQUIRE(rbb.empty());
        auto rb = std::move(rbb).build();
        BOOST_CHECK(rb.empty());
        BOOST_CHECK_EQUAL(rb.record_count(), 0);
    }

    // negative case
    {
        storage::record_batch_builder rbb(
          model::record_batch_type::raft_data, model::offset(0));

        rbb.add_raw_kv(std::nullopt, std::nullopt);

        BOOST_REQUIRE(!rbb.empty());
        auto rb = std::move(rbb).build();
        BOOST_CHECK(!rb.empty());
        BOOST_CHECK_EQUAL(rb.record_count(), 1);
    }
}

SEASTAR_THREAD_TEST_CASE(serialize_deserialize_then_cmp) {
    /// 1. Build working set for test
    const std::size_t total = random_generators::get_int(50, 100);
    std::vector<detail::sample_type> sample_data;
    sample_data.reserve(total);
    for (size_t i = 0; i < total; ++i) {
        absl::btree_map<ss::sstring, ss::sstring> kv_pairs;
        const std::size_t kv_total = random_generators::get_int(2, 10);
        for (size_t j = 0; j < kv_total; ++j) {
            kv_pairs.emplace(
              random_generators::gen_alphanum_string(32),
              random_generators::gen_alphanum_string(32));
        }
        sample_data.push_back(detail::sample_type{
          .key = random_generators::gen_alphanum_string(32),
          .value = random_generators::gen_alphanum_string(32),
          .kv_pairs = std::move(kv_pairs)});
    }

    /// 2. Serialize to record_batch
    storage::record_batch_builder rbb(
      model::record_batch_type::raft_data, model::offset(0));
    for (const auto& st : sample_data) {
        detail::serialize_sample_type(rbb, st);
    }
    auto record_batch = std::move(rbb).build();

    /// 3. Deserialize and compare
    std::vector<detail::sample_type> sample_output;
    record_batch.for_each_record([&sample_output](model::record record) {
        sample_output.push_back(detail::deserialize_sample_type(record));
    });
    BOOST_CHECK_EQUAL(sample_data, sample_output);
}
