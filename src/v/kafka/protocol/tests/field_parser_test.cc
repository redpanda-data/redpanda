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

#include "kafka/protocol/request_reader.h"
#include "kafka/protocol/response_writer.h"
#include "kafka/protocol/types.h"
#include "kafka/types.h"
#include "random/generators.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/iterator/counting_iterator.hpp>

kafka::tagged_fields make_random_tags(size_t n) {
    kafka::tagged_fields tags;
    for (uint32_t i = 0; i < n; ++i) {
        tags.emplace_back(n, bytes_to_iobuf(random_generators::get_bytes()));
    }
    return tags;
}

kafka::tagged_fields copy_tags(const kafka::tagged_fields& otags) {
    kafka::tagged_fields tags;
    std::transform(
      otags.begin(), otags.end(), std::back_inserter(tags), [](auto& t) {
          auto& [tag_id, tag] = t;
          return std::make_tuple(tag_id, tag.copy());
      });
    return tags;
}

SEASTAR_THREAD_TEST_CASE(serde_tags) {
    iobuf buf;
    auto tags = make_random_tags(10);

    /// Serialize the random tags into an iobuf
    kafka::response_writer writer(buf);
    writer.write_tags(std::move(copy_tags(tags)));

    /// Copy the result to use for a later comparison
    iobuf copy = buf.copy();

    /// Deserialize the tags with the kafka::request_reader
    kafka::request_reader reader(std::move(buf));
    auto deser_tags = reader.read_tags();

    /// Verify the inital values are equivalent
    BOOST_REQUIRE(copy_tags(tags) == deser_tags);

    /// Re-serialize these tags to compare against the previous
    iobuf result;
    kafka::response_writer end_writer(result);
    end_writer.write_tags(std::move(deser_tags));

    /// Perform checks against serialized copies
    BOOST_REQUIRE_EQUAL(result.size_bytes(), copy.size_bytes());
    BOOST_CHECK_EQUAL(iobuf_to_bytes(result), iobuf_to_bytes(copy));
}

struct test_struct {
    ss::sstring field_a;
    int32_t field_b;

    static test_struct make_random() {
        return test_struct{
          .field_a = random_generators::gen_alphanum_string(5),
          .field_b = random_generators::get_int(0, 15)};
    }

    friend bool operator==(const test_struct& a, const test_struct& b) {
        return a.field_a == b.field_a && a.field_b == b.field_b;
    }

    friend std::ostream& operator<<(std::ostream& os, const test_struct& ts) {
        os << "field_a: " << ts.field_a << " field_b: " << ts.field_b;
        return os;
    }
};

/// Only includes impls of supported types used in following unit tests
template<typename T>
void write_flex(T& type, iobuf& buf) {
    kafka::response_writer writer(buf);
    if constexpr (std::is_same_v<T, std::vector<test_struct>>) {
        writer.write_flex_array(
          type, [](test_struct& ts, kafka::response_writer& writer) {
              writer.write_flex(ts.field_a);
              writer.write(ts.field_b);
              writer.write_tags();
          });
    } else if constexpr (std::is_same_v<
                           T,
                           std::optional<std::vector<test_struct>>>) {
        writer.write_nullable_flex_array(
          type, [](test_struct& ts, kafka::response_writer& writer) {
              writer.write_flex(ts.field_a);
              writer.write(ts.field_b);
              writer.write_tags();
          });
    } else if constexpr (std::is_same_v<T, kafka::uuid>) {
        writer.write(type);
    } else {
        writer.write_flex(type);
    }
}

template<typename T>
T read_flex(iobuf buf) {
    kafka::request_reader reader(std::move(buf));
    if constexpr (std::is_same_v<T, ss::sstring>) {
        return reader.read_flex_string();
    } else if constexpr (std::is_same_v<T, kafka::uuid>) {
        return reader.read_uuid();
    } else if constexpr (std::is_same_v<T, std::optional<ss::sstring>>) {
        return reader.read_nullable_flex_string();
    } else if constexpr (std::is_same_v<T, bytes>) {
        return reader.read_flex_bytes();
    } else if constexpr (std::is_same_v<T, std::vector<test_struct>>) {
        return reader.read_flex_array([](kafka::request_reader& reader) {
            test_struct v;
            v.field_a = reader.read_flex_string();
            v.field_b = reader.read_int32();
            reader.consume_tags();
            return v;
        });
    } else if constexpr (std::is_same_v<
                           T,
                           std::optional<std::vector<test_struct>>>) {
        return reader.read_nullable_flex_array(
          [](kafka::request_reader& reader) {
              test_struct v;
              v.field_a = reader.read_flex_string();
              v.field_b = reader.read_int32();
              reader.consume_tags();
              return v;
          });
    }
}

template<typename T>
T serde_flex(T& type) {
    iobuf buf;
    write_flex(type, buf);
    return read_flex<T>(std::move(buf));
}

SEASTAR_THREAD_TEST_CASE(serde_flex_types) {
    auto gen_random_string = []() {
        const auto str_len = random_generators::get_int(0, 20);
        return random_generators::gen_alphanum_string(15);
    };
    {
        /// uuid
        auto bytes = random_generators::get_bytes(16);
        auto encoded = bytes_to_base64(bytes);
        auto uuid = kafka::uuid::from_string(encoded);
        auto rt = serde_flex(uuid);
        BOOST_CHECK_EQUAL(uuid.view(), rt.view());
        BOOST_CHECK_EQUAL(encoded, rt.to_string());
    }
    {
        /// flex strings
        auto str = gen_random_string();
        BOOST_CHECK_EQUAL(str, serde_flex(str));
    }
    {
        /// optional flex strings
        auto opt_str = std::optional<ss::sstring>(gen_random_string());
        BOOST_CHECK(opt_str == serde_flex(opt_str));

        {
            /// .. of a value of nullopt
            std::optional<ss::sstring> null_str;
            BOOST_CHECK(null_str == serde_flex(null_str));
        }
    }
    {
        /// flex bytes
        auto b = random_generators::get_bytes();
        BOOST_CHECK(b == serde_flex(b));
    }
    {
        /// flex array
        std::vector<test_struct> v;
        std::for_each(
          boost::counting_iterator<int>(0),
          boost::counting_iterator<int>(100),
          [&v](int) { v.push_back(test_struct::make_random()); });
        BOOST_CHECK_EQUAL(v, serde_flex(v));
    }
    {
        /// optional flex array
        std::optional<std::vector<test_struct>> v = std::vector<test_struct>();
        std::for_each(
          boost::counting_iterator<int>(0),
          boost::counting_iterator<int>(100),
          [&v](int) { v->push_back(test_struct::make_random()); });
        BOOST_CHECK(v == serde_flex(v));

        {
            /// ... of a value of nullopt
            std::optional<std::vector<test_struct>> null_vec;
            BOOST_CHECK(null_vec == serde_flex(null_vec));
        }
    }
    {
        /// flex iobuf
        auto str = gen_random_string();
        iobuf data, writers_buf, copy;
        data.append(str.begin(), str.length());
        copy.append(str.begin(), str.length());
        kafka::response_writer writer(writers_buf);
        writer.write_flex(std::move(data));

        kafka::request_reader reader(std::move(writers_buf));
        auto result = reader.read_fragmented_nullable_flex_bytes();
        BOOST_REQUIRE(result.has_value());
        BOOST_CHECK_EQUAL(iobuf_to_bytes(*result), iobuf_to_bytes(copy));
    }
}
