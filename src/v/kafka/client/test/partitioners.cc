// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iobuf_parser.h"
#include "hashing/murmur.h"
#define BOOST_TEST_MODULE kafka_client_unit

#include "kafka/client/partitioners.h"
#include "model/fundamental.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

namespace k = kafka;
namespace kc = k::client;

iobuf make_iobuf(std::string_view sv) {
    iobuf buf{};
    buf.append(sv.data(), sv.size());
    return buf;
}

model::partition_id murmur2(const iobuf& buf, size_t p_cnt) {
    iobuf_const_parser parser{buf};
    auto b = parser.read_bytes(parser.bytes_left());
    auto hash = murmur2(b.data(), b.size());
    return model::partition_id(hash % p_cnt);
}

static const auto no_partition{std::nullopt};
static const auto no_key{std::nullopt};
static const auto no_value{std::nullopt};
static const auto empty_key{iobuf()};
static const auto empty_value{iobuf()};
static const auto a_partition{model::partition_id{4}};
static const auto initial_partition{model::partition_id{5}};

iobuf a_key() { return make_iobuf("the key"); };

static const auto match_partition = kc::record_essence{
  .partition_id = a_partition, .key = no_key, .value = no_value, .headers{}};
static const auto match_key = kc::record_essence{
  .partition_id = no_partition, .key = a_key(), .value = no_value, .headers{}};
static const auto match_none = kc::record_essence{
  .partition_id = no_partition, .key = no_key, .value = no_value, .headers{}};

BOOST_AUTO_TEST_CASE(test_identity_partitioner) {
    auto partitioner{kc::identity_partitioner()};
    BOOST_REQUIRE(partitioner(match_none, 6) == std::nullopt);
    BOOST_REQUIRE_EQUAL(*partitioner(match_partition, 6), a_partition);
}

BOOST_AUTO_TEST_CASE(test_murmur2_key_partitioner) {
    auto partitioner{kc::murmur2_key_partitioner()};
    BOOST_REQUIRE(partitioner(match_none, 6) == std::nullopt);
    BOOST_REQUIRE_EQUAL(*partitioner(match_key, 6), murmur2(a_key(), 6));
}

BOOST_AUTO_TEST_CASE(test_roundrobin_partitioner) {
    auto partitioner{kc::roundrobin_partitioner(initial_partition)};
    BOOST_REQUIRE_EQUAL(*partitioner(match_none, 6), initial_partition);
    BOOST_REQUIRE_EQUAL(
      *partitioner(match_none, 6), (initial_partition + 1) % 6);
}

BOOST_AUTO_TEST_CASE(test_default_partitioner) {
    auto partitioner{kc::default_partitioner(initial_partition)};
    BOOST_REQUIRE_EQUAL(*partitioner(match_partition, 6), a_partition);
    BOOST_REQUIRE_EQUAL(*partitioner(match_key, 6), murmur2(a_key(), 6));
    BOOST_REQUIRE_EQUAL(*partitioner(match_none, 6), initial_partition);
}
