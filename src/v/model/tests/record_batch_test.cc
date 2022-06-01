// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/record.h"
#include "model/record_utils.h"
#include "model/tests/random_batch.h"
#include "model/timestamp.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/data/monomorphic.hpp>
#include <boost/test/data/test_case.hpp>
#include <boost/test/unit_test.hpp>

namespace bdata = boost::unit_test::data;

std::array<model::compression, 5> compressions{
  model::compression::none,
  model::compression::gzip,
  model::compression::snappy,
  model::compression::zstd,
  model::compression::lz4};

BOOST_DATA_TEST_CASE(
  test_with_append_time,
  bdata::make(compressions) ^ bdata::make(model::timestamp_type::append_time),
  c,
  ts_tp) {
    model::record_batch_attributes attrs;
    attrs |= c;
    attrs |= ts_tp;

    BOOST_REQUIRE_EQUAL(attrs.compression(), c);
    BOOST_REQUIRE_EQUAL(attrs.timestamp_type(), ts_tp);
}

BOOST_DATA_TEST_CASE(
  test_with_create_time,
  bdata::make(compressions) ^ bdata::make(model::timestamp_type::create_time),
  c,
  ts_tp) {
    model::record_batch_attributes attrs;
    attrs |= c;
    attrs |= ts_tp;

    BOOST_REQUIRE_EQUAL(attrs.compression(), c);
    BOOST_REQUIRE_EQUAL(attrs.timestamp_type(), ts_tp);
}

SEASTAR_THREAD_TEST_CASE(set_max_timestamp) {
    auto batch = model::test::make_random_batch(model::offset(0), 10, true);

    // nothing changes if set to same values
    auto crc = batch.header().crc;
    auto hdr_crc = batch.header().header_crc;
    batch.set_max_timestamp(
      batch.header().attrs.timestamp_type(), batch.header().max_timestamp);
    BOOST_TEST(crc == batch.header().crc);
    BOOST_TEST(hdr_crc == batch.header().header_crc);

    // ts change updates crcs
    batch.set_max_timestamp(
      model::timestamp_type::append_time,
      model::timestamp(batch.header().max_timestamp() + 1));
    BOOST_TEST(crc != batch.header().crc);
    BOOST_TEST(hdr_crc != batch.header().header_crc);

    // same ts produces orig crcs
    batch.set_max_timestamp(
      model::timestamp_type::create_time,
      model::timestamp(batch.header().max_timestamp() - 1));
    BOOST_TEST(crc == batch.header().crc);
    BOOST_TEST(hdr_crc == batch.header().header_crc);
}
