// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/record.h"
#define BOOST_TEST_MODULE model
#include "model/compression.h"

#include <seastar/core/sstring.hh>

#include <boost/lexical_cast.hpp>
#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_CASE(test_cast_from_string) {
    BOOST_CHECK_EQUAL(
      boost::lexical_cast<model::compression>("none"),
      model::compression::none);
    BOOST_CHECK_EQUAL(
      boost::lexical_cast<model::compression>("uncompressed"),
      model::compression::none);
    BOOST_CHECK_EQUAL(
      boost::lexical_cast<model::compression>("gzip"),
      model::compression::gzip);
    BOOST_CHECK_EQUAL(
      boost::lexical_cast<model::compression>("snappy"),
      model::compression::snappy);
    BOOST_CHECK_EQUAL(
      boost::lexical_cast<model::compression>("lz4"), model::compression::lz4);
    BOOST_CHECK_EQUAL(
      boost::lexical_cast<model::compression>("zstd"),
      model::compression::zstd);
};
BOOST_AUTO_TEST_CASE(removing_compression) {
    model::record_batch_attributes attr(std::numeric_limits<uint16_t>::max());
    attr.remove_compression();
    BOOST_CHECK_EQUAL(attr.compression(), model::compression::none);
    BOOST_CHECK_EQUAL(
      attr.timestamp_type(), model::timestamp_type::append_time);
    BOOST_REQUIRE(attr.is_transactional());

    attr |= model::compression::lz4;
    BOOST_CHECK_EQUAL(attr.compression(), model::compression::lz4);
    attr.remove_compression();
    BOOST_CHECK_EQUAL(attr.compression(), model::compression::none);
    BOOST_CHECK_EQUAL(
      attr.timestamp_type(), model::timestamp_type::append_time);
    BOOST_REQUIRE(attr.is_transactional());

    attr |= model::compression::zstd;
    BOOST_CHECK_EQUAL(attr.compression(), model::compression::zstd);
    attr.remove_compression();
    BOOST_CHECK_EQUAL(attr.compression(), model::compression::none);
    BOOST_CHECK_EQUAL(
      attr.timestamp_type(), model::timestamp_type::append_time);
    BOOST_REQUIRE(attr.is_transactional());

    attr |= model::compression::gzip;
    BOOST_CHECK_EQUAL(attr.compression(), model::compression::gzip);
    attr.remove_compression();
    BOOST_CHECK_EQUAL(attr.compression(), model::compression::none);

    attr |= model::compression::snappy;
    BOOST_CHECK_EQUAL(attr.compression(), model::compression::snappy);
    attr.remove_compression();
    BOOST_CHECK_EQUAL(attr.compression(), model::compression::none);
};

BOOST_AUTO_TEST_CASE(timestamp_type_cast_from_string) {
    BOOST_CHECK_EQUAL(
      boost::lexical_cast<model::timestamp_type>("CreateTime"),
      model::timestamp_type::create_time);
    BOOST_CHECK_EQUAL(
      boost::lexical_cast<model::timestamp_type>("LogAppendTime"),
      model::timestamp_type::append_time);
};

BOOST_AUTO_TEST_CASE(timestamp_type_printing) {
    BOOST_CHECK_EQUAL(
      "CreateTime", fmt::format("{}", model::timestamp_type::create_time));
    BOOST_CHECK_EQUAL(
      "LogAppendTime", fmt::format("{}", model::timestamp_type::append_time));
};
