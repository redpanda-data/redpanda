#define BOOST_TEST_MODULE model
#include "model/record.h"

#include <boost/test/data/monomorphic.hpp>
#include <boost/test/data/test_case.hpp>
#include <boost/test/unit_test.hpp>

namespace bdata = boost::unit_test::data;

std::array<model::compression, 5> compressions{model::compression::none,
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