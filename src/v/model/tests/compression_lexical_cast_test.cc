#define BOOST_TEST_MODULE model
#include "boost/lexical_cast.hpp"
#include "model/compression.h"

#include <seastar/core/sstring.hh>

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