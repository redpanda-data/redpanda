/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#define BOOST_TEST_MODULE pandaproxy

#include "kafka/protocol/errors.h"

#include "pandaproxy/error.h"
#include "pandaproxy/json/error.h"
#include "pandaproxy/parsing/error.h"

#include <boost/test/data/monomorphic.hpp>
#include <boost/test/data/test_case.hpp>
#include <boost/test/unit_test.hpp>

#include <ostream>
#include <system_error>
#include <utility>

namespace bdata = boost::unit_test::data;
namespace pp = pandaproxy;
using rec = pp::reply_error_code;
using kec = kafka::error_code;
using pec = pandaproxy::parse::error_code;
using jec = pandaproxy::json::error_code;

struct ec_cond {
    std::error_code ec;
    rec cond;
    friend std::ostream& operator<<(std::ostream& os, const ec_cond& p) {
        return os << p.ec.message() << ", "
                  << make_error_condition(p.cond).message();
    }
};

static const std::array<ec_cond, 5> conversion_data{
  {{kec::offset_out_of_range, rec::kafka_bad_request},
   {kec::unknown_server_error, rec::kafka_error},
   {kec::unknown_topic_or_partition, rec::partition_not_found},
   {pec::not_acceptable, rec::not_acceptable},
   {jec::invalid_json, rec::unprocessable_entity}}};

BOOST_DATA_TEST_CASE(
  test_error_condition_conversion, bdata::make(conversion_data), sample) {
    auto ec = sample.ec;
    auto cond = pp::make_error_condition(ec);
    BOOST_REQUIRE(sample.cond == cond);
}

BOOST_DATA_TEST_CASE(
  test_error_condition_equivalence, bdata::make(conversion_data), sample) {
    BOOST_REQUIRE(sample.cond == sample.ec);
    BOOST_REQUIRE(sample.ec == sample.cond);
}
