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

#include "pandaproxy/parsing/httpd.h"

#include "pandaproxy/json/types.h"

#include <boost/test/data/monomorphic.hpp>
#include <boost/test/data/test_case.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/tuple/tuple.hpp>
#include <boost/tuple/tuple_io.hpp>
#include <boost/utility/string_view.hpp>

namespace bdata = boost::unit_test::data;
namespace pp = pandaproxy;
namespace ppj = pp::json;

namespace pandaproxy::json {
std::ostream& operator<<(std::ostream& os, serialization_format fmt) {
    return os << name(fmt);
}
} // namespace pandaproxy::json

using ppjfmt = ppj::serialization_format;

static const std::vector<
  boost::
    tuple<std::string_view, std::vector<ppjfmt>, ppj::serialization_format>>
  success_samples{
    // Simple cases
    {"", {ppjfmt::none}, ppjfmt::none},
    {"*/*", {ppjfmt::none}, ppjfmt::none},
    {name(ppjfmt::json_v2), {ppjfmt::json_v2}, ppjfmt::json_v2},
    {name(ppjfmt::binary_v2), {ppjfmt::binary_v2}, ppjfmt::binary_v2},
    // Prefer first entry
    {name(ppjfmt::json_v2), {ppjfmt::json_v2, ppjfmt::none}, ppjfmt::json_v2},
    {"", {ppjfmt::json_v2, ppjfmt::none}, ppjfmt::json_v2},
    // Support two types
    {name(ppjfmt::json_v2),
     {ppjfmt::json_v2, ppjfmt::binary_v2},
     ppjfmt::json_v2},
    {name(ppjfmt::binary_v2),
     {ppjfmt::json_v2, ppjfmt::binary_v2},
     ppjfmt::binary_v2},
    // Unsupported
    {name(ppjfmt::json_v2),
     {ppjfmt::binary_v2, ppjfmt::none},
     ppjfmt::unsupported},
  };

BOOST_DATA_TEST_CASE(
  parse_serialization_format_success, bdata::make(success_samples), sample) {
    BOOST_REQUIRE_EQUAL(
      pp::parse::detail::parse_serialization_format(
        boost::get<0>(sample), boost::get<1>(sample)),
      boost::get<2>(sample));
}
