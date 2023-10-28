/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "http/mime.h"

#include <boost/test/data/monomorphic.hpp>
#include <boost/test/data/test_case.hpp>
#include <boost/test/tools/context.hpp>
#include <boost/test/unit_test.hpp>
#include <fmt/format.h>

#include <memory>
#include <new>
#include <stdexcept>

BOOST_AUTO_TEST_CASE(test_mime_format_simple) {
    auto test_format = [](http::media_type&& m, const std::string_view& f) {
        BOOST_TEST_CHECK(http::format_media_type(m) == f);
    };

    test_format(
      {
        .type = "MultiPart/MiXeD",
        .params = {{"boundary", "batch_357de4f7-6d0b-4e02-8cd2-6361411a9525"}},
      },
      "multipart/mixed; boundary=batch_357de4f7-6d0b-4e02-8cd2-6361411a9525");

    test_format(
      {
        .type = "onlyMajor",
        .params = {{"boundary", "batch_357de4f7-6d0b-4e02-8cd2-6361411a9525"}},
      },
      "onlymajor; boundary=batch_357de4f7-6d0b-4e02-8cd2-6361411a9525");

    test_format(
      {
        .type = "text/plain",
        .params = {{"c", "v1"}, {"b", "v2"}, {"a", "v3"}},
      },
      "text/plain; a=v3; b=v2; c=v1");
}

BOOST_AUTO_TEST_CASE(test_mime_format_exceptions) {
    auto check_exception =
      [](http::media_type&& m, const std::string_view& exp) {
          BOOST_TEST_INFO_SCOPE("expecting e.what() = " << '"' << exp << '"');
          BOOST_CHECK_EXCEPTION(
            http::format_media_type(m),
            std::runtime_error,
            [exp](const std::runtime_error& e) { return e.what() == exp; });
      };

    check_exception(
      {.type = "multip@rt"}, "media type contains non-token characters");
    check_exception(
      {.type = "multip@rt/mixed"}, "media type contains non-token characters");
    check_exception(
      {.type = "multipart/m,xed"}, "media type contains non-token characters");

    check_exception(
      {.type = "multipart/mixed", .params = {{"@", "a"}}},
      "media attributes contain invalid characters");
    check_exception(
      {.type = "multipart/mixed", .params = {{"a", "@"}}},
      "media attribute value encoding is not implemented");
}

BOOST_AUTO_TEST_CASE(test_mime_parse_exceptions) {
    auto check_exception =
      [](const std::string_view& in, const std::string_view& exp) {
          BOOST_TEST_INFO_SCOPE("in = " << '"' << in << '"');

          bool throws = false;

          try {
              http::parse_media_type(in);
          } catch (std::runtime_error& e) {
              throws = true;
              BOOST_TEST_CHECK(e.what() == exp);
          }

          BOOST_TEST_CHECK(throws);
      };

    check_exception("foo", "invalid media type");
    check_exception("; boundary=a", "invalid media type");
    check_exception("/; boundary=a", "invalid media type");
    check_exception("multipart/; boundary=a", "invalid media type");
    check_exception("/mixed; boundary=a", "invalid media type");
    check_exception("multipart/mixed; k", "expecting `=`");
    check_exception(
      "multipart/mixed; boundary=", "empty or invalid parameter value");
    check_exception(
      "multipart/mixed; boundary=@", "empty or invalid parameter value");

    // Found by fuzzing.
    check_exception(".;.", "expecting `=`");
}

BOOST_AUTO_TEST_CASE(test_mime_parse_simple) {
    auto test_parse = [](const std::string_view in, const http::media_type& m) {
        BOOST_TEST_INFO_SCOPE("in = " << '"' << in << '"');
        auto parsed_m = http::parse_media_type(in);
        BOOST_TEST_CHECK(parsed_m.type == m.type);
        BOOST_TEST_CHECK(
          parsed_m.params == m.params, boost::test_tools::per_element());
    };

    test_parse("MultiPart/MiXeD ;", {.type = "multipart/mixed"});
    test_parse("MultiPart/MiXeD ;  ", {.type = "multipart/mixed"});
    test_parse(
      "MultiPart/MiXeD ;    k    =     v      ",
      {.type = "multipart/mixed", .params = {{"k", "v"}}});
    test_parse(
      "MultiPart/MiXeD ;    k    =     v    ;    k2    =    v2  ",
      {.type = "multipart/mixed", .params = {{"k", "v"}, {"k2", "v2"}}});
    test_parse(
      "MultiPart/MiXeD ;;    k    =     v  ;  ;    k2    =    v2  ; ",
      {.type = "multipart/mixed", .params = {{"k", "v"}, {"k2", "v2"}}});
    test_parse(
      "MultiPart/MiXeD;K=V;K2=v2",
      {.type = "multipart/mixed", .params = {{"k", "V"}, {"k2", "v2"}}});
    test_parse(
      "MultiPart/MiXeD;k=v;k2=v2",
      {.type = "multipart/mixed", .params = {{"k", "v"}, {"k2", "v2"}}});

    // Quoted values.
    test_parse(
      R"(MultiPart/MiXeD;k="v";k2="v2")",
      {.type = "multipart/mixed", .params = {{"k", "v"}, {"k2", "v2"}}});
    test_parse(
      R"(MultiPart/MiXeD;k=  "  v  " ;k2= " v2 " )",
      {.type = "multipart/mixed", .params = {{"k", "  v  "}, {"k2", " v2 "}}});
}

// Specialization needed for test assertions of the following format:
// clang-format off
// BOOST_TEST_CHECK(parsed_m.params == m.params,
//    boost::test_tools::per_element());
// clang-format on
template<>
std::ostream& boost::test_tools::tt_detail::impl::boost_test_print_type<
  std::pair<const std::string, std::string>>(
  std::ostream& ostr, std::pair<const std::string, std::string> const& t) {
    ostr << fmt::format("({}, {})", t.first, t.second);
    return ostr;
}
