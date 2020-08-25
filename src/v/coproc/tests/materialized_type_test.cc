#include "coproc/types.h"
#include "model/fundamental.h"

#include <seastar/core/sstring.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>
#include <fmt/core.h>

#include <string_view>
#include <vector>

ss::sstring as_dest(const std::string_view& sv) {
    return fmt::format("${}$", sv);
}
ss::sstring as_dest(ss::sstring&& str) {
    return as_dest(std::string_view(str));
}

BOOST_AUTO_TEST_CASE(test_valid_materialized_topic) {
    // Dash, underscore, period and any alphanumeric chars are the only
    // types of characters allowed.
    std::vector<std::tuple<ss::sstring, ss::sstring>> test_data{
      {{"abc"}, {"$123$"}},
      {{"123sd-34"}, {"$df83d$"}},
      {{"baz"}, {"$safd.asfd$"}},
      {{"--3---"}, {"$-3-_-$"}}};
    for (const auto& e : test_data) {
        const auto& [src, dest] = e;
        model::topic t(src + "." + dest);
        auto mat_topic = coproc::make_materialized_topic(t);
        BOOST_REQUIRE(mat_topic.has_value());
        BOOST_REQUIRE_EQUAL(mat_topic->src, src);
        BOOST_REQUIRE_EQUAL(as_dest(mat_topic->dest), dest);
    }
}

BOOST_AUTO_TEST_CASE(test_invalid_materialized_topic) {
    const static ss::sstring too_large_str(
      "abcdefghij_abcdefghij_abcdefghij_abcdefghij_abcdefghij_abcdefghij_"
      "abcdefghij_abcdefghij_abcdefghij_abcdefghij_abcdefghij_abcdefghij_"
      "abcdefghij_abcdefghij_abcdefghij_abcdefghij_abcdefghij_abcdefghij_"
      "abcdefghij_abcdefghij_abcdefghij_abcdefghij_abcdefghij_abcdefghij_"
      "abcdefghij_abcdefghij_abcdefghij_abcdefghij_abcdefghij_abcdefghij_"
      "abcdefghij_abcdefghij_");
    // Possible errors in parsing include:
    // - Empty source of dest topic
    // - Missing surrounding dollar chars on dest topic
    // - Including erraneous non alphanumeric chars
    // - Too long of a source or destination topic
    std::vector<std::tuple<ss::sstring, ss::sstring>> test_data{
      {{""}, {"foo"}},
      {{"asdf"}, {""}},
      {{"bar"}, {"1234"}},
      {{"bar"}, {"$1234"}},
      {{"bar"}, {"1234$"}},
      {{"bar"}, {"$%1234$"}},
      {{"baz"}, {"$$"}},
      {{"OK"}, {too_large_str}},
      {{too_large_str}, {"$OK$"}},
      {{"OK"}, as_dest(too_large_str)}};

    for (const auto& e : test_data) {
        const auto& [src, dest] = e;
        model::topic t(src + "." + dest);
        auto mat_topic = coproc::make_materialized_topic(t);
        BOOST_REQUIRE(!mat_topic.has_value());
    }
}
