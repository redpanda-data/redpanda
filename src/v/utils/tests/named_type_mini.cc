// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#define BOOST_TEST_MODULE nt_unit

#include "config/convert.h"
#include "json/json.h"
#include "reflection/type_traits.h"
#include "utils/named_base.h"

#include <boost/test/unit_test_suite.hpp>

#include <array>
#include <concepts>
#include <functional>
#include <iostream>
#include <unordered_set>

// #include <seastar/core/sstring.hh>

#include <boost/test/unit_test.hpp>

struct nothing {};

struct int_derived : named_base<int32_t, int_derived> {
    using base::base;
};

struct int_derived_additional_member
  : named_base<int32_t, int_derived_additional_member> {
    using base::base;
    int more;
};

using string_alias = named_type<ss::sstring, struct string_alias_tag>;
struct string_derived
  : named_base<ss::sstring, struct string_derived_unit_test> {};

using int_alias = named_type<int32_t, struct int_alias_test_module>;

template<typename T>
concept SubclassConcept = detail::NamedTypeTrivialSubclass<T>;

BOOST_AUTO_TEST_CASE(named_type_concept) {
    static_assert(SubclassConcept<int> == false);
    static_assert(SubclassConcept<int_derived> == true);
    // static_assert(SubclassConcept<string_derived> == true);
    static_assert(SubclassConcept<int_derived_additional_member> == false);
}

BOOST_AUTO_TEST_CASE(named_type_ctor) {
    int_derived t0{0}, t1{1};
    int_derived r = t0 + t1;
    t0 = t1;
    t0 = t0 + t0;
    // int_derived alias1{0};
    t0 += 1;
    t0 = t0 + 1;

    t1 = t0++;
    t1 = ++t0;
}

BOOST_AUTO_TEST_CASE(named_base_aggr_init) { std::array a1 = {int_derived{1}}; }

BOOST_AUTO_TEST_CASE(named_base_type_traits) {
    static_assert(reflection::is_named_type<int_alias>::value);
    static_assert(reflection::is_named_type_v<int_alias>);
    static_assert(reflection::is_named_type<int_derived>::value);
    static_assert(reflection::is_named_type_v<int_derived>);

    static_assert(!reflection::is_named_type_v<nothing>);
}

BOOST_AUTO_TEST_CASE(named_base_operator_equals) {
    int_derived left, right;
    if (left == right) {
    }
}

BOOST_AUTO_TEST_CASE(named_base_yaml_convert) { YAML::convert<int_derived> c; }

BOOST_AUTO_TEST_CASE(named_base_rjson) {
    ::json::StringBuffer str_buf;
    ::json::Writer<::json::StringBuffer> wrt(str_buf);

    ::json::rjson_serialize(wrt, std::optional<int_alias>{});
    ::json::rjson_serialize(wrt, std::optional<int_derived>{});
    ::json::rjson_serialize(wrt, string_alias{});
    ::json::rjson_serialize(wrt, string_derived{});
}

BOOST_AUTO_TEST_CASE(named_type_basic) {
    // constexpr auto x = int_alias(5);
    std::unordered_set<int_alias> set0;
    std::unordered_set<int_derived> set1;
}
