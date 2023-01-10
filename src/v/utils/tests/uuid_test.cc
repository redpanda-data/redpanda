// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "serde/serde.h"
#include "utils/named_type.h"
#include "utils/uuid.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

using test_uuid = named_type<uuid_t, struct test_uuid_tag>;

SEASTAR_THREAD_TEST_CASE(test_uuid_create) {
    auto uuid1 = uuid_t::create();
    auto uuid2 = uuid_t::create();
    BOOST_REQUIRE_NE(uuid1, uuid2);
}

SEASTAR_THREAD_TEST_CASE(test_named_uuid_type) {
    auto uuid1 = test_uuid(uuid_t::create());
    auto uuid2 = test_uuid(uuid_t::create());
    BOOST_REQUIRE_NE(uuid1, uuid2);
}

SEASTAR_THREAD_TEST_CASE(test_to_from_vec) {
    auto uuid1 = test_uuid(uuid_t::create());
    auto uuid1_vec = uuid1().to_vector();
    auto uuid2 = test_uuid(uuid1_vec);
    BOOST_REQUIRE_EQUAL(uuid1, uuid2);
}

SEASTAR_THREAD_TEST_CASE(uuid_test) {
    auto b = iobuf();
    uuid_t u = uuid_t::create();
    serde::write(b, u);

    auto parser = iobuf_parser{std::move(b)};
    const auto r = serde::read<uuid_t>(parser);
    BOOST_REQUIRE_EQUAL(u, r);
}

struct uuid_struct
  : serde::envelope<uuid_struct, serde::version<0>, serde::compat_version<0>> {
    uuid_t single;
    std::optional<uuid_t> opt1;
    std::optional<uuid_t> opt2;
    std::vector<uuid_t> vec;
    std::vector<std::optional<uuid_t>> opt_vec;
};

template<typename map_t>
void verify_uuid_map() {
    map_t m = {
      {uuid_t::create(), 0},
      {uuid_t::create(), 1},
      {uuid_t::create(), 2},
    };
    auto b = iobuf();
    serde::write(b, m);
    auto parser = iobuf_parser{std::move(b)};
    const auto r = serde::read<map_t>(parser);
    BOOST_CHECK_EQUAL(m.size(), r.size());
    for (const auto& [k, v] : m) {
        const auto r_it = r.find(k);
        BOOST_CHECK(m.end() != r_it);
        BOOST_CHECK_EQUAL(v, r_it->second);
    }
}

namespace std {
template<>
struct hash<uuid_t> {
    size_t operator()(const uuid_t& u) const {
        return boost::hash<uuid_t::underlying_t>()(u.uuid());
    }
};
} // namespace std

SEASTAR_THREAD_TEST_CASE(complex_uuid_types_test) {
    uuid_struct us;
    us.single = uuid_t::create();
    us.opt1 = std::make_optional<uuid_t>(uuid_t::create());
    us.opt2 = std::nullopt;
    us.vec = {
      uuid_t::create(),
      uuid_t::create(),
      uuid_t::create(),
    };
    us.opt_vec = {
      std::make_optional<uuid_t>(uuid_t::create()),
      std::nullopt,
      std::make_optional<uuid_t>(uuid_t::create()),
      std::nullopt,
    };
    auto b = iobuf();
    serde::write(b, us);
    auto parser = iobuf_parser{std::move(b)};
    const auto r = serde::read<uuid_struct>(parser);
    BOOST_CHECK_EQUAL(us.single, r.single);
    BOOST_CHECK(us.opt1 == r.opt1);
    BOOST_CHECK(us.opt2 == r.opt2);
    BOOST_CHECK_EQUAL(us.vec, r.vec);
    BOOST_CHECK_EQUAL(us.opt_vec.size(), r.opt_vec.size());
    for (int i = 0; i < us.opt_vec.size(); ++i) {
        BOOST_CHECK(us.opt_vec[i] == r.opt_vec[i]);
    }

    // Map types.
    verify_uuid_map<std::unordered_map<uuid_t, int>>();
    verify_uuid_map<absl::flat_hash_map<uuid_t, int>>();
}
