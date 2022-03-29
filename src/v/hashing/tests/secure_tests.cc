// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#define BOOST_TEST_MODULE secure_hash
#include "hashing/secure.h"

#include <boost/test/unit_test.hpp>

template<typename hmac>
static auto hmac_test_vector() {
    hmac mac("redpanda");
    mac.update("is the cutest panda");
    return mac.reset();
}

template<typename hash>
static auto hash_test_vector() {
    hash h;
    h.update("redpanda is the cutest panda");
    return h.reset();
}

// $> echo -n "is the cutest panda" | sha256hmac -K redpanda
// 89fd20823dc7f76e21feeb350f5f08f7fc097f82709cb4212cc820353c589e12  -
BOOST_AUTO_TEST_CASE(hmac) {
    BOOST_REQUIRE_EQUAL(
      to_hex(hmac_test_vector<hmac_sha256>()),
      "89fd20823dc7f76e21feeb350f5f08f7fc097f82709cb4212cc820353c589e12");

    BOOST_REQUIRE_EQUAL(
      to_hex(hmac_test_vector<hmac_sha512>()),
      "d7cd2df2f8f48ba03fc9fb9023c2c576c5f6088d8d09533f8250f467094331b4c5eed3f1"
      "fe733ddddd29149e3cda1a95c984334c318c61f1aef79d1622eceda0");
}

// $> echo -n "redpanda is the cutest panda" | sha256sum
// 77356443c4344b7edb08b5ef93a32e50a4e191d5a1e896ac8e42d5cb1f9f1c91  -
BOOST_AUTO_TEST_CASE(hash) {
    BOOST_REQUIRE_EQUAL(
      to_hex(hash_test_vector<hash_sha256>()),
      "77356443c4344b7edb08b5ef93a32e50a4e191d5a1e896ac8e42d5cb1f9f1c91");

    BOOST_REQUIRE_EQUAL(
      to_hex(hash_test_vector<hash_sha512>()),
      "e8074d0234262e048639041e6fef05a66c5f540bad3e85be3ef784c709d6e377ff206026"
      "4b9821c919f3e2d92a36e716354b8f8a1c02640f0d71fdf4cf85a54a");
}
