// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/fundamental.h"
#include "model/ktp.h"

#include <seastar/core/sstring.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/test/unit_test_suite.hpp>

using model::ktp;
using model::ktp_with_hash;
using model::ntp;

static ktp makek(const ss::sstring& t, const int p) {
    return ktp{model::topic(t), model::partition_id(p)};
}

static ktp_with_hash makekh(const ss::sstring& t, const int p) {
    return model::ktp_with_hash{model::topic(t), model::partition_id(p)};
}

static ntp maken(const ss::sstring& t, const int p) {
    return ntp{model::kafka_namespace, model::topic(t), model::partition_id(p)};
}

template<typename T>
static size_t hash_of(const T& t) {
    return std::hash<T>{}(t);
}

static size_t hashk(const ss::sstring& t, const int p) {
    return hash_of(makek(t, p));
}

static size_t hashkh(const ss::sstring& t, const int p) {
    return hash_of(makekh(t, p));
}

static size_t hashn(const ss::sstring& t, const int p) {
    return hash_of(maken(t, p));
}

template<typename L, typename R>
void equals_helper_inner(L makel, R maker) {
    BOOST_CHECK_EQUAL(makel("", 0), maker("", 0));
    BOOST_CHECK_EQUAL(makel("topic", 0), maker("topic", 0));
    BOOST_CHECK_EQUAL(makel("topic", 100), maker("topic", 100));

    BOOST_CHECK_NE(makel("", 0), maker("", 1));
    BOOST_CHECK_NE(makel("", 1), maker("", 0));
    BOOST_CHECK_NE(makel("a", 0), maker("", 0));
    BOOST_CHECK_NE(makel("", 0), maker("a", 0));

    BOOST_CHECK(makel("", 0) != maker("", 1)); // check != operator
}

template<typename L, typename R>
void equals_helper(L make1, R make2) {
    equals_helper_inner(make1, make2);
    equals_helper_inner(make2, make1);
}

BOOST_AUTO_TEST_CASE(test_ktp_equals_default_objects) {
    BOOST_CHECK_EQUAL(ktp(), ktp());
    BOOST_CHECK_EQUAL(ktp(), ktp_with_hash());
    BOOST_CHECK_EQUAL(ktp_with_hash(), ktp_with_hash());
}

BOOST_AUTO_TEST_CASE(test_ktp_equals_internal) { equals_helper(makek, makek); }

BOOST_AUTO_TEST_CASE(test_ktp_equals_cross_with_hash) {
    equals_helper(makek, makekh);
}

BOOST_AUTO_TEST_CASE(test_equals_cross) { equals_helper(makek, maken); }

BOOST_AUTO_TEST_CASE(test_equals_not_kakfa_namespace) {
    BOOST_CHECK_EQUAL(makek("topic", 1), model::ntp("kafka", "topic", 1));

    // check that when ntp has a topic other than kafka it is always unequal
    BOOST_CHECK_NE(makek("topic", 1), model::ntp("not_kafka", "topic", 1));
    BOOST_CHECK_NE(makek("topic", 1), model::ntp("", "topic", 1));
}

BOOST_AUTO_TEST_CASE(test_ktp_hash_internal) { equals_helper(hashk, hashk); }

BOOST_AUTO_TEST_CASE(test_ktp_hash_cross_with_hash) {
    equals_helper(hashk, hashkh);
}

BOOST_AUTO_TEST_CASE(test_ktp_hash_cross) { equals_helper(hashk, hashn); }

BOOST_AUTO_TEST_CASE(test_ktp_hash_of_default_objects) {
    BOOST_CHECK_EQUAL(hash_of(ktp()), hash_of(ktp()));
    BOOST_CHECK_EQUAL(hash_of(ktp()), hash_of(ktp_with_hash()));
    BOOST_CHECK_EQUAL(hash_of(ktp_with_hash()), hash_of(ktp_with_hash()));
}

// Test assignment operations, which are prone to failure for
// cached hash code objects if not written carefully. L and R
// are the types on the left and right sides of the comparison.
template<typename L, typename R>
void test_methods_after_assignment() {
    L ktp0{"topic1", 0};
    R ktp1{ktp0.get_topic(), ktp0.get_partition()}, ktp1_orig = ktp1;
    R ktp_diff_topic{"topic2", ktp0.get_partition()};
    R ktp_diff_part{ktp0.get_topic(), 1};

    // 0 and 1 are the same at first
    BOOST_CHECK_EQUAL(ktp0, ktp1);
    BOOST_CHECK_EQUAL(hash_of(ktp0), hash_of(ktp1));

    BOOST_CHECK_NE(ktp0, ktp_diff_topic);
    BOOST_CHECK_NE(hash_of(ktp0), hash_of(ktp_diff_topic));

    BOOST_CHECK_NE(ktp0, ktp_diff_part);
    BOOST_CHECK_NE(hash_of(ktp0), hash_of(ktp_diff_part));

    // assign 1 with differnet topic
    ktp1 = ktp_diff_topic;

    // now they should be different
    BOOST_CHECK_NE(ktp0, ktp1);
    BOOST_CHECK_NE(hash_of(ktp0), hash_of(ktp1));

    // back to original value
    ktp1 = ktp1_orig;

    // now they should be different
    BOOST_CHECK_EQUAL(ktp0, ktp1);
    BOOST_CHECK_EQUAL(hash_of(ktp0), hash_of(ktp1));

    // assign 1 with differnet topic
    ktp1 = ktp_diff_part;

    // now they should be different
    BOOST_CHECK_NE(ktp0, ktp1);
    BOOST_CHECK_NE(hash_of(ktp0), hash_of(ktp1));

    // repeat above with move-assignment

    // assign 1 with differnet topic
    ktp1 = R{ktp_diff_topic};

    // now they should be different
    BOOST_CHECK_NE(ktp0, ktp1);
    BOOST_CHECK_NE(hash_of(ktp0), hash_of(ktp1));

    // back to original value
    ktp1 = R{ktp1_orig};

    // now they should be different
    BOOST_CHECK_EQUAL(ktp0, ktp1);
    BOOST_CHECK_EQUAL(hash_of(ktp0), hash_of(ktp1));

    // assign 1 with differnet topic
    ktp1 = R{ktp_diff_part};

    // now they should be different
    BOOST_CHECK_NE(ktp0, ktp1);
    BOOST_CHECK_NE(hash_of(ktp0), hash_of(ktp1));
}

BOOST_AUTO_TEST_CASE(test_ktp_after_assignment) {
    test_methods_after_assignment<ktp, ktp>();
}

BOOST_AUTO_TEST_CASE(test_ktp_with_hash_after_assignment) {
    test_methods_after_assignment<ktp_with_hash, ktp_with_hash>();
}

BOOST_AUTO_TEST_CASE(test_ktp_mixed_after_assignment) {
    test_methods_after_assignment<ktp, ktp_with_hash>();
    test_methods_after_assignment<ktp_with_hash, ktp>();
}
