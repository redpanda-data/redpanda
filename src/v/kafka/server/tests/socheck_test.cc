// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "socheck.h"

#include <boost/test/unit_test.hpp>

using namespace socheck;

socheck_data kafka_socheck();
socheck_data model_socheck();

BOOST_AUTO_TEST_CASE(socheck_global_objects) {
    auto k = kafka_socheck();
    auto m = model_socheck();

    BOOST_CHECK_EQUAL(k.inline_global, m.inline_global);
    BOOST_CHECK_EQUAL(k.static_var_inline_fn_ptr, m.static_var_inline_fn_ptr);
    BOOST_CHECK_EQUAL(
      k.static_var_inline_fn_value, m.static_var_inline_fn_value);

    BOOST_CHECK_NE(k.static_var_static_fn_value, m.static_var_static_fn_value);
    BOOST_CHECK_NE(k.anon_global, m.anon_global);
}

BOOST_AUTO_TEST_CASE(socheck_absl_hash) {
    auto k = kafka_socheck();
    auto m = model_socheck();

    BOOST_CHECK_EQUAL(k.absl_hash, m.absl_hash);
    BOOST_CHECK_EQUAL(k.absl_hash, absl::HashOf(hash_input));
}
