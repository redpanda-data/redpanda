// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/sharded_store.h"

#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/exceptions.h"
#include "pandaproxy/schema_registry/protobuf.h"
#include "pandaproxy/schema_registry/test/compatibility_protobuf.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <boost/test/unit_test.hpp>

namespace pp = pandaproxy;
namespace pps = pp::schema_registry;

// default_context, no fallback
// Resolution: default_context config → hardcoded default
// (global_context is never consulted with no_fallback)
SEASTAR_THREAD_TEST_CASE(
  test_sharded_store_default_context_config_no_fallback) {
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    auto no_fallback = pps::default_to_global::no;
    pps::seq_marker dummy_marker;

    BOOST_REQUIRE(
      store.get_compatibility(pps::default_context, no_fallback).get()
      == pps::default_top_level_compat);

    auto expected = pps::compatibility_level::full;
    BOOST_REQUIRE(
      store.set_compatibility(dummy_marker, pps::default_context, expected)
        .get());

    BOOST_REQUIRE(
      store.get_compatibility(pps::default_context, no_fallback).get()
      == expected);

    BOOST_REQUIRE(store.clear_compatibility(pps::default_context).get());

    BOOST_REQUIRE(
      store.get_compatibility(pps::default_context, no_fallback).get()
      == pps::default_top_level_compat);
}

// default_context, fallback enabled
// Resolution: default_context config → global_context config → hardcoded
// default
SEASTAR_THREAD_TEST_CASE(test_sharded_store_default_context_config_fallback) {
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    auto fallback = pps::default_to_global::yes;
    pps::seq_marker dummy_marker;

    BOOST_REQUIRE(
      store.get_compatibility(pps::default_context, fallback).get()
      == pps::default_top_level_compat);

    auto expected1 = pps::compatibility_level::full;
    BOOST_REQUIRE(
      store
        .set_compatibility(
          dummy_marker, pandaproxy::schema_registry::global_context, expected1)
        .get());

    BOOST_REQUIRE(
      store.get_compatibility(pps::default_context, fallback).get()
      == expected1);

    auto expected2 = pps::compatibility_level::none;
    BOOST_REQUIRE(
      store
        .set_compatibility(
          dummy_marker, pandaproxy::schema_registry::default_context, expected2)
        .get());

    BOOST_REQUIRE(
      store.get_compatibility(pps::default_context, fallback).get()
      == expected2);

    BOOST_REQUIRE(store.clear_compatibility(pps::default_context).get());

    BOOST_REQUIRE(
      store.get_compatibility(pps::default_context, fallback).get()
      == expected1);

    BOOST_REQUIRE(store.clear_compatibility(pps::global_context).get());

    BOOST_REQUIRE(
      store.get_compatibility(pps::default_context, fallback).get()
      == pps::default_top_level_compat);
}

// subject in default_context, no fallback
// Resolution: subject config → error (no context or global fallback)
SEASTAR_THREAD_TEST_CASE(
  test_sharded_store_subject_default_context_config_no_fallback) {
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    auto no_fallback = pps::default_to_global::no;
    auto ctx_sub = pps::context_subject{
      pps::default_context, pps::subject{"sub"}};
    pps::seq_marker dummy_marker;

    BOOST_REQUIRE_EXCEPTION(
      store.get_compatibility(ctx_sub, no_fallback).get(),
      pps::exception,
      [](const pps::exception& e) {
          return e.code() == pps::error_code::compatibility_not_found;
      });

    auto expected = pps::compatibility_level::full;
    BOOST_REQUIRE(
      store.set_compatibility(dummy_marker, ctx_sub, expected).get());
    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, no_fallback).get() == expected);
}

// subject in default_context, fallback enabled
// Resolution: subject config → default_context config → global_context config →
// hardcoded default
SEASTAR_THREAD_TEST_CASE(
  test_sharded_store_subject_default_context_config_fallback) {
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    auto fallback = pps::default_to_global::yes;
    auto subject = pps::subject{"sub"};
    auto ctx = pps::default_context;
    auto ctx_sub = pps::context_subject{ctx, subject};
    pps::seq_marker dummy_marker;

    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, fallback).get()
      == pps::default_top_level_compat);

    auto expected1 = pps::compatibility_level::full;
    BOOST_REQUIRE(
      store.set_compatibility(dummy_marker, pps::global_context, expected1)
        .get());
    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, fallback).get() == expected1);

    auto expected2 = pps::compatibility_level::forward;
    BOOST_REQUIRE(
      store.set_compatibility(dummy_marker, pps::default_context, expected2)
        .get());
    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, fallback).get() == expected2);

    auto expected3 = pps::compatibility_level::none;
    BOOST_REQUIRE(
      store.set_compatibility(dummy_marker, ctx_sub, expected3).get());
    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, fallback).get() == expected3);

    BOOST_REQUIRE(store.clear_compatibility(dummy_marker, ctx_sub).get());
    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, fallback).get() == expected2);

    BOOST_REQUIRE(store.clear_compatibility(pps::default_context).get());
    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, fallback).get() == expected1);

    BOOST_REQUIRE(store.clear_compatibility(pps::global_context).get());
    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, fallback).get()
      == pps::default_top_level_compat);
}

// non-default context, no fallback
// Resolution: context config → error (no hardcoded default for non-default
// contexts)
SEASTAR_THREAD_TEST_CASE(
  test_sharded_store_nondefault_context_config_no_fallback) {
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    auto no_fallback = pps::default_to_global::no;
    auto ctx = pps::context{".ctx"};
    pps::seq_marker dummy_marker;

    BOOST_REQUIRE_EXCEPTION(
      store.get_compatibility(ctx, no_fallback).get(),
      pps::exception,
      [](const pps::exception& e) {
          return e.code() == pps::error_code::compatibility_not_found;
      });

    auto expected = pps::compatibility_level::full;
    BOOST_REQUIRE(store.set_compatibility(dummy_marker, ctx, expected).get());

    BOOST_REQUIRE(store.get_compatibility(ctx, no_fallback).get() == expected);

    BOOST_REQUIRE(store.clear_compatibility(ctx).get());

    BOOST_REQUIRE_EXCEPTION(
      store.get_compatibility(ctx, no_fallback).get(),
      pps::exception,
      [](const pps::exception& e) {
          return e.code() == pps::error_code::compatibility_not_found;
      });
}

// non-default context, fallback enabled
// Resolution: context config → global_context config → hardcoded default
SEASTAR_THREAD_TEST_CASE(
  test_sharded_store_nondefault_context_config_fallback) {
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    auto fallback = pps::default_to_global::yes;
    auto ctx = pps::context{".ctx"};
    pps::seq_marker dummy_marker;

    BOOST_REQUIRE(
      store.get_compatibility(ctx, fallback).get()
      == pps::default_top_level_compat);

    auto expected1 = pps::compatibility_level::full;
    BOOST_REQUIRE(
      store.set_compatibility(dummy_marker, pps::global_context, expected1)
        .get());
    BOOST_REQUIRE(store.get_compatibility(ctx, fallback).get() == expected1);

    auto expected2 = pps::compatibility_level::forward;
    BOOST_REQUIRE(store.set_compatibility(dummy_marker, ctx, expected2).get());
    BOOST_REQUIRE(store.get_compatibility(ctx, fallback).get() == expected2);

    BOOST_REQUIRE(store.clear_compatibility(ctx).get());
    BOOST_REQUIRE(store.get_compatibility(ctx, fallback).get() == expected1);

    BOOST_REQUIRE(store.clear_compatibility(pps::global_context).get());
    BOOST_REQUIRE(
      store.get_compatibility(ctx, fallback).get()
      == pps::default_top_level_compat);
}

// subject in non-default context, no fallback
// Resolution: subject config → error (no context or global fallback)
SEASTAR_THREAD_TEST_CASE(
  test_sharded_store_subject_nondefault_context_config_no_fallback) {
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    auto no_fallback = pps::default_to_global::no;
    auto ctx = pps::context{".ctx"};
    auto ctx_sub = pps::context_subject{ctx, pps::subject{"sub"}};
    pps::seq_marker dummy_marker;

    BOOST_REQUIRE_EXCEPTION(
      store.get_compatibility(ctx_sub, no_fallback).get(),
      pps::exception,
      [](const pps::exception& e) {
          return e.code() == pps::error_code::compatibility_not_found;
      });

    auto expected = pps::compatibility_level::full;
    BOOST_REQUIRE(
      store.set_compatibility(dummy_marker, ctx_sub, expected).get());

    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, no_fallback).get() == expected);

    BOOST_REQUIRE(store.clear_compatibility(dummy_marker, ctx_sub).get());

    BOOST_REQUIRE_EXCEPTION(
      store.get_compatibility(ctx_sub, no_fallback).get(),
      pps::exception,
      [](const pps::exception& e) {
          return e.code() == pps::error_code::compatibility_not_found;
      });
}

// subject in non-default context, fallback enabled
// Resolution: subject config → context config → global_context config →
// hardcoded default
SEASTAR_THREAD_TEST_CASE(
  test_sharded_store_subject_nondefault_context_config_fallback) {
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    auto fallback = pps::default_to_global::yes;
    auto ctx = pps::context{".ctx"};
    auto ctx_sub = pps::context_subject{ctx, pps::subject{"subject"}};
    pps::seq_marker dummy_marker;

    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, fallback).get()
      == pps::default_top_level_compat);

    auto expected1 = pps::compatibility_level::full;
    BOOST_REQUIRE(
      store.set_compatibility(dummy_marker, pps::global_context, expected1)
        .get());
    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, fallback).get() == expected1);

    auto expected2 = pps::compatibility_level::forward;
    BOOST_REQUIRE(store.set_compatibility(dummy_marker, ctx, expected2).get());
    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, fallback).get() == expected2);

    auto expected3 = pps::compatibility_level::none;
    BOOST_REQUIRE(
      store.set_compatibility(dummy_marker, ctx_sub, expected3).get());
    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, fallback).get() == expected3);

    BOOST_REQUIRE(store.clear_compatibility(dummy_marker, ctx_sub).get());
    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, fallback).get() == expected2);

    BOOST_REQUIRE(store.clear_compatibility(ctx).get());
    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, fallback).get() == expected1);

    BOOST_REQUIRE(store.clear_compatibility(pps::global_context).get());
    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, fallback).get()
      == pps::default_top_level_compat);
}

// global_context, no fallback
// Resolution: global_context config → hardcoded default
// (no_fallback is a no-op; global_context is already the top of the chain)
SEASTAR_THREAD_TEST_CASE(test_sharded_store_global_context_config_no_fallback) {
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    auto no_fallback = pps::default_to_global::no;
    auto ctx = pandaproxy::schema_registry::global_context;
    pps::seq_marker dummy_marker;

    BOOST_REQUIRE(
      store.get_compatibility(ctx, no_fallback).get()
      == pps::default_top_level_compat);

    auto expected = pps::compatibility_level::full;
    BOOST_REQUIRE(store.set_compatibility(dummy_marker, ctx, expected).get());

    BOOST_REQUIRE(store.get_compatibility(ctx, no_fallback).get() == expected);

    BOOST_REQUIRE(store.clear_compatibility(ctx).get());

    BOOST_REQUIRE(
      store.get_compatibility(ctx, no_fallback).get()
      == pps::default_top_level_compat);
}

// global_context, fallback enabled
// Resolution: global_context config → hardcoded default
// (fallback flag is a no-op; global_context is already the top of the chain)
SEASTAR_THREAD_TEST_CASE(test_sharded_store_global_context_config_fallback) {
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    auto fallback = pps::default_to_global::yes;
    auto ctx = pps::global_context;
    pps::seq_marker dummy_marker;

    BOOST_REQUIRE(
      store.get_compatibility(ctx, fallback).get()
      == pps::default_top_level_compat);

    auto expected = pps::compatibility_level::full;
    BOOST_REQUIRE(
      store.set_compatibility(dummy_marker, pps::global_context, expected)
        .get());
    BOOST_REQUIRE(store.get_compatibility(ctx, fallback).get() == expected);

    BOOST_REQUIRE(store.clear_compatibility(pps::global_context).get());
    BOOST_REQUIRE(
      store.get_compatibility(ctx, fallback).get()
      == pps::default_top_level_compat);
}

// subject in global_context, no fallback
// Resolution: subject config → hardcoded default
// (global_context subjects fallback to the hardcoded default even with
// no_fallback)
SEASTAR_THREAD_TEST_CASE(
  test_sharded_store_subject_global_context_no_fallback) {
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    auto no_fallback = pps::default_to_global::no;
    auto subject = pps::subject{"sub"};
    auto ctx = pps::global_context;
    auto ctx_sub = pps::context_subject{ctx, subject};
    pps::seq_marker dummy_marker;

    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, no_fallback).get()
      == pps::default_top_level_compat);

    auto expected = pps::compatibility_level::full;
    BOOST_REQUIRE(
      store.set_compatibility(dummy_marker, ctx_sub, expected).get());

    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, no_fallback).get() == expected);

    BOOST_REQUIRE(store.clear_compatibility(dummy_marker, ctx_sub).get());

    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, no_fallback).get()
      == pps::default_top_level_compat);
}

// subject in global_context, fallback enabled
// Resolution: subject config → global_context config → hardcoded default
SEASTAR_THREAD_TEST_CASE(test_sharded_store_subject_global_context_fallback) {
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    auto fallback = pps::default_to_global::yes;
    auto ctx = pps::global_context;
    auto ctx_sub = pps::context_subject{ctx, pps::subject{"subject"}};
    pps::seq_marker dummy_marker;

    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, fallback).get()
      == pps::default_top_level_compat);

    auto expected1 = pps::compatibility_level::full;
    BOOST_REQUIRE(
      store.set_compatibility(dummy_marker, pps::global_context, expected1)
        .get());
    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, fallback).get() == expected1);

    auto expected2 = pps::compatibility_level::forward;
    BOOST_REQUIRE(
      store.set_compatibility(dummy_marker, ctx_sub, expected2).get());
    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, fallback).get() == expected2);

    BOOST_REQUIRE(store.clear_compatibility(dummy_marker, ctx_sub).get());
    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, fallback).get() == expected1);

    BOOST_REQUIRE(store.clear_compatibility(pps::global_context).get());
    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, fallback).get()
      == pps::default_top_level_compat);
}

// default_context mode, no fallback
// Resolution: default_context mode → hardcoded default
SEASTAR_THREAD_TEST_CASE(test_sharded_store_default_context_mode_no_fallback) {
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    auto no_fallback = pps::default_to_global::no;
    pps::seq_marker dummy_marker;

    BOOST_REQUIRE(
      store.get_mode(pps::default_context, no_fallback).get()
      == pps::default_top_level_mode);

    auto expected = pps::mode::read_only;
    BOOST_REQUIRE(
      store
        .set_mode(dummy_marker, pps::default_context, expected, pps::force::no)
        .get());

    BOOST_REQUIRE(
      store.get_mode(pps::default_context, no_fallback).get() == expected);

    BOOST_REQUIRE(store.clear_mode(pps::default_context, pps::force::no).get());

    BOOST_REQUIRE(
      store.get_mode(pps::default_context, no_fallback).get()
      == pps::default_top_level_mode);
}

// default_context mode, fallback enabled
// Resolution: default_context mode → global_context mode → hardcoded
// default
SEASTAR_THREAD_TEST_CASE(test_sharded_store_default_context_mode_fallback) {
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    auto fallback = pps::default_to_global::yes;
    pps::seq_marker dummy_marker;

    BOOST_REQUIRE(
      store.get_mode(pps::default_context, fallback).get()
      == pps::default_top_level_mode);

    auto expected1 = pps::mode::read_only;
    BOOST_REQUIRE(
      store
        .set_mode(dummy_marker, pps::global_context, expected1, pps::force::no)
        .get());

    BOOST_REQUIRE(
      store.get_mode(pps::default_context, fallback).get() == expected1);

    auto expected2 = pps::mode::import;
    BOOST_REQUIRE(
      store
        .set_mode(dummy_marker, pps::default_context, expected2, pps::force::no)
        .get());

    BOOST_REQUIRE(
      store.get_mode(pps::default_context, fallback).get() == expected2);

    BOOST_REQUIRE(store.clear_mode(pps::default_context, pps::force::no).get());

    BOOST_REQUIRE(
      store.get_mode(pps::default_context, fallback).get() == expected1);

    BOOST_REQUIRE(store.clear_mode(pps::global_context, pps::force::no).get());

    BOOST_REQUIRE(
      store.get_mode(pps::default_context, fallback).get()
      == pps::default_top_level_mode);
}

// non-default context mode, no fallback
// Resolution: context mode → error
SEASTAR_THREAD_TEST_CASE(
  test_sharded_store_nondefault_context_mode_no_fallback) {
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    auto no_fallback = pps::default_to_global::no;
    auto ctx = pps::context{".ctx"};
    pps::seq_marker dummy_marker;

    BOOST_REQUIRE_EXCEPTION(
      store.get_mode(ctx, no_fallback).get(),
      pps::exception,
      [](const pps::exception& e) {
          return e.code() == pps::error_code::mode_not_found;
      });

    auto expected = pps::mode::read_only;
    BOOST_REQUIRE(
      store.set_mode(dummy_marker, ctx, expected, pps::force::no).get());

    BOOST_REQUIRE(store.get_mode(ctx, no_fallback).get() == expected);

    BOOST_REQUIRE(store.clear_mode(ctx, pps::force::no).get());

    BOOST_REQUIRE_EXCEPTION(
      store.get_mode(ctx, no_fallback).get(),
      pps::exception,
      [](const pps::exception& e) {
          return e.code() == pps::error_code::mode_not_found;
      });
}

// non-default context mode, fallback enabled
// Resolution: context mode → global_context mode → hardcoded default
SEASTAR_THREAD_TEST_CASE(test_sharded_store_nondefault_context_mode_fallback) {
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    auto fallback = pps::default_to_global::yes;
    auto ctx = pps::context{".ctx"};
    pps::seq_marker dummy_marker;

    BOOST_REQUIRE(
      store.get_mode(ctx, fallback).get() == pps::default_top_level_mode);

    auto expected1 = pps::mode::read_only;
    BOOST_REQUIRE(
      store
        .set_mode(dummy_marker, pps::global_context, expected1, pps::force::no)
        .get());
    BOOST_REQUIRE(store.get_mode(ctx, fallback).get() == expected1);

    auto expected2 = pps::mode::import;
    BOOST_REQUIRE(
      store.set_mode(dummy_marker, ctx, expected2, pps::force::no).get());
    BOOST_REQUIRE(store.get_mode(ctx, fallback).get() == expected2);

    BOOST_REQUIRE(store.clear_mode(ctx, pps::force::no).get());
    BOOST_REQUIRE(store.get_mode(ctx, fallback).get() == expected1);

    BOOST_REQUIRE(store.clear_mode(pps::global_context, pps::force::no).get());
    BOOST_REQUIRE(
      store.get_mode(ctx, fallback).get() == pps::default_top_level_mode);
}

// subject in default_context mode, no fallback
// Resolution: subject mode → error
SEASTAR_THREAD_TEST_CASE(
  test_sharded_store_subject_default_context_mode_no_fallback) {
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    auto no_fallback = pps::default_to_global::no;
    auto ctx_sub = pps::context_subject{
      pps::default_context, pps::subject{"sub"}};
    pps::seq_marker dummy_marker;

    BOOST_REQUIRE_EXCEPTION(
      store.get_mode(ctx_sub, no_fallback).get(),
      pps::exception,
      [](const pps::exception& e) {
          return e.code() == pps::error_code::mode_not_found;
      });

    auto expected = pps::mode::read_only;
    BOOST_REQUIRE(
      store.set_mode(dummy_marker, ctx_sub, expected, pps::force::no).get());
    BOOST_REQUIRE(store.get_mode(ctx_sub, no_fallback).get() == expected);
}

// subject in default_context mode, fallback enabled
// Resolution: subject mode → default_context mode → global_context mode →
// hardcoded default
SEASTAR_THREAD_TEST_CASE(
  test_sharded_store_subject_default_context_mode_fallback) {
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    auto fallback = pps::default_to_global::yes;
    auto subject = pps::subject{"sub"};
    auto ctx = pps::default_context;
    auto ctx_sub = pps::context_subject{ctx, subject};
    pps::seq_marker dummy_marker;

    BOOST_REQUIRE(
      store.get_mode(ctx_sub, fallback).get() == pps::default_top_level_mode);

    auto expected1 = pps::mode::read_only;
    BOOST_REQUIRE(
      store
        .set_mode(dummy_marker, pps::global_context, expected1, pps::force::no)
        .get());
    BOOST_REQUIRE(store.get_mode(ctx_sub, fallback).get() == expected1);

    auto expected2 = pps::mode::import;
    BOOST_REQUIRE(
      store
        .set_mode(dummy_marker, pps::default_context, expected2, pps::force::no)
        .get());
    BOOST_REQUIRE(store.get_mode(ctx_sub, fallback).get() == expected2);

    auto expected3 = pps::mode::read_write;
    BOOST_REQUIRE(
      store.set_mode(dummy_marker, ctx_sub, expected3, pps::force::no).get());
    BOOST_REQUIRE(store.get_mode(ctx_sub, fallback).get() == expected3);

    BOOST_REQUIRE(
      store.clear_mode(dummy_marker, ctx_sub, pps::force::no).get());
    BOOST_REQUIRE(store.get_mode(ctx_sub, fallback).get() == expected2);

    BOOST_REQUIRE(store.clear_mode(pps::default_context, pps::force::no).get());
    BOOST_REQUIRE(store.get_mode(ctx_sub, fallback).get() == expected1);

    BOOST_REQUIRE(store.clear_mode(pps::global_context, pps::force::no).get());
    BOOST_REQUIRE(
      store.get_mode(ctx_sub, fallback).get() == pps::default_top_level_mode);
}

// subject in non-default context mode, no fallback
// Resolution: subject mode → error
SEASTAR_THREAD_TEST_CASE(
  test_sharded_store_subject_nondefault_context_mode_no_fallback) {
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    auto no_fallback = pps::default_to_global::no;
    auto ctx = pps::context{".ctx"};
    auto ctx_sub = pps::context_subject{ctx, pps::subject{"sub"}};
    pps::seq_marker dummy_marker;

    BOOST_REQUIRE_EXCEPTION(
      store.get_mode(ctx_sub, no_fallback).get(),
      pps::exception,
      [](const pps::exception& e) {
          return e.code() == pps::error_code::mode_not_found;
      });

    auto expected = pps::mode::read_only;
    BOOST_REQUIRE(
      store.set_mode(dummy_marker, ctx_sub, expected, pps::force::no).get());

    BOOST_REQUIRE(store.get_mode(ctx_sub, no_fallback).get() == expected);

    BOOST_REQUIRE(
      store.clear_mode(dummy_marker, ctx_sub, pps::force::no).get());

    BOOST_REQUIRE_EXCEPTION(
      store.get_mode(ctx_sub, no_fallback).get(),
      pps::exception,
      [](const pps::exception& e) {
          return e.code() == pps::error_code::mode_not_found;
      });
}

// subject in non-default context mode, fallback enabled
// Resolution: subject mode → context mode → global_context mode →
// hardcoded default
SEASTAR_THREAD_TEST_CASE(
  test_sharded_store_subject_nondefault_context_mode_fallback) {
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    auto fallback = pps::default_to_global::yes;
    auto ctx = pps::context{".ctx"};
    auto ctx_sub = pps::context_subject{ctx, pps::subject{"subject"}};
    pps::seq_marker dummy_marker;

    BOOST_REQUIRE(
      store.get_mode(ctx_sub, fallback).get() == pps::default_top_level_mode);

    auto expected1 = pps::mode::read_only;
    BOOST_REQUIRE(
      store
        .set_mode(dummy_marker, pps::global_context, expected1, pps::force::no)
        .get());
    BOOST_REQUIRE(store.get_mode(ctx_sub, fallback).get() == expected1);

    auto expected2 = pps::mode::import;
    BOOST_REQUIRE(
      store.set_mode(dummy_marker, ctx, expected2, pps::force::no).get());
    BOOST_REQUIRE(store.get_mode(ctx_sub, fallback).get() == expected2);

    auto expected3 = pps::mode::read_write;
    BOOST_REQUIRE(
      store.set_mode(dummy_marker, ctx_sub, expected3, pps::force::no).get());
    BOOST_REQUIRE(store.get_mode(ctx_sub, fallback).get() == expected3);

    BOOST_REQUIRE(
      store.clear_mode(dummy_marker, ctx_sub, pps::force::no).get());
    BOOST_REQUIRE(store.get_mode(ctx_sub, fallback).get() == expected2);

    BOOST_REQUIRE(store.clear_mode(ctx, pps::force::no).get());
    BOOST_REQUIRE(store.get_mode(ctx_sub, fallback).get() == expected1);

    BOOST_REQUIRE(store.clear_mode(pps::global_context, pps::force::no).get());
    BOOST_REQUIRE(
      store.get_mode(ctx_sub, fallback).get() == pps::default_top_level_mode);
}

// global_context mode, no fallback
// Resolution: global_context mode → hardcoded default
// (no_fallback is a no-op; global_context is already the top of the chain)
SEASTAR_THREAD_TEST_CASE(test_sharded_store_global_context_mode_no_fallback) {
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    auto no_fallback = pps::default_to_global::no;
    auto ctx = pps::global_context;
    pps::seq_marker dummy_marker;

    BOOST_REQUIRE(
      store.get_mode(ctx, no_fallback).get() == pps::default_top_level_mode);

    auto expected = pps::mode::read_only;
    BOOST_REQUIRE(
      store.set_mode(dummy_marker, ctx, expected, pps::force::no).get());

    BOOST_REQUIRE(store.get_mode(ctx, no_fallback).get() == expected);

    BOOST_REQUIRE(store.clear_mode(ctx, pps::force::no).get());

    BOOST_REQUIRE(
      store.get_mode(ctx, no_fallback).get() == pps::default_top_level_mode);
}

// global_context mode, fallback enabled
// Resolution: global_context mode → hardcoded default
// (fallback flag is a no-op; global_context is already the top of the chain)
SEASTAR_THREAD_TEST_CASE(test_sharded_store_global_context_mode_fallback) {
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    auto fallback = pps::default_to_global::yes;
    auto ctx = pps::global_context;
    pps::seq_marker dummy_marker;

    BOOST_REQUIRE(
      store.get_mode(ctx, fallback).get() == pps::default_top_level_mode);

    auto expected = pps::mode::read_only;
    BOOST_REQUIRE(
      store
        .set_mode(dummy_marker, pps::global_context, expected, pps::force::no)
        .get());
    BOOST_REQUIRE(store.get_mode(ctx, fallback).get() == expected);

    BOOST_REQUIRE(store.clear_mode(pps::global_context, pps::force::no).get());
    BOOST_REQUIRE(
      store.get_mode(ctx, fallback).get() == pps::default_top_level_mode);
}

// subject in global_context mode, no fallback
// Resolution: subject mode → global_context mode → hardcoded default
// (global_context subjects always fall through regardless of fallback flag)
SEASTAR_THREAD_TEST_CASE(
  test_sharded_store_subject_global_context_mode_no_fallback) {
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    auto no_fallback = pps::default_to_global::no;
    auto subject = pps::subject{"sub"};
    auto ctx = pps::global_context;
    auto ctx_sub = pps::context_subject{ctx, subject};
    pps::seq_marker dummy_marker;

    BOOST_REQUIRE(
      store.get_mode(ctx_sub, no_fallback).get()
      == pps::default_top_level_mode);

    auto expected1 = pps::mode::import;
    BOOST_REQUIRE(
      store.set_mode(dummy_marker, ctx, expected1, pps::force::no).get());
    BOOST_REQUIRE(store.get_mode(ctx_sub, no_fallback).get() == expected1);

    auto expected2 = pps::mode::read_only;
    BOOST_REQUIRE(
      store.set_mode(dummy_marker, ctx_sub, expected2, pps::force::no).get());

    BOOST_REQUIRE(store.get_mode(ctx_sub, no_fallback).get() == expected2);

    BOOST_REQUIRE(
      store.clear_mode(dummy_marker, ctx_sub, pps::force::no).get());

    BOOST_REQUIRE(store.get_mode(ctx_sub, no_fallback).get() == expected1);

    BOOST_REQUIRE(store.clear_mode(ctx, pps::force::no).get());

    BOOST_REQUIRE(
      store.get_mode(ctx_sub, no_fallback).get()
      == pps::default_top_level_mode);
}

// subject in global_context mode, fallback enabled
// Resolution: subject mode → global_context mode → hardcoded default
SEASTAR_THREAD_TEST_CASE(
  test_sharded_store_subject_global_context_mode_fallback) {
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    auto fallback = pps::default_to_global::yes;
    auto ctx = pps::global_context;
    auto ctx_sub = pps::context_subject{ctx, pps::subject{"subject"}};
    pps::seq_marker dummy_marker;

    BOOST_REQUIRE(
      store.get_mode(ctx_sub, fallback).get() == pps::default_top_level_mode);

    auto expected1 = pps::mode::read_only;
    BOOST_REQUIRE(
      store
        .set_mode(dummy_marker, pps::global_context, expected1, pps::force::no)
        .get());
    BOOST_REQUIRE(store.get_mode(ctx_sub, fallback).get() == expected1);

    auto expected2 = pps::mode::import;
    BOOST_REQUIRE(
      store.set_mode(dummy_marker, ctx_sub, expected2, pps::force::no).get());
    BOOST_REQUIRE(store.get_mode(ctx_sub, fallback).get() == expected2);

    BOOST_REQUIRE(
      store.clear_mode(dummy_marker, ctx_sub, pps::force::no).get());
    BOOST_REQUIRE(store.get_mode(ctx_sub, fallback).get() == expected1);

    BOOST_REQUIRE(store.clear_mode(pps::global_context, pps::force::no).get());
    BOOST_REQUIRE(
      store.get_mode(ctx_sub, fallback).get() == pps::default_top_level_mode);
}

SEASTAR_THREAD_TEST_CASE(test_sharded_store_referenced_by) {
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    const pps::schema_version ver1{1};

    // Insert simple
    auto referenced_schema = pps::subject_schema{
      pps::context_subject::unqualified("simple.proto"), simple.share()};
    store
      .upsert(
        pps::seq_marker{
          std::nullopt, std::nullopt, ver1, pps::seq_marker_key_type::schema},
        referenced_schema.share(),
        pps::schema_id{1},
        ver1,
        pps::is_deleted::no)
      .get();

    // Insert referenced
    auto importing_schema = pps::subject_schema{
      pps::context_subject::unqualified("imported.proto"), imported.share()};

    store
      .upsert(
        pps::seq_marker{
          std::nullopt, std::nullopt, ver1, pps::seq_marker_key_type::schema},
        importing_schema.share(),
        pps::schema_id{2},
        ver1,
        pps::is_deleted::no)
      .get();

    auto referenced_by
      = store.referenced_by(referenced_schema.sub(), ver1).get();

    BOOST_REQUIRE_EQUAL(referenced_by.size(), 1);
    BOOST_REQUIRE_EQUAL(referenced_by[0].id, pps::schema_id{2});

    BOOST_REQUIRE(store
                    .is_referenced(
                      pps::context_subject::unqualified("simple.proto"),
                      pps::schema_version{1})
                    .get());

    auto importing
      = store.get_schema_definition({pps::default_context, pps::schema_id{2}})
          .get();
    BOOST_REQUIRE_EQUAL(importing.refs().size(), 1);
    BOOST_REQUIRE_EQUAL(importing.refs()[0].sub, imported.refs()[0].sub);
    BOOST_REQUIRE_EQUAL(
      importing.refs()[0].version, imported.refs()[0].version);
    BOOST_REQUIRE_EQUAL(importing.refs()[0].name, imported.refs()[0].name);

    // soft delete subject
    store
      .upsert(
        pps::seq_marker{
          std::nullopt, std::nullopt, ver1, pps::seq_marker_key_type::schema},
        importing_schema.share(),
        pps::schema_id{2},
        ver1,
        pps::is_deleted::yes)
      .get();

    // Soft-deleted should not partake in reference calculations
    BOOST_REQUIRE(store
                    .referenced_by(
                      pps::context_subject::unqualified("simple.proto"),
                      pps::schema_version{1})
                    .get()
                    .empty());
    BOOST_REQUIRE(!store
                     .is_referenced(
                       pps::context_subject::unqualified("simple.proto"),
                       pps::schema_version{1})
                     .get());
}

SEASTAR_THREAD_TEST_CASE(test_sharded_store_find_unordered) {
    pps::sharded_store store;
    store.start(pps::is_mutable::no, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    pps::subject_schema array_unsanitized{
      pps::context_subject::unqualified("array"),
      pps::schema_definition{
        R"({"type": "array", "default": [], "items" : "string"})",
        pps::schema_type::avro}};

    pps::subject_schema array_sanitized{
      pps::context_subject::unqualified("array"),
      pps::schema_definition{
        R"({"type":"array","items":"string","default":[]})",
        pps::schema_type::avro}};

    const pps::schema_version ver1{1};

    // Insert an unsorted schema "onto the topic".
    auto referenced_schema = pps::subject_schema{
      pps::context_subject::unqualified("simple.proto"), simple.share()};
    store
      .upsert(
        pps::seq_marker{
          std::nullopt, std::nullopt, ver1, pps::seq_marker_key_type::schema},
        array_unsanitized.share(),
        pps::schema_id{1},
        ver1,
        pps::is_deleted::no)
      .get();

    auto res = store.has_schema(array_sanitized.share()).get();
    BOOST_REQUIRE_EQUAL(res.id, pps::schema_id{1});
    BOOST_REQUIRE_EQUAL(res.version, ver1);
}
