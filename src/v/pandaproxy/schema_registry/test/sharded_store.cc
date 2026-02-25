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
#include "pandaproxy/schema_registry/util.h"

#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <boost/test/unit_test.hpp>

namespace pp = pandaproxy;
namespace pps = pp::schema_registry;

SEASTAR_THREAD_TEST_CASE(test_sharded_store_global_compat) {
    // Setting and retrieving global compatibility should be allowed multiple
    // times

    pps::seq_marker dummy_marker;
    pps::compatibility_level expected{pps::compatibility_level::backward};
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    BOOST_REQUIRE(
      store.get_compatibility(pps::default_context).get() == expected);

    // duplicate should return false
    BOOST_REQUIRE(
      store.clear_compatibility(pps::default_context).get() == false);
    BOOST_REQUIRE(
      store.get_compatibility(pps::default_context).get() == expected);

    expected = pps::compatibility_level::full_transitive;
    BOOST_REQUIRE(
      store.set_compatibility(dummy_marker, pps::default_context, expected)
        .get()
      == true);
    BOOST_REQUIRE(
      store.get_compatibility(pps::default_context).get() == expected);
}

constexpr std::string_view sv_string_def0{R"({"type":"string"})"};
const pps::schema_definition string_def0{
  pps::make_schema_definition<json::UTF8<>>(sv_string_def0).value(),
  pps::schema_type::avro};
const auto subject0 = pps::context_subject::unqualified("subject0");

SEASTAR_THREAD_TEST_CASE(test_sharded_store_subject_compat) {
    // Setting and retrieving a subject compatibility should be allowed multiple
    // times

    pps::seq_marker dummy_marker;
    auto fallback = pps::default_to_global::yes;

    pps::compatibility_level global_expected{
      pps::compatibility_level::backward};
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    BOOST_REQUIRE(
      store.get_compatibility(pps::default_context).get() == global_expected);

    auto sub_expected = pps::compatibility_level::backward;
    BOOST_REQUIRE(
      store.set_compatibility(dummy_marker, subject0, sub_expected).get()
      == true);
    BOOST_REQUIRE(
      store.get_compatibility(subject0, fallback).get() == sub_expected);

    // duplicate should return false
    sub_expected = pps::compatibility_level::backward;
    BOOST_REQUIRE(
      store.set_compatibility(dummy_marker, subject0, sub_expected).get()
      == false);
    BOOST_REQUIRE(
      store.get_compatibility(subject0, fallback).get() == sub_expected);

    sub_expected = pps::compatibility_level::full_transitive;
    BOOST_REQUIRE(
      store.set_compatibility(dummy_marker, subject0, sub_expected).get()
      == true);
    BOOST_REQUIRE(
      store.get_compatibility(subject0, fallback).get() == sub_expected);
    BOOST_REQUIRE(
      store.get_compatibility(pps::default_context).get() == global_expected);

    // Clearing compatibility should fallback to global
    BOOST_REQUIRE(
      store.clear_compatibility(dummy_marker, subject0).get() == true);
    BOOST_REQUIRE(
      store.get_compatibility(subject0, fallback).get() == global_expected);
}

SEASTAR_THREAD_TEST_CASE(test_sharded_store_subject_compat_fallback) {
    // A Subject should fallback to the current global setting
    pps::seq_marker dummy_marker;
    auto fallback = pps::default_to_global::yes;

    pps::compatibility_level expected{pps::compatibility_level::backward};
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    BOOST_REQUIRE(
      store.get_compatibility(subject0, fallback).get() == expected);

    expected = pps::compatibility_level::forward;
    BOOST_REQUIRE(
      store.set_compatibility(dummy_marker, pps::default_context, expected)
        .get()
      == true);
    BOOST_REQUIRE(
      store.get_compatibility(subject0, fallback).get() == expected);
}

SEASTAR_THREAD_TEST_CASE(test_sharded_store_context_config) {
    // Test setting and getting compatibility (config) at the context level
    auto test_ctx = pps::context{".test"};
    pps::seq_marker dummy_marker;
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });
    auto fallback = pps::default_to_global::yes;

    // Default config is backward compatibility
    BOOST_REQUIRE(
      store.get_compatibility(pps::default_context, fallback).get()
      == pps::compatibility_level::backward);
    BOOST_REQUIRE(
      store.get_compatibility(test_ctx, fallback).get()
      == pps::compatibility_level::backward);

    // Set config on default context
    BOOST_REQUIRE(
      store
        .set_compatibility(
          dummy_marker, pps::default_context, pps::compatibility_level::full)
        .get());
    BOOST_REQUIRE(
      store.get_compatibility(pps::default_context, fallback).get()
      == pps::compatibility_level::full);
    BOOST_REQUIRE(
      store.get_compatibility(test_ctx, fallback).get()
      == pps::compatibility_level::backward);

    // Set different config on test context
    BOOST_REQUIRE(store
                    .set_compatibility(
                      dummy_marker, test_ctx, pps::compatibility_level::none)
                    .get());
    BOOST_REQUIRE(
      store.get_compatibility(pps::default_context, fallback).get()
      == pps::compatibility_level::full);
    BOOST_REQUIRE(
      store.get_compatibility(test_ctx, fallback).get()
      == pps::compatibility_level::none);

    // Clear config returns to default
    BOOST_REQUIRE(store.clear_compatibility(test_ctx).get());
    BOOST_REQUIRE(
      store.get_compatibility(test_ctx, fallback).get()
      == pps::compatibility_level::backward);
}

// Scenarios A, Ca, Cb
// Verifies context-level compatibility in the default context (subject="")
// when fallback to global is disabled (default_to_global::no).
//
// With no fallback, the resolution is only:
//   default_context config → hard-coded default
//
// Steps:
// 1. Initially, no config is set.
//    get_compatibility returns the hard-coded default_top_level_compat
//    (NOT from global_context — fallback is disabled).
//
// 2. Set compatibility on default_context (full).
//    get_compatibility returns the explicitly set value.
//
// 3. Clear default_context config.
//    get_compatibility reverts to default_top_level_compat.
//
// This confirms that with no_fallback, global_context is never consulted
// and the context resolves only its own config or the hard-coded default.
SEASTAR_THREAD_TEST_CASE(
  test_sharded_store_default_context_config_no_fallback) {
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    auto no_fallback = pps::default_to_global::no;
    auto ctx_sub = pps::context_subject{pps::default_context, pps::subject{""}};
    pps::seq_marker dummy_marker;

    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, no_fallback).get()
      == store.default_top_level_compat);

    auto expected = pps::compatibility_level::full;
    BOOST_REQUIRE(
      store
        // TODO: Replace with single set_compatibility(context_subject) overload
        .set_compatibility(dummy_marker, pps::default_context, expected)
        .get());

    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, no_fallback).get() == expected);

    BOOST_REQUIRE(
      // TODO: Replace with single clear_compatibility(context_subject) overload
      store.clear_compatibility(pps::default_context).get());

    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, no_fallback).get()
      == store.default_top_level_compat);
}

// Scenarios B, Da, Db
// Verifies the fallback chain for context-level compatibility in the
// default context (subject="") when default_to_global::yes is used.
//
// The expected resolution order is:
//   default_context config → global_context config → hard-coded default
//
// Steps:
// 1. Initially, no context or global config is set.
//    get_compatibility falls back all the way to default_top_level_compat.
//
// 2. Set compatibility on global_context (full).
//    get_compatibility now resolves to the global_context value.
//
// 3. Set compatibility on default_context (none).
//    get_compatibility now resolves to the default_context value,
//    which takes priority over global_context.
//
// 4. Clear default_context config.
//    get_compatibility falls back to the global_context value again.
//
// 5. Clear global_context config.
//    get_compatibility falls back to the hard-coded default_top_level_compat.
//
// This confirms the full fallback chain works correctly and that each
// layer properly shadows the one below it.
SEASTAR_THREAD_TEST_CASE(test_sharded_store_default_context_config_fallback) {
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    auto fallback = pps::default_to_global::yes;
    auto ctx_sub = pps::context_subject{pps::default_context, pps::subject{""}};
    pps::seq_marker dummy_marker;

    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, fallback).get()
      == store.default_top_level_compat);

    auto expected1 = pps::compatibility_level::full;
    BOOST_REQUIRE(
      store
        // TODO: Replace with single set_compatibility(context_subject) overload
        .set_compatibility(
          dummy_marker, pandaproxy::schema_registry::global_context, expected1)
        .get());

    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, fallback).get() == expected1);

    auto expected2 = pps::compatibility_level::none;
    BOOST_REQUIRE(
      store
        // TODO: Replace with single set_compatibility(context_subject) overload
        .set_compatibility(
          dummy_marker, pandaproxy::schema_registry::default_context, expected2)
        .get());

    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, fallback).get() == expected2);

    BOOST_REQUIRE(
      // TODO: Replace with single clear_compatibility(context_subject) overload
      store.clear_compatibility(pps::default_context).get());

    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, fallback).get() == expected1);

    BOOST_REQUIRE(
      // TODO: Replace with single clear_compatibility(context_subject) overload
      store.clear_compatibility(pps::global_context).get());

    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, fallback).get()
      == store.default_top_level_compat);
}

// Scenario Cc
// Verifies that get_compatibility for a subject in the default context
// behaves correctly when fallback to the global default is disabled
// (default_to_global::no):
//
// 1. With no compatibility set, get_compatibility throws
//    compatibility_not_found — it does NOT fall back to any global default.
// 2. After explicitly setting compatibility on the subject,
//    get_compatibility returns the value that was set.
//
// This ensures the no-fallback path is isolated: subjects only see
// their own explicitly configured compatibility level.
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

// Scenario Cd
// Verifies context-level compatibility for a non-default context (".ctx")
// with subject="" when fallback is disabled (default_to_global::no).
//
// Unlike the default context, a non-default context has NO hard-coded
// default to fall back to. With fallback also disabled, the resolution is:
//   context config → error (compatibility_not_found)
//
// Steps:
// 1. Initially, no config is set.
//    get_compatibility throws compatibility_not_found — there is no
//    implicit default for non-default contexts.
//
// 2. Set compatibility on the context (full).
//    get_compatibility returns the explicitly set value.
//
// 3. Clear the context config.
//    get_compatibility throws compatibility_not_found again.
//
// This confirms that non-default contexts are strictly explicit: they
// have no built-in default and no global fallback, so an unset config
// is an error rather than a silent default.
SEASTAR_THREAD_TEST_CASE(
  test_sharded_store_nondefault_context_config_no_fallback) {
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    auto no_fallback = pps::default_to_global::no;
    auto ctx = pps::context{".ctx"};
    auto ctx_sub = pps::context_subject{ctx, pps::subject{""}};
    pps::seq_marker dummy_marker;

    BOOST_REQUIRE_EXCEPTION(
      store.get_compatibility(ctx_sub, no_fallback).get(),
      pps::exception,
      [](const pps::exception& e) {
          return e.code() == pps::error_code::compatibility_not_found;
      });

    auto expected = pps::compatibility_level::full;
    BOOST_REQUIRE(
      store
        // TODO: Replace with single set_compatibility(context_subject) overload
        .set_compatibility(dummy_marker, ctx, expected)
        .get());

    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, no_fallback).get() == expected);

    BOOST_REQUIRE(
      // TODO: Replace with single clear_compatibility(context_subject) overload
      store.clear_compatibility(ctx).get());

    BOOST_REQUIRE_EXCEPTION(
      store.get_compatibility(ctx_sub, no_fallback).get(),
      pps::exception,
      [](const pps::exception& e) {
          return e.code() == pps::error_code::compatibility_not_found;
      });
}

// Scenario Ce
// Verifies subject-level compatibility for a subject ("sub") in a
// non-default context (".ctx") when fallback is disabled
// (default_to_global::no).
//
// This is the most restrictive lookup: a specific subject in a non-default
// context with no fallback. Resolution is:
//   subject config → error (compatibility_not_found)
//
// Steps:
// 1. Initially, no config is set.
//    get_compatibility throws compatibility_not_found — no subject config,
//    no context fallback, no global fallback.
//
// 2. Set compatibility directly on the context_subject (full).
//    get_compatibility returns the explicitly set value.
//
// 3. Clear the subject's config.
//    get_compatibility throws compatibility_not_found again.
//
// This confirms that subject-level lookups in non-default contexts are
// fully isolated when fallback is off: only the subject's own explicit
// config is used, with no chain to the context or global level.
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

// Scenario Cf
// Verifies context-level compatibility for the global context (subject="")
// when fallback is disabled (default_to_global::no).
//
// The global context is the root of the fallback hierarchy. Even with
// fallback disabled, it still has the hard-coded default to fall back to.
// Resolution is:
//   global_context config → hard-coded default
//
// Steps:
// 1. Initially, no config is set.
//    get_compatibility returns default_top_level_compat.
//
// 2. Set compatibility on global_context (full).
//    get_compatibility returns the explicitly set value.
//
// 3. Clear global_context config.
//    get_compatibility reverts to default_top_level_compat.
//
// This confirms that the global context behaves like the default context
// in no-fallback mode: it resolves its own config or the hard-coded
// default. The no_fallback flag is effectively a no-op here since the
// global context is already the top of the chain.
SEASTAR_THREAD_TEST_CASE(test_sharded_store_global_context_config_no_fallback) {
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    auto no_fallback = pps::default_to_global::no;
    auto ctx = pandaproxy::schema_registry::global_context;
    auto ctx_sub = pps::context_subject{ctx, pps::subject{""}};
    pps::seq_marker dummy_marker;

    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, no_fallback).get()
      == store.default_top_level_compat);

    auto expected = pps::compatibility_level::full;
    BOOST_REQUIRE(
      store
        // TODO: Replace with single set_compatibility(context_subject) overload
        .set_compatibility(dummy_marker, ctx, expected)
        .get());

    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, no_fallback).get() == expected);

    BOOST_REQUIRE(
      // TODO: Replace with single clear_compatibility(context_subject) overload
      store.clear_compatibility(ctx).get());

    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, no_fallback).get()
      == store.default_top_level_compat);
}

// Scenario Cg
// Verifies subject-level compatibility for a subject ("sub") in the
// global context when fallback is disabled (default_to_global::no).
//
// Unlike subjects in non-default contexts (which error out when unset),
// subjects in the global context fall back to the hard-coded default.
// Resolution is:
//   subject config → hard-coded default
//
// Steps:
// 1. Initially, no subject config is set.
//    get_compatibility returns default_top_level_compat — the global
//    context's implicit baseline applies even with no_fallback.
//
// 2. Set compatibility on the subject (full).
//    get_compatibility returns the explicitly set value.
//
// 3. Clear the subject's config.
//    get_compatibility reverts to default_top_level_compat.
//
// This confirms that subjects in the global context inherit the same
// hard-coded default as the global context itself, even with fallback
// disabled.
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
      == store.default_top_level_compat);

    // Set global context compatibility
    auto expected = pps::compatibility_level::full;
    BOOST_REQUIRE(
      store.set_compatibility(dummy_marker, ctx_sub, expected).get());

    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, no_fallback).get() == expected);

    BOOST_REQUIRE(store.clear_compatibility(dummy_marker, ctx_sub).get());

    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, no_fallback).get()
      == store.default_top_level_compat);
}

// Scenario Dc
// Verifies the full fallback chain for a subject ("sub") in the default
// context when fallback is enabled (default_to_global::yes).
//
// This is the most complete fallback test — it exercises all four tiers:
//   subject config → default_context config → global_context config →
//   hard-coded default
//
// Steps (building up the chain, then tearing it down):
//
// 1. Initially, nothing is set.
//    get_compatibility falls through everything to default_top_level_compat.
//
// 2. Set global_context compatibility (full).
//    get_compatibility resolves to global_context value.
//
// 3. Set default_context compatibility (forward).
//    get_compatibility resolves to default_context value, shadowing global.
//
// 4. Set subject-level compatibility (none).
//    get_compatibility resolves to subject value, shadowing both contexts.
//
// 5. Clear subject config.
//    get_compatibility falls back to default_context value (forward).
//
// 6. Clear default_context config.
//    get_compatibility falls back to global_context value (full).
//
// 7. Clear global_context config.
//    get_compatibility falls back to hard-coded default_top_level_compat.
//
// This confirms the complete four-tier priority chain: each layer
// correctly shadows those below it, and clearing a layer reveals
// the next one down.
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
      == store.default_top_level_compat);

    auto expected1 = pps::compatibility_level::full;
    BOOST_REQUIRE(
      store
        // TODO: Replace with single set_compatibility(context_subject) overload
        .set_compatibility(dummy_marker, pps::global_context, expected1)
        .get());
    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, fallback).get() == expected1);

    auto expected2 = pps::compatibility_level::forward;
    BOOST_REQUIRE(
      store
        // TODO: Replace with single set_compatibility(context_subject) overload
        .set_compatibility(dummy_marker, pps::default_context, expected2)
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

    BOOST_REQUIRE(
      // TODO: Replace with single clear_compatibility(context_subject) overload
      store.clear_compatibility(pps::default_context).get());
    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, fallback).get() == expected1);

    BOOST_REQUIRE(
      // TODO: Replace with single clear_compatibility(context_subject) overload
      store.clear_compatibility(pps::global_context).get());
    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, fallback).get()
      == store.default_top_level_compat);
}

// Scenario Dd
// Verifies the fallback chain for context-level compatibility in a
// non-default context (".ctx", subject="") when fallback is enabled
// (default_to_global::yes).
//
// Non-default contexts skip the default_context layer and fall back
// directly to global_context. The resolution order is:
//   context config → global_context config → hard-coded default
//
// Steps (build up, then tear down):
//
// 1. Initially, nothing is set.
//    get_compatibility falls through to default_top_level_compat.
//
// 2. Set global_context compatibility (full).
//    get_compatibility resolves to global_context value.
//
// 3. Set context-level compatibility on ".ctx" (forward).
//    get_compatibility resolves to the context's own value, shadowing global.
//
// 4. Clear ".ctx" config.
//    get_compatibility falls back to global_context value (full).
//
// 5. Clear global_context config.
//    get_compatibility falls back to hard-coded default_top_level_compat.
//
// This confirms that non-default contexts with fallback enabled have a
// three-tier chain (context → global → default), notably skipping the
// default_context layer that default-context lookups include.
SEASTAR_THREAD_TEST_CASE(
  test_sharded_store_nondefault_context_config_fallback) {
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    auto fallback = pps::default_to_global::yes;
    auto ctx = pps::context{".ctx"};
    auto ctx_sub = pps::context_subject{ctx, pps::subject{""}};
    pps::seq_marker dummy_marker;

    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, fallback).get()
      == store.default_top_level_compat);

    auto expected1 = pps::compatibility_level::full;
    BOOST_REQUIRE(
      store
        // TODO: Replace with single set_compatibility(context_subject) overload
        .set_compatibility(dummy_marker, pps::global_context, expected1)
        .get());
    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, fallback).get() == expected1);

    auto expected2 = pps::compatibility_level::forward;
    BOOST_REQUIRE(
      store
        // TODO: Replace with single set_compatibility(context_subject) overload
        .set_compatibility(dummy_marker, ctx, expected2)
        .get());
    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, fallback).get() == expected2);

    // TODO: Replace with single clear_compatibility(context_subject) overload
    BOOST_REQUIRE(store.clear_compatibility(ctx).get());
    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, fallback).get() == expected1);

    BOOST_REQUIRE(
      // TODO: Replace with single clear_compatibility(context_subject) overload
      store.clear_compatibility(pps::global_context).get());
    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, fallback).get()
      == store.default_top_level_compat);
}

// Scenario De
// Verifies the full fallback chain for a subject ("subject") in a
// non-default context (".ctx") when fallback is enabled
// (default_to_global::yes).
//
// This is the most complete fallback test for non-default contexts.
// The resolution order is:
//   subject config → context config → global_context config → hard-coded
//   default
//
// Note: unlike the default context equivalent, there is no default_context
// layer — non-default contexts skip it and fall back directly to global.
//
// Steps (build up, then tear down):
//
// 1. Initially, nothing is set.
//    get_compatibility falls through everything to default_top_level_compat.
//
// 2. Set global_context compatibility (full).
//    get_compatibility resolves to global_context value.
//
// 3. Set context-level compatibility on ".ctx" (forward).
//    get_compatibility resolves to context value, shadowing global.
//
// 4. Set subject-level compatibility (none).
//    get_compatibility resolves to subject value, shadowing both.
//
// 5. Clear subject config.
//    get_compatibility falls back to context value (forward).
//
// 6. Clear ".ctx" config.
//    get_compatibility falls back to global_context value (full).
//
// 7. Clear global_context config.
//    get_compatibility falls back to hard-coded default_top_level_compat.
//
// This confirms the complete four-tier chain for non-default contexts:
// subject → context → global → hard-coded default, with each layer
// correctly shadowing those below and revealing the next on removal.
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
      == store.default_top_level_compat);

    auto expected1 = pps::compatibility_level::full;
    BOOST_REQUIRE(
      store
        // TODO: Replace with single set_compatibility(context_subject) overload
        .set_compatibility(dummy_marker, pps::global_context, expected1)
        .get());
    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, fallback).get() == expected1);

    auto expected2 = pps::compatibility_level::forward;
    BOOST_REQUIRE(
      store
        // TODO: Replace with single set_compatibility(context_subject) overload
        .set_compatibility(dummy_marker, ctx, expected2)
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

    // TODO: Replace with single clear_compatibility(context_subject) overload
    BOOST_REQUIRE(store.clear_compatibility(ctx).get());
    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, fallback).get() == expected1);

    BOOST_REQUIRE(
      // TODO: Replace with single clear_compatibility(context_subject) overload
      store.clear_compatibility(pps::global_context).get());
    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, fallback).get()
      == store.default_top_level_compat);
}

// Scenario Df
// Verifies context-level compatibility for the global context (subject="")
// when fallback is enabled (default_to_global::yes).
//
// Since the global context is already the top of the fallback hierarchy,
// enabling fallback has no additional effect — the behavior is identical
// to the no_fallback variant. Resolution is:
//   global_context config → hard-coded default
//
// Steps:
// 1. Initially, no config is set.
//    get_compatibility returns default_top_level_compat.
//
// 2. Set global_context compatibility (full).
//    get_compatibility returns the explicitly set value.
//
// 3. Clear global_context config.
//    get_compatibility reverts to default_top_level_compat.
//
// This confirms that the fallback flag is a no-op for the global context
// itself — there is nothing above it to fall back to, so the result is
// the same regardless of the default_to_global setting.
SEASTAR_THREAD_TEST_CASE(test_sharded_store_global_context_config_fallback) {
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    auto fallback = pps::default_to_global::yes;
    auto ctx = pps::global_context;
    auto ctx_sub = pps::context_subject{ctx, pps::subject{""}};
    pps::seq_marker dummy_marker;

    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, fallback).get()
      == store.default_top_level_compat);

    auto expected = pps::compatibility_level::full;
    BOOST_REQUIRE(
      store
        // TODO: Replace with single set_compatibility(context_subject) overload
        .set_compatibility(dummy_marker, pps::global_context, expected)
        .get());
    BOOST_REQUIRE(store.get_compatibility(ctx_sub, fallback).get() == expected);

    BOOST_REQUIRE(
      // TODO: Replace with single clear_compatibility(context_subject) overload
      store.clear_compatibility(pps::global_context).get());
    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, fallback).get()
      == store.default_top_level_compat);
}

// Scenario Dg
// Verifies the fallback chain for a subject ("subject") in the global
// context when fallback is enabled (default_to_global::yes).
//
// Subjects in the global context have a three-tier resolution:
//   subject config → global_context config → hard-coded default
//
// Since we're already in the global context, there is no intermediate
// context layer to traverse — fallback goes straight from subject to
// global-level config.
//
// Steps (build up, then tear down):
//
// 1. Initially, nothing is set.
//    get_compatibility falls through to default_top_level_compat.
//
// 2. Set global_context compatibility (full).
//    get_compatibility resolves to global_context value.
//
// 3. Set subject-level compatibility (forward).
//    get_compatibility resolves to subject value, shadowing global.
//
// 4. Clear subject config.
//    get_compatibility falls back to global_context value (full).
//
// 5. Clear global_context config.
//    get_compatibility falls back to hard-coded default_top_level_compat.
//
// This confirms the three-tier chain for subjects in the global context.
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
      == store.default_top_level_compat);

    auto expected1 = pps::compatibility_level::full;
    BOOST_REQUIRE(
      store
        // TODO: Replace with single set_compatibility(context_subject) overload
        .set_compatibility(dummy_marker, pps::global_context, expected1)
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

    BOOST_REQUIRE(
      // TODO: Replace with single clear_compatibility(context_subject) overload
      store.clear_compatibility(pps::global_context).get());
    BOOST_REQUIRE(
      store.get_compatibility(ctx_sub, fallback).get()
      == store.default_top_level_compat);
}

SEASTAR_THREAD_TEST_CASE(test_sharded_store_context_config_written_at) {
    // Test that config (compatibility) write markers are tracked correctly
    auto test_ctx = pps::context{".test"};
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    // Initially no write markers
    auto markers = store.get_context_config_written_at(test_ctx).get();
    BOOST_REQUIRE(markers.empty());

    // Create distinct markers
    auto marker1 = pps::seq_marker{
      .seq = model::offset{10},
      .node = model::node_id{1},
      .version = pps::schema_version{0},
      .key_type = pps::seq_marker_key_type::config};
    auto marker2 = pps::seq_marker{
      .seq = model::offset{20},
      .node = model::node_id{1},
      .version = pps::schema_version{0},
      .key_type = pps::seq_marker_key_type::config};

    // Set compatibility on test context, verify marker is tracked
    BOOST_REQUIRE(
      store.set_compatibility(marker1, test_ctx, pps::compatibility_level::full)
        .get());
    markers = store.get_context_config_written_at(test_ctx).get();
    BOOST_REQUIRE_EQUAL(markers.size(), 1);
    BOOST_REQUIRE_EQUAL(markers[0], marker1);

    // Set compatibility again, second marker is added
    BOOST_REQUIRE(
      store.set_compatibility(marker2, test_ctx, pps::compatibility_level::none)
        .get());
    markers = store.get_context_config_written_at(test_ctx).get();
    BOOST_REQUIRE_EQUAL(markers.size(), 2);
    BOOST_REQUIRE_EQUAL(markers[0], marker1);
    BOOST_REQUIRE_EQUAL(markers[1], marker2);

    // Default context should still have no markers
    markers = store.get_context_config_written_at(pps::default_context).get();
    BOOST_REQUIRE(markers.empty());

    // Clear compatibility clears all markers
    BOOST_REQUIRE(store.clear_compatibility(test_ctx).get());
    markers = store.get_context_config_written_at(test_ctx).get();
    BOOST_REQUIRE(markers.empty());
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
