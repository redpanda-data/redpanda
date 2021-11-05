// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/test/compatibility_protobuf.h"

#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/exceptions.h"
#include "pandaproxy/schema_registry/protobuf.h"
#include "pandaproxy/schema_registry/sharded_store.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

namespace pp = pandaproxy;
namespace pps = pp::schema_registry;

struct simple_sharded_store {
    simple_sharded_store() {
        store.start(ss::default_smp_service_group()).get();
    }
    ~simple_sharded_store() { store.stop().get(); }
    simple_sharded_store(const simple_sharded_store&) = delete;
    simple_sharded_store(simple_sharded_store&&) = delete;
    simple_sharded_store& operator=(const simple_sharded_store&) = delete;
    simple_sharded_store& operator=(simple_sharded_store&&) = delete;

    pps::schema_id
    insert(const pps::canonical_schema& schema, pps::schema_version version) {
        const auto id = next_id++;
        store
          .upsert(
            pps::seq_marker{
              std::nullopt,
              std::nullopt,
              version,
              pps::seq_marker_key_type::schema},
            schema,
            id,
            version,
            pps::is_deleted::no)
          .get();
        return id;
    }

    pps::schema_id next_id{1};
    pps::sharded_store store;
};

SEASTAR_THREAD_TEST_CASE(test_protobuf_simple) {
    simple_sharded_store store;

    auto schema1 = pps::canonical_schema{pps::subject{"simple"}, simple};
    auto sch1 = store.insert(schema1, pps::schema_version{1});
    auto valid_simple
      = pps::make_protobuf_schema_definition(store.store, schema1).get();
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_imported_failure) {
    simple_sharded_store store;

    // imported depends on simple, which han't been inserted
    auto schema1 = pps::canonical_schema{pps::subject{"imported"}, imported};
    auto sch1 = store.insert(schema1, pps::schema_version{1});
    BOOST_REQUIRE_EXCEPTION(
      pps::make_protobuf_schema_definition(store.store, schema1).get(),
      pps::exception,
      [](const pps::exception& ex) {
          return ex.code() == pps::error_code::schema_invalid;
      });
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_imported) {
    simple_sharded_store store;

    auto schema1 = pps::canonical_schema{pps::subject{"simple"}, simple};
    auto schema2 = pps::canonical_schema{pps::subject{"imported"}, imported};
    auto schema3 = pps::canonical_schema{
      pps::subject{"imported-again"}, imported_again};

    auto sch1 = store.insert(schema1, pps::schema_version{1});
    auto sch2 = store.insert(schema2, pps::schema_version{1});
    auto sch3 = store.insert(schema3, pps::schema_version{1});

    auto valid_simple
      = pps::make_protobuf_schema_definition(store.store, schema1).get();
    auto valid_imported
      = pps::make_protobuf_schema_definition(store.store, schema2).get();
    auto valid_imported_again
      = pps::make_protobuf_schema_definition(store.store, schema3).get();
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_referenced) {
    simple_sharded_store store;

    auto schema1 = pps::canonical_schema{pps::subject{"simple.proto"}, simple};
    auto schema2 = pps::canonical_schema{
      pps::subject{"imported.proto"},
      imported,
      {{"simple", pps::subject{"simple.proto"}, pps::schema_version{1}}}};
    auto schema3 = pps::canonical_schema{
      pps::subject{"imported-again.proto"},
      imported_again,
      {{"imported", pps::subject{"imported.proto"}, pps::schema_version{1}}}};

    auto sch1 = store.insert(schema1, pps::schema_version{1});
    auto sch2 = store.insert(schema2, pps::schema_version{1});
    auto sch3 = store.insert(schema3, pps::schema_version{1});

    auto valid_simple
      = pps::make_protobuf_schema_definition(store.store, schema1).get();
    auto valid_imported
      = pps::make_protobuf_schema_definition(store.store, schema2).get();
    auto valid_imported_again
      = pps::make_protobuf_schema_definition(store.store, schema3).get();
}
