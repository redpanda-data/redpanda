// Copyright 2021 Redpanda Data, Inc.
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
#include "pandaproxy/schema_registry/types.h"

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

bool check_compatible(
  pps::compatibility_level lvl,
  std::string_view reader,
  std::string_view writer) {
    simple_sharded_store store;
    store.store.set_compatibility(lvl).get();
    store.insert(
      pandaproxy::schema_registry::canonical_schema{
        pps::subject{"sub"},
        pps::canonical_schema_definition{writer, pps::schema_type::protobuf},
        {}},
      pps::schema_version{1});
    return store.store
      .is_compatible(
        pps::schema_version{1},
        pps::canonical_schema{
          pps::subject{"sub"},
          pps::canonical_schema_definition{reader, pps::schema_type::protobuf},
          {}})
      .get();
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_simple) {
    simple_sharded_store store;

    auto schema1 = pps::canonical_schema{pps::subject{"simple"}, simple};
    store.insert(schema1, pps::schema_version{1});
    auto valid_simple
      = pps::make_protobuf_schema_definition(store.store, schema1).get();
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_imported_failure) {
    simple_sharded_store store;

    // imported depends on simple, which han't been inserted
    auto schema1 = pps::canonical_schema{pps::subject{"imported"}, imported};
    store.insert(schema1, pps::schema_version{1});
    BOOST_REQUIRE_EXCEPTION(
      pps::make_protobuf_schema_definition(store.store, schema1).get(),
      pps::exception,
      [](const pps::exception& ex) {
          return ex.code() == pps::error_code::schema_invalid;
      });
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_imported_not_referenced) {
    simple_sharded_store store;

    auto schema1 = pps::canonical_schema{pps::subject{"simple"}, simple};
    auto schema2 = pps::canonical_schema{pps::subject{"imported"}, imported};

    store.insert(schema1, pps::schema_version{1});

    auto valid_simple
      = pps::make_protobuf_schema_definition(store.store, schema1).get();
    BOOST_REQUIRE_EXCEPTION(
      pps::make_protobuf_schema_definition(store.store, schema2).get(),
      pps::exception,
      [](const pps::exception& ex) {
          return ex.code() == pps::error_code::schema_invalid;
      });
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

    store.insert(schema1, pps::schema_version{1});
    store.insert(schema2, pps::schema_version{1});
    store.insert(schema3, pps::schema_version{1});

    auto valid_simple
      = pps::make_protobuf_schema_definition(store.store, schema1).get();
    auto valid_imported
      = pps::make_protobuf_schema_definition(store.store, schema2).get();
    auto valid_imported_again
      = pps::make_protobuf_schema_definition(store.store, schema3).get();
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_recursive_reference) {
    simple_sharded_store store;

    auto schema1 = pps::canonical_schema{pps::subject{"simple.proto"}, simple};
    auto schema2 = pps::canonical_schema{
      pps::subject{"imported.proto"},
      imported,
      {{"simple", pps::subject{"simple.proto"}, pps::schema_version{1}}}};
    auto schema3 = pps::canonical_schema{
      pps::subject{"imported-twice.proto"},
      imported_twice,
      {{"simple", pps::subject{"simple.proto"}, pps::schema_version{1}},
       {"imported", pps::subject{"imported.proto"}, pps::schema_version{1}}}};

    store.insert(schema1, pps::schema_version{1});
    store.insert(schema2, pps::schema_version{1});
    store.insert(schema3, pps::schema_version{1});

    auto valid_simple
      = pps::make_protobuf_schema_definition(store.store, schema1).get();
    auto valid_imported
      = pps::make_protobuf_schema_definition(store.store, schema2).get();
    auto valid_imported_again
      = pps::make_protobuf_schema_definition(store.store, schema3).get();
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_well_known) {
    simple_sharded_store store;

    auto schema = pps::canonical_schema{
      pps::subject{"test_auto_well_known"},
      pps::canonical_schema_definition{
        R"(
syntax =  "proto3";
package test;
import "google/protobuf/any.proto";
import "google/protobuf/api.proto";
import "google/protobuf/duration.proto";
import "google/protobuf/empty.proto";
import "google/protobuf/field_mask.proto";
import "google/protobuf/source_context.proto";
import "google/protobuf/struct.proto";
import "google/protobuf/timestamp.proto";
import "google/protobuf/type.proto";
import "google/protobuf/wrappers.proto";
import "google/type/calendar_period.proto";
import "google/type/color.proto";
import "google/type/date.proto";
import "google/type/datetime.proto";
import "google/type/dayofweek.proto";
import "google/type/decimal.proto";
import "google/type/expr.proto";
import "google/type/fraction.proto";
import "google/type/interval.proto";
import "google/type/latlng.proto";
import "google/type/localized_text.proto";
import "google/type/money.proto";
import "google/type/month.proto";
import "google/type/phone_number.proto";
import "google/type/postal_address.proto";
import "google/type/quaternion.proto";
import "google/type/timeofday.proto";
import "confluent/meta.proto";
import "confluent/types/decimal.proto";

message well_known_types {
  google.protobuf.Any any = 1;
  google.protobuf.Api api = 2;
  google.protobuf.BoolValue bool_value = 3;
  google.protobuf.BytesValue bytes_value = 4;
  google.protobuf.DoubleValue double_value = 5;
  google.protobuf.Duration duration = 6;
  google.protobuf.Empty empty = 7;
  google.protobuf.Enum enum = 8;
  google.protobuf.EnumValue enum_value = 9;
  google.protobuf.Field field = 10;
  google.protobuf.FieldMask field_mask = 11;
  google.protobuf.FloatValue float_value = 12;
  google.protobuf.Int32Value int32_value = 13;
  google.protobuf.Int64Value int64_value = 14;
  google.protobuf.ListValue list_value = 15;
  google.protobuf.Method method = 16;
  google.protobuf.Mixin mixin = 17;
  google.protobuf.NullValue null_value = 18;
  google.protobuf.Option option = 19;
  google.protobuf.SourceContext source_context = 20;
  google.protobuf.StringValue string_value = 21;
  google.protobuf.Struct struct = 22;
  google.protobuf.Syntax syntax = 23;
  google.protobuf.Timestamp timestamp = 24;
  google.protobuf.Type type = 25;
  google.protobuf.UInt32Value uint32_value = 26;
  google.protobuf.UInt64Value uint64_value = 27;
  google.protobuf.Value value = 28;
  google.type.CalendarPeriod calendar_period = 29;
  google.type.Color color = 30;
  google.type.Date date = 31;
  google.type.DateTime date_time = 32;
  google.type.DayOfWeek day_of_wekk = 33;
  google.type.Decimal decimal = 34;
  google.type.Expr expr = 35;
  google.type.Fraction fraction = 36;
  google.type.Interval interval = 37;
  google.type.LatLng lat_lng = 39;
  google.type.LocalizedText localized_text = 40;
  google.type.Money money = 41;
  google.type.Month month = 42;
  google.type.PhoneNumber phone_number = 43;
  google.type.PostalAddress postal_address = 44;
  google.type.Quaternion quaternion = 45;
  google.type.TimeOfDay time_of_day = 46;
  confluent.Meta c_meta = 47;
  confluent.type.Decimal c_decimal = 48;
})",
        pps::schema_type::protobuf}};
    store.insert(schema, pps::schema_version{1});

    auto valid_empty
      = pps::make_protobuf_schema_definition(store.store, schema).get();
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_compatibility_empty) {
    BOOST_REQUIRE(check_compatible(
      pps::compatibility_level::full,
      R"(syntax = "proto3";)",
      R"(syntax = "proto3";)"));
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_compatibility_encoding) {
    BOOST_REQUIRE(check_compatible(
      // varint
      pps::compatibility_level::full_transitive,
      R"(syntax = "proto3"; message Test { int32 id = 1; })",
      R"(syntax = "proto3"; message Test { int32 id = 1; })"));

    BOOST_REQUIRE(check_compatible(
      pps::compatibility_level::full_transitive,
      R"(syntax = "proto3"; message Test { int32 id = 1; })",
      R"(syntax = "proto3"; message Test { uint32 id = 1; })"));

    BOOST_REQUIRE(check_compatible(
      pps::compatibility_level::full_transitive,
      R"(syntax = "proto3"; message Test { int32 id = 1; })",
      R"(syntax = "proto3"; message Test { uint64 id = 1; })"));

    BOOST_REQUIRE(check_compatible(
      pps::compatibility_level::full_transitive,
      R"(syntax = "proto3"; message Test { int32 id = 1; })",
      R"(syntax = "proto3"; message Test { bool id = 1; })"));

    // zigzag
    BOOST_REQUIRE(check_compatible(
      pps::compatibility_level::full_transitive,
      R"(syntax = "proto3"; message Test { sint32 id = 1; })",
      R"(syntax = "proto3"; message Test { sint64 id = 1; })"));

    // bytes
    BOOST_REQUIRE(check_compatible(
      pps::compatibility_level::full_transitive,
      R"(syntax = "proto3"; message Test { string id = 1; })",
      R"(syntax = "proto3"; message Test { bytes id = 1; })"));

    // int32
    BOOST_REQUIRE(check_compatible(
      pps::compatibility_level::full_transitive,
      R"(syntax = "proto3"; message Test { fixed32 id = 1; })",
      R"(syntax = "proto3"; message Test { sfixed32 id = 1; })"));

    // int64
    BOOST_REQUIRE(check_compatible(
      pps::compatibility_level::full_transitive,
      R"(syntax = "proto3"; message Test { fixed64 id = 1; })",
      R"(syntax = "proto3"; message Test { sfixed64 id = 1; })"));

    // A subset of incompatible types
    BOOST_REQUIRE(!check_compatible(
      pps::compatibility_level::forward,
      R"(syntax = "proto3"; message Test { int32 id = 1; })",
      R"(syntax = "proto3"; message Test { string id = 1; })"));

    BOOST_REQUIRE(!check_compatible(
      pps::compatibility_level::backward,
      R"(syntax = "proto3"; message Test { int32 id = 1; })",
      R"(syntax = "proto3"; message Test { string id = 1; })"));

    BOOST_REQUIRE(!check_compatible(
      pps::compatibility_level::backward_transitive,
      R"(syntax = "proto3"; message Test { int32 id = 1; })",
      R"(syntax = "proto3"; message Test { fixed32 id = 1; })"));

    BOOST_REQUIRE(!check_compatible(
      pps::compatibility_level::forward_transitive,
      R"(syntax = "proto3"; message Test { fixed32 id = 1; })",
      R"(syntax = "proto3"; message Test { fixed64 id = 1; })"));

    BOOST_REQUIRE(!check_compatible(
      pps::compatibility_level::full,
      R"(syntax = "proto3"; message Test { float id = 1; })",
      R"(syntax = "proto3"; message Test { double id = 1; })"));
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_compatibility_rename_field) {
    BOOST_REQUIRE(check_compatible(
      pps::compatibility_level::full,
      R"(syntax = "proto3"; message Simple { string id = 1; })",
      R"(syntax = "proto3"; message Simple { string identifier = 1; })"));
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_compatibility_add_field) {
    BOOST_REQUIRE(check_compatible(
      pps::compatibility_level::full,
      R"(
syntax = "proto3"; message Simple { string id = 1; })",
      R"(
syntax = "proto3"; message Simple { string id = 1; string name = 2; })"));
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_compatibility_add_message_after) {
    auto reader = R"(syntax = "proto3";
message Simple { string id = 1; }
message Simple2 { int64 id = 1; })";
    auto writer = R"(syntax = "proto3";
message Simple { string id = 1; })";
    BOOST_REQUIRE(
      check_compatible(pps::compatibility_level::backward, reader, writer));
    BOOST_REQUIRE(
      !check_compatible(pps::compatibility_level::forward, reader, writer));
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_compatibility_add_message_before) {
    auto reader = R"(
syntax = "proto3";
message Simple2 { int64 id = 1; }
message Simple { string id = 1; })";
    auto writer = R"(
syntax = "proto3";
message Simple { string id = 1; })";
    BOOST_REQUIRE(
      check_compatible(pps::compatibility_level::backward, reader, writer));
    BOOST_REQUIRE(
      !check_compatible(pps::compatibility_level::forward, reader, writer));
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_compatibility_reserved_field) {
    BOOST_REQUIRE(check_compatible(
      pps::compatibility_level::full_transitive,
      R"(syntax = "proto3"; message Simple { reserved 1; int32 id = 2; })",
      R"(syntax = "proto3"; message Simple { int64 res = 1; int32 id = 2; })"));
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_compatibility_missing_field) {
    BOOST_REQUIRE(check_compatible(
      pps::compatibility_level::full_transitive,
      R"(syntax = "proto3"; message Simple { int32 id = 2; })",
      R"(syntax = "proto3"; message Simple { string res = 1; int32 id = 2; })"));
}

constexpr std::string_view recursive = R"(syntax = "proto3";

package recursive;

message Payload {
  oneof payload {
    .recursive.Message message = 1;
  }
}

message Message {
  string rule_name = 1;
  .recursive.Payload payload = 2;
})";

SEASTAR_THREAD_TEST_CASE(
  test_protobuf_compatibility_of_mutually_recursive_types) {
    BOOST_REQUIRE(check_compatible(
      pps::compatibility_level::full_transitive, recursive, recursive));
}
