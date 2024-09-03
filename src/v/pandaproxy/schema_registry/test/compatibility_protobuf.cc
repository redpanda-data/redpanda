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
#include "pandaproxy/schema_registry/test/compatibility_common.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/testing/thread_test_case.hh>

#include <absl/container/flat_hash_set.h>
#include <boost/test/unit_test.hpp>
#include <fmt/core.h>

#include <array>
#include <utility>

namespace pp = pandaproxy;
namespace pps = pp::schema_registry;

namespace {

struct simple_sharded_store {
    simple_sharded_store() {
        store.start(pps::is_mutable::yes, ss::default_smp_service_group())
          .get();
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
            schema.share(),
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
        pps::canonical_schema_definition{writer, pps::schema_type::protobuf}},
      pps::schema_version{1});
    return store.store
      .is_compatible(
        pps::schema_version{1},
        pps::canonical_schema{
          pps::subject{"sub"},
          pps::canonical_schema_definition{reader, pps::schema_type::protobuf}})
      .get();
}

pps::compatibility_result check_compatible_verbose(
  const pps::canonical_schema_definition& r,
  const pps::canonical_schema_definition& w) {
    pps::sharded_store s;
    return check_compatible(
      pps::make_protobuf_schema_definition(
        s, {pps::subject("r"), {r.shared_raw(), pps::schema_type::protobuf}})
        .get(),
      pps::make_protobuf_schema_definition(
        s, {pps::subject("w"), {w.shared_raw(), pps::schema_type::protobuf}})
        .get(),
      pps::verbose::yes);
}

} // namespace

SEASTAR_THREAD_TEST_CASE(test_protobuf_simple) {
    simple_sharded_store store;

    auto schema1 = pps::canonical_schema{
      pps::subject{"simple"}, simple.share()};
    store.insert(schema1, pps::schema_version{1});
    auto valid_simple = pps::make_protobuf_schema_definition(
                          store.store, schema1.share())
                          .get();
    BOOST_REQUIRE_EQUAL(valid_simple.name({0}).value(), "Simple");
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_nested) {
    simple_sharded_store store;

    auto schema1 = pps::canonical_schema{
      pps::subject{"nested"}, nested.share()};
    store.insert(schema1, pps::schema_version{1});
    auto valid_nested = pps::make_protobuf_schema_definition(
                          store.store, schema1.share())
                          .get();
    BOOST_REQUIRE_EQUAL(valid_nested.name({0}).value(), "A0");
    BOOST_REQUIRE_EQUAL(valid_nested.name({1, 0, 2}).value(), "A1.B0.C2");
    BOOST_REQUIRE_EQUAL(valid_nested.name({1, 0, 4}).value(), "A1.B0.C4");
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_imported_failure) {
    simple_sharded_store store;

    // imported depends on simple, which han't been inserted
    auto schema1 = pps::canonical_schema{
      pps::subject{"imported"}, imported.share()};
    store.insert(schema1, pps::schema_version{1});
    BOOST_REQUIRE_EXCEPTION(
      pps::make_protobuf_schema_definition(store.store, schema1.share()).get(),
      pps::exception,
      [](const pps::exception& ex) {
          return ex.code() == pps::error_code::schema_invalid;
      });
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_imported_not_referenced) {
    simple_sharded_store store;

    auto schema1 = pps::canonical_schema{
      pps::subject{"simple"}, simple.share()};
    auto schema2 = pps::canonical_schema{
      pps::subject{"imported"}, imported_no_ref.share()};

    store.insert(schema1, pps::schema_version{1});

    auto valid_simple = pps::make_protobuf_schema_definition(
                          store.store, schema1.share())
                          .get();
    BOOST_REQUIRE_EXCEPTION(
      pps::make_protobuf_schema_definition(store.store, schema2.share()).get(),
      pps::exception,
      [](const pps::exception& ex) {
          return ex.code() == pps::error_code::schema_invalid;
      });
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_referenced) {
    simple_sharded_store store;

    auto schema1 = pps::canonical_schema{
      pps::subject{"simple.proto"}, simple.share()};
    auto schema2 = pps::canonical_schema{
      pps::subject{"imported.proto"}, imported.share()};
    auto schema3 = pps::canonical_schema{
      pps::subject{"imported-again.proto"}, imported_again.share()};

    store.insert(schema1, pps::schema_version{1});
    store.insert(schema2, pps::schema_version{1});
    store.insert(schema3, pps::schema_version{1});

    auto valid_simple = pps::make_protobuf_schema_definition(
                          store.store, schema1.share())
                          .get();
    auto valid_imported = pps::make_protobuf_schema_definition(
                            store.store, schema2.share())
                            .get();
    auto valid_imported_again = pps::make_protobuf_schema_definition(
                                  store.store, schema3.share())
                                  .get();
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_recursive_reference) {
    simple_sharded_store store;

    auto schema1 = pps::canonical_schema{
      pps::subject{"simple.proto"}, simple.share()};
    auto schema2 = pps::canonical_schema{
      pps::subject{"imported.proto"}, imported.share()};
    auto schema3 = pps::canonical_schema{
      pps::subject{"imported-twice.proto"}, imported_twice.share()};

    store.insert(schema1, pps::schema_version{1});
    store.insert(schema2, pps::schema_version{1});
    store.insert(schema3, pps::schema_version{1});

    auto valid_simple = pps::make_protobuf_schema_definition(
                          store.store, schema1.share())
                          .get();
    auto valid_imported = pps::make_protobuf_schema_definition(
                            store.store, schema2.share())
                            .get();
    auto valid_imported_again = pps::make_protobuf_schema_definition(
                                  store.store, schema3.share())
                                  .get();
}

SEASTAR_THREAD_TEST_CASE(test_binary_protobuf) {
    simple_sharded_store store;

    BOOST_REQUIRE_NO_THROW(store.store
                             .make_valid_schema(pps::canonical_schema{
                               pps::subject{"com.redpanda.Payload.proto"},
                               pps::canonical_schema_definition{
                                 base64_raw_proto, pps::schema_type::protobuf}})
                             .get());
}

SEASTAR_THREAD_TEST_CASE(test_invalid_binary_protobuf) {
    simple_sharded_store store;

    auto broken_base64_raw_proto = base64_raw_proto.substr(1);

    auto schema = pps::canonical_schema{
      pps::subject{"com.redpanda.Payload.proto"},
      pps::canonical_schema_definition{
        broken_base64_raw_proto, pps::schema_type::protobuf}};

    BOOST_REQUIRE_EXCEPTION(
      store.store
        .make_valid_schema(pps::canonical_schema{
          pps::subject{"com.redpanda.Payload.proto"},
          pps::canonical_schema_definition{
            broken_base64_raw_proto, pps::schema_type::protobuf}})
        .get(),
      pps::exception,
      [](const pps::exception& e) {
          std::cout << e.what();
          return e.code() == pps::error_code::schema_invalid;
      });
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
      = pps::make_protobuf_schema_definition(store.store, schema.share()).get();
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

auto sanitize(std::string_view raw_proto) {
    simple_sharded_store s;
    return pps::make_canonical_protobuf_schema(
             s.store,
             pps::unparsed_schema{
               pps::subject{"foo"},
               pps::unparsed_schema_definition{
                 raw_proto, pps::schema_type::protobuf}})
      .get()
      .def()
      .raw()();
}

constexpr auto foobar_proto = R"(syntax = "proto3";
package foo;

import "google/protobuf/timestamp.proto";

message Bar {
  .google.protobuf.Timestamp timestamp = 1;
}
)";

SEASTAR_THREAD_TEST_CASE(test_protobuf_sanitize_strip_comments_and_newlines) {
    BOOST_REQUIRE_EQUAL(
      sanitize(R"(

/* comment */

syntax = "proto3";

/* comment */

package foo;

/* comment */

import "google/protobuf/timestamp.proto";

/* comment */

message Bar {
  google.protobuf.Timestamp timestamp = 1; //comment
}

/* comment */

)"),
      foobar_proto);
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_sanitize_ordering_no_newlines) {
    BOOST_REQUIRE_EQUAL(
      sanitize(R"(syntax = "proto3";
import "google/protobuf/timestamp.proto";
package foo;
message Bar {
  google.protobuf.Timestamp timestamp = 1; //comment
}
)"),
      foobar_proto);
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_sanitize_ordering_more_newlines) {
    BOOST_REQUIRE_EQUAL(
      sanitize(R"(

syntax = "proto3";


import "google/protobuf/timestamp.proto";


package foo;


message Bar {
  google.protobuf.Timestamp timestamp = 1; //comment
}

)"),
      foobar_proto);
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_sanitize_no_syntax) {
    BOOST_REQUIRE_EQUAL(
      sanitize(R"(
package foo;

import "google/protobuf/timestamp.proto";

message Bar {
  optional google.protobuf.Timestamp timestamp = 1; //comment
}
)"),
      R"(syntax = "proto2";
package foo;

import "google/protobuf/timestamp.proto";

message Bar {
  optional .google.protobuf.Timestamp timestamp = 1;
}
)");
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_sanitize_no_package) {
    BOOST_REQUIRE_EQUAL(
      sanitize(R"(syntax = "proto3";

import "google/protobuf/timestamp.proto";

message Bar {
  google.protobuf.Timestamp timestamp = 1; //comment
}
)"),
      R"(syntax = "proto3";

import "google/protobuf/timestamp.proto";
message Bar {
  .google.protobuf.Timestamp timestamp = 1;
}

)");
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_sanitize_no_syntax_package) {
    BOOST_REQUIRE_EQUAL(
      sanitize(R"(

import "google/protobuf/timestamp.proto";

message Bar {
  optional google.protobuf.Timestamp timestamp = 1; //comment
}
)"),
      R"(syntax = "proto2";

import "google/protobuf/timestamp.proto";
message Bar {
  optional .google.protobuf.Timestamp timestamp = 1;
}

)");
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_sanitize_no_imports) {
    BOOST_REQUIRE_EQUAL(
      sanitize(R"(syntax = "proto3";
package foo;

message Bar {
  int64 val = 1;
}
)"),
      R"(syntax = "proto3";

package foo;

message Bar {
  int64 val = 1;
}

)");
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_sanitize_multiple_imports) {
    BOOST_REQUIRE_EQUAL(
      sanitize(R"(syntax = "proto3";

package foo;

import "google/protobuf/timestamp.proto";
import "google/protobuf/any.proto";


message Bar {
  .google.protobuf.Timestamp timestamp = 1;
  .google.protobuf.Any any = 2;
}

)"),
      R"(syntax = "proto3";
package foo;

import "google/protobuf/timestamp.proto";
import "google/protobuf/any.proto";

message Bar {
  .google.protobuf.Timestamp timestamp = 1;
  .google.protobuf.Any any = 2;
}
)");
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_compatibility_message_removed) {
    BOOST_REQUIRE(!check_compatible(
      pps::compatibility_level::backward,
      R"(syntax = "proto3"; message Outer { message Inner { int32 id = 1;}; Inner x = 1; })",
      R"(syntax = "proto3"; message Outer { message Inner { int32 id = 1;}; message Inner2 { int32 id = 1;}; Inner x = 1; })"));
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_compatibility_field_name_type_changed) {
    BOOST_REQUIRE(!check_compatible(
      pps::compatibility_level::backward,
      R"(syntax = "proto3"; message Outer { message Inner { int32 id = 1;}; message Inner2 { int32 id = 1;}; Inner2 x = 1; })",
      R"(syntax = "proto3"; message Outer { message Inner { int32 id = 1;}; message Inner2 { int32 id = 1;}; Inner  x = 1; })"));
}

SEASTAR_THREAD_TEST_CASE(
  test_protobuf_compatibility_required_field_added_removed) {
    // field added
    BOOST_REQUIRE(!check_compatible(
      pps::compatibility_level::backward,
      R"(syntax = "proto2"; message Simple { optional int32 id = 1; required int32 new_id = 2; })",
      R"(syntax = "proto2"; message Simple { optional int32 id = 1; })"));
    // field removed
    BOOST_REQUIRE(!check_compatible(
      pps::compatibility_level::backward,
      R"(syntax = "proto2"; message Simple { optional int32 id = 1; })",
      R"(syntax = "proto2"; message Simple { optional int32 id = 1; required int32 new_id = 2; })"));
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_compatibility_field_made_reserved) {
    // required
    BOOST_REQUIRE(!check_compatible(
      pps::compatibility_level::backward,
      R"(syntax = "proto2"; message Simple { optional int32 id = 1; reserved 2; })",
      R"(syntax = "proto2"; message Simple { optional int32 id = 1; required int32 new_id = 2; })"));
    // not required
    BOOST_REQUIRE(check_compatible(
      pps::compatibility_level::backward,
      R"(syntax = "proto2"; message Simple { optional int32 id = 1; reserved 2; })",
      R"(syntax = "proto2"; message Simple { optional int32 id = 1; optional int32 new_id = 2; })"));
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_compatibility_field_unmade_reserved) {
    // required
    BOOST_REQUIRE(!check_compatible(
      pps::compatibility_level::backward,
      R"(syntax = "proto2"; message Simple { optional int32 id = 1; required int32 new_id = 2; })",
      R"(syntax = "proto2"; message Simple { optional int32 id = 1; reserved 2; })"));
    // not required
    BOOST_REQUIRE(check_compatible(
      pps::compatibility_level::backward,
      R"(syntax = "proto2"; message Simple { optional int32 id = 1; optional int32 new_id = 2; })",
      R"(syntax = "proto2"; message Simple { optional int32 id = 1; reserved 2; })"));
}

SEASTAR_THREAD_TEST_CASE(
  test_protobuf_compatibility_multiple_fields_moved_to_oneof) {
    BOOST_REQUIRE(check_compatible(
      pps::compatibility_level::backward,
      R"(syntax = "proto3"; message Simple { oneof wrapper { int32 id = 1; } })",
      R"(syntax = "proto3"; message Simple { int32 id = 1; })"));
    BOOST_REQUIRE(!check_compatible(
      pps::compatibility_level::backward,
      R"(syntax = "proto3"; message Simple { oneof wrapper { int32 id = 1; int32 new_id = 2; } })",
      R"(syntax = "proto3"; message Simple { int32 id = 1; int32 new_id = 2; })"));
}

SEASTAR_THREAD_TEST_CASE(
  test_protobuf_compatibility_fields_moved_out_of_oneof) {
    BOOST_REQUIRE(check_compatible(
      pps::compatibility_level::backward,
      R"(syntax = "proto3"; message Simple { int32 id = 1; int32 new_id = 2; })",
      R"(syntax = "proto3"; message Simple { oneof wrapper { int32 id = 1; int32 new_id = 2; } })"));
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_compatibility_oneof_field_removed) {
    BOOST_REQUIRE(!check_compatible(
      pps::compatibility_level::backward,
      R"(syntax = "proto3"; message Simple { oneof wrapper { int32 id = 1; } })",
      R"(syntax = "proto3"; message Simple { oneof wrapper { int32 id = 1; int32 new_id = 2; } })"));
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_compatibility_oneof_fully_removed) {
    BOOST_REQUIRE(check_compatible(
      pps::compatibility_level::backward,
      R"(syntax = "proto3"; message Simple { int32 other = 3; })",
      R"(syntax = "proto3"; message Simple { oneof wrapper { int32 id = 1; int32 new_id = 2; } int32 other = 3; })"));
}

namespace {

const pps::canonical_schema_definition proto2_old{
  R"(syntax = "proto2";

message someMessage {
  required int32 a = 1;
}

message myrecord {
  message Msg1 {
    required int32 f1 = 1;
  }
  message Msg2 {
     required int32 f1 = 1;
  }
  required Msg1 m1 = 1;
  required Msg1 m2 = 2;
  required int32 i1 = 3;

  required int32 i2 = 4;

  oneof union {
    int32 u1 = 5;
    string u2 = 6;
    bool u3 = 23;
    bool u4 = 40;
  }

  required int32 notu1 = 7;
  required string notu2 = 8;

})",
  pps::schema_type::protobuf};

const pps::canonical_schema_definition proto2_new{
  R"(syntax = "proto2";

message myrecord {
  message Msg1d {
    required int32 f1 = 1;
  }
  message Msg2 {
     required string f1 = 1;
  }
  required Msg1d m1 = 1;
  required int32 m2 = 2;
  required string i1 = 3;
  // required int32 i2 = 4;

  oneof union {
    int32 u1 = 5;
    string u2 = 16;
    string u3 = 23;
  }
  required bool u4 = 40;

  oneof union2 {
    int32 notu1 = 7;
    string notu2 = 8;
  }

  required string whoops = 12;
})",
  pps::schema_type::protobuf};

const pps::canonical_schema_definition proto3_old{
  R"(syntax = "proto3";

message someMessage {
   int32 a = 1;
}

message myrecord {
  message Msg1 {
     int32 f1 = 1;
  }
  message Msg2 {
     int32 f1 = 1;
  }
   Msg1 m1 = 1;
   Msg1 m2 = 2;
   int32 i1 = 3;

   int32 i2 = 4;

  oneof union {
    int32 u1 = 5;
    string u2 = 6;
    bool u3 = 23;
    bool u4 = 40;
  }

   int32 notu1 = 7;
   string notu2 = 8;

}
)",
  pps::schema_type::protobuf};

const pps::canonical_schema_definition proto3_new{
  R"(syntax = "proto3";

message myrecord {
  message Msg1d {
     int32 f1 = 1;
  }
  message Msg2 {
     string f1 = 1;
  }
   Msg1d m1 = 1;
   int32 m2 = 2;
   string i1 = 3;

  oneof union {
    int32 u1 = 5;
    string u2 = 16;
    string u3 = 23;
  }

  bool u4 = 40;

  oneof union2 {
    int32 notu1 = 7;
    string notu2 = 8;
  }

   string whoops = 12;
})",
  pps::schema_type::protobuf};

using incompatibility = pps::proto_incompatibility;

const absl::flat_hash_set<incompatibility> forward_expected{
  {"#/myrecord/union/16", incompatibility::Type::oneof_field_removed},
  {"#/myrecord/union/23", incompatibility::Type::field_scalar_kind_changed},
  {"#/myrecord/1", incompatibility::Type::field_named_type_changed},
  {"#/myrecord/2", incompatibility::Type::field_kind_changed},
  {"#/myrecord/3", incompatibility::Type::field_scalar_kind_changed},
  {"#/myrecord/Msg1d", incompatibility::Type::message_removed},
  {"#/myrecord/Msg2/1", incompatibility::Type::field_scalar_kind_changed},
  // These are ignored for proto3 schemas
  {"#/myrecord/4", incompatibility::Type::required_field_added},
  {"#/myrecord/7", incompatibility::Type::required_field_added},
  {"#/myrecord/8", incompatibility::Type::required_field_added},
  {"#/myrecord/12", incompatibility::Type::required_field_removed},
};

const absl::flat_hash_set<incompatibility> backward_expected{
  {"#/someMessage", incompatibility::Type::message_removed},
  {"#/myrecord/union2", incompatibility::Type::multiple_fields_moved_to_oneof},
  {"#/myrecord/union/6", incompatibility::Type::oneof_field_removed},
  {"#/myrecord/union/23", incompatibility::Type::field_scalar_kind_changed},
  {"#/myrecord/union/40", incompatibility::Type::oneof_field_removed},
  {"#/myrecord/1", incompatibility::Type::field_named_type_changed},
  {"#/myrecord/2", incompatibility::Type::field_kind_changed},
  {"#/myrecord/3", incompatibility::Type::field_scalar_kind_changed},
  {"#/myrecord/Msg1", incompatibility::Type::message_removed},
  {"#/myrecord/Msg2/1", incompatibility::Type::field_scalar_kind_changed},
  // These are ignored for proto3 schemas
  {"#/myrecord/4", incompatibility::Type::required_field_removed},
  {"#/myrecord/40", incompatibility::Type::required_field_added},
  {"#/myrecord/12", incompatibility::Type::required_field_added},
};

absl::flat_hash_set<incompatibility>
remove_proto2_incompatibilites(absl::flat_hash_set<incompatibility> exp) {
    absl::erase_if(exp, [](const auto& e) {
        return (
          e.type() == incompatibility::Type::required_field_removed
          || e.type() == incompatibility::Type::required_field_added);
    });
    return exp;
}

const auto compat_data = std::to_array<compat_test_data<incompatibility>>({
  {
    proto2_old.copy(),
    proto2_new.copy(),
    forward_expected,
  },
  {
    proto2_new.copy(),
    proto2_old.copy(),
    backward_expected,
  },
  {
    proto3_old.copy(),
    proto3_new.copy(),
    remove_proto2_incompatibilites(forward_expected),
  },
  {
    proto3_new.copy(),
    proto3_old.copy(),
    remove_proto2_incompatibilites(backward_expected),
  },
});

std::string format_set(const absl::flat_hash_set<ss::sstring>& d) {
    return fmt::format("{}", fmt::join(d, "\n"));
}

} // namespace

SEASTAR_THREAD_TEST_CASE(test_protobuf_compat_messages) {
    for (const auto& cd : compat_data) {
        auto compat = check_compatible_verbose(cd.reader, cd.writer);
        absl::flat_hash_set<ss::sstring> errs{
          compat.messages.begin(), compat.messages.end()};
        absl::flat_hash_set<ss::sstring> expected{
          cd.expected.messages.begin(), cd.expected.messages.end()};
        BOOST_CHECK(!compat.is_compat);
        BOOST_CHECK_EQUAL(errs.size(), expected.size());
        BOOST_REQUIRE_MESSAGE(
          errs == expected,
          fmt::format("{} != {}", format_set(errs), format_set(expected)));
    }
}
