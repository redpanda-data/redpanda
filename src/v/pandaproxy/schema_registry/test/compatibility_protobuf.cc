// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/test/compatibility_protobuf.h"

#include "absl/container/flat_hash_set.h"
#include "bytes/iobuf_parser.h"
#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/exceptions.h"
#include "pandaproxy/schema_registry/protobuf.h"
#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/test/compatibility_common.h"
#include "pandaproxy/schema_registry/test/protobuf_utils.h"
#include "pandaproxy/schema_registry/test/store_fixture.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>
#include <fmt/core.h>

#include <array>
#include <utility>

namespace pp = pandaproxy;
namespace pps = pp::schema_registry;
namespace ppstu = pp::schema_registry::test_utils;

namespace {

bool check_compatible(
  pps::compatibility_level lvl,
  std::string_view reader,
  std::string_view writer) {
    ppstu::store_fixture store;
    auto dummy_marker = pps::seq_marker{};
    store.store()
      .set_compatibility(dummy_marker, pps::default_context, lvl)
      .get();
    store.insert(
      pandaproxy::schema_registry::subject_schema{
        pps::subject{"sub"},
        pps::schema_definition{writer, pps::schema_type::protobuf}},
      pps::schema_version{1});
    return store.store()
      .is_compatible(
        pps::schema_version{1},
        pps::subject_schema{
          pps::subject{"sub"},
          pps::schema_definition{reader, pps::schema_type::protobuf}})
      .get();
}

pps::compatibility_result check_compatible_verbose(
  const pps::schema_definition& r, const pps::schema_definition& w) {
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
    ppstu::store_fixture store;

    auto schema1 = pps::subject_schema{pps::subject{"simple"}, simple.share()};
    store.insert(schema1.share(), pps::schema_version{1});
    auto valid_simple = pps::make_protobuf_schema_definition(
                          store.store(), schema1.share())
                          .get();
    BOOST_REQUIRE_EQUAL(valid_simple.name({0}).value(), "Simple");
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_nested) {
    ppstu::store_fixture store;

    auto schema1 = pps::subject_schema{pps::subject{"nested"}, nested.share()};
    store.insert(schema1.share(), pps::schema_version{1});
    auto valid_nested = pps::make_protobuf_schema_definition(
                          store.store(), schema1.share())
                          .get();
    BOOST_REQUIRE_EQUAL(valid_nested.name({0}).value(), "A0");
    BOOST_REQUIRE_EQUAL(valid_nested.name({1, 0, 2}).value(), "A1.B0.C2");
    BOOST_REQUIRE_EQUAL(valid_nested.name({1, 0, 4}).value(), "A1.B0.C4");
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_imported_failure) {
    ppstu::store_fixture store;

    // imported depends on simple, which han't been inserted
    auto schema1 = pps::subject_schema{
      pps::subject{"imported"}, imported.share()};
    store.insert(schema1.share(), pps::schema_version{1});
    BOOST_REQUIRE_EXCEPTION(
      pps::make_protobuf_schema_definition(store.store(), schema1.share())
        .get(),
      pps::exception,
      [](const pps::exception& ex) {
          return ex.code() == pps::error_code::schema_missing_reference
                 && std::string_view(ex.message())
                      .contains("No schema reference found for subject");
      });
}

// Setup: A -> B -> C (missing). Error should mention that the subject
// "subject-for-B" is missing the reference to C.
SEASTAR_THREAD_TEST_CASE(test_protobuf_missing_nested_reference_error_subject) {
    ppstu::store_fixture store;

    auto schema_b = pps::subject_schema{
      pps::subject{"subject-for-B"},
      pps::schema_definition{
        R"(syntax = "proto3"; import "c.proto"; message B { C c = 1; })",
        pps::schema_type::protobuf,
        {{"c.proto", pps::subject{"subject-for-C"}, pps::schema_version{1}}},
        {}}};

    auto schema_a = pps::subject_schema{
      pps::subject{"subject-for-A"},
      pps::schema_definition{
        R"(syntax = "proto3"; import "b.proto"; message A { B b = 1; })",
        pps::schema_type::protobuf,
        {{"b.proto", pps::subject{"subject-for-B"}, pps::schema_version{1}}},
        {}}};

    store.insert(schema_b.share(), pps::schema_version{1});

    BOOST_REQUIRE_EXCEPTION(
      pps::make_protobuf_schema_definition(store.store(), schema_a.share())
        .get(),
      pps::exception,
      [](const pps::exception& ex) {
          auto msg = std::string_view(ex.message());
          return ex.code() == pps::error_code::schema_missing_reference
                 && msg.contains("subject=subject-for-B")
                 && msg.contains("subject \"subject-for-C\"");
      });
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_imported_not_referenced) {
    ppstu::store_fixture store;

    auto schema1 = pps::subject_schema{pps::subject{"simple"}, simple.share()};
    auto schema2 = pps::subject_schema{
      pps::subject{"imported"}, imported_no_ref.share()};

    store.insert(schema1.share(), pps::schema_version{1});

    auto valid_simple = pps::make_protobuf_schema_definition(
                          store.store(), schema1.share())
                          .get();
    BOOST_REQUIRE_EXCEPTION(
      pps::make_protobuf_schema_definition(store.store(), schema2.share())
        .get(),
      pps::exception,
      [](const pps::exception& ex) {
          return ex.code() == pps::error_code::schema_invalid;
      });
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_referenced) {
    ppstu::store_fixture store;

    auto schema1 = pps::subject_schema{
      pps::subject{"simple.proto"}, simple.share()};
    auto schema2 = pps::subject_schema{
      pps::subject{"imported.proto"}, imported.share()};
    auto schema3 = pps::subject_schema{
      pps::subject{"imported-again.proto"}, imported_again.share()};

    store.insert(schema1.share(), pps::schema_version{1});
    store.insert(schema2.share(), pps::schema_version{1});
    store.insert(schema3.share(), pps::schema_version{1});

    auto valid_simple = pps::make_protobuf_schema_definition(
                          store.store(), schema1.share())
                          .get();
    auto valid_imported = pps::make_protobuf_schema_definition(
                            store.store(), schema2.share())
                            .get();
    auto valid_imported_again = pps::make_protobuf_schema_definition(
                                  store.store(), schema3.share())
                                  .get();
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_recursive_reference) {
    ppstu::store_fixture store;

    auto schema1 = pps::subject_schema{
      pps::subject{"simple.proto"}, simple.share()};
    auto schema2 = pps::subject_schema{
      pps::subject{"imported.proto"}, imported.share()};
    auto schema3 = pps::subject_schema{
      pps::subject{"imported-twice.proto"}, imported_twice.share()};

    store.insert(schema1.share(), pps::schema_version{1});
    store.insert(schema2.share(), pps::schema_version{1});
    store.insert(schema3.share(), pps::schema_version{1});

    auto valid_simple = pps::make_protobuf_schema_definition(
                          store.store(), schema1.share())
                          .get();
    auto valid_imported = pps::make_protobuf_schema_definition(
                            store.store(), schema2.share())
                            .get();
    auto valid_imported_again = pps::make_protobuf_schema_definition(
                                  store.store(), schema3.share())
                                  .get();
}

SEASTAR_THREAD_TEST_CASE(test_binary_protobuf) {
    ppstu::store_fixture store;

    BOOST_REQUIRE_NO_THROW(
      store.store()
        .make_valid_schema(
          pps::subject_schema{
            pps::subject{"com.redpanda.Payload.proto"},
            pps::schema_definition{
              base64_raw_proto, pps::schema_type::protobuf}})
        .get());
}

SEASTAR_THREAD_TEST_CASE(test_invalid_binary_protobuf) {
    ppstu::store_fixture store;

    auto broken_base64_raw_proto = base64_raw_proto.substr(1);

    auto schema = pps::subject_schema{
      pps::subject{"com.redpanda.Payload.proto"},
      pps::schema_definition{
        broken_base64_raw_proto, pps::schema_type::protobuf}};

    BOOST_REQUIRE_EXCEPTION(
      store.store()
        .make_valid_schema(
          pps::subject_schema{
            pps::subject{"com.redpanda.Payload.proto"},
            pps::schema_definition{
              broken_base64_raw_proto, pps::schema_type::protobuf}})
        .get(),
      pps::exception,
      [](const pps::exception& e) {
          std::cout << e.what();
          return e.code() == pps::error_code::schema_invalid;
      });
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_well_known) {
    ppstu::store_fixture store;

    auto schema = pps::subject_schema{
      pps::subject{"test_auto_well_known"},
      pps::schema_definition{
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
import "buf/validate/validate.proto";

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
  buf.validate.FieldRules field_rules = 49;
  buf.validate.StringRules string_rules = 50;
  buf.validate.Int32Rules int32_rules = 51;
  buf.validate.MessageRules message_rules = 52;
  buf.validate.RepeatedRules repeated_rules = 53;
  buf.validate.MapRules map_rules = 54;
  buf.validate.AnyRules any_rules = 55;
  buf.validate.DurationRules duration_rules = 56;
  buf.validate.TimestampRules timestamp_rules = 57;
})",
        pps::schema_type::protobuf}};
    store.insert(schema.share(), pps::schema_version{1});

    auto valid_empty = pps::make_protobuf_schema_definition(
                         store.store(), schema.share())
                         .get();
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

constexpr auto foobar_proto = R"(syntax = "proto3";
package foo;

import "google/protobuf/timestamp.proto";

message Bar {
  .google.protobuf.Timestamp timestamp = 1;
}
)";

SEASTAR_THREAD_TEST_CASE(test_protobuf_sanitize_strip_comments_and_newlines) {
    BOOST_REQUIRE_EQUAL(
      ppstu::sanitize(R"(

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
      ppstu::sanitize(R"(syntax = "proto3";
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
      ppstu::sanitize(R"(

syntax = "proto3";


import "google/protobuf/timestamp.proto";


package foo;


message Bar {
  google.protobuf.Timestamp timestamp = 1; //comment
}

)"),
      foobar_proto);
}

// proto file heavily inspired from
// https://protobuf.dev/programming-guides/proto2/#customoptions
SEASTAR_THREAD_TEST_CASE(test_protobuf_normalize_custom_options) {
    auto schema = R"(import "google/protobuf/descriptor.proto";

extend google.protobuf.FileOptions {
  optional string my_file_option_b = 50008 [(my_field_option_b) = 5.5, (my_field_option_a) = 4.5];
  optional string my_file_option_a = 50000;
  repeated uint32 my_repeated_file_option = 60000;
}
extend google.protobuf.MessageOptions {
  optional int32 my_message_option_b = 50009;
  optional int32 my_message_option_a = 50001;
}
extend google.protobuf.FileOptions {
  optional string my_file_option_c = 50015;
}
extend google.protobuf.FieldOptions {
  optional float my_field_option_b = 50010;
  optional float my_field_option_a = 50002;
}
extend google.protobuf.OneofOptions {
  optional int64 my_oneof_option_b = 50011;
  optional int64 my_oneof_option_a = 50003;
}
extend google.protobuf.EnumOptions {
  optional bool my_enum_option_b = 50012;
  optional bool my_enum_option_a = 50004;
}
extend google.protobuf.EnumValueOptions {
  optional uint32 my_enum_value_option_b = 50013;
  optional uint32 my_enum_value_option_a = 50005;
}
extend google.protobuf.ServiceOptions {
  optional MyEnum my_service_option_b = 50014;
  optional MyEnum my_service_option_a = 50006;
}
extend google.protobuf.MethodOptions {
  optional MyMessage my_method_option_b = 50015;
  optional MyMessage my_method_option_a = 50007;
}

option (my_repeated_file_option) = 2;
option (my_file_option_b) = "Some other string";
option (my_repeated_file_option) = 1;
option (my_repeated_file_option) = 3;
option (my_file_option_a) = "Hello world!";

message MyMessage {
  option (my_message_option_b) = 2345;
  option (my_message_option_a) = 1234;

  optional int32 foo = 1 [(my_field_option_b) = 5.5, (my_field_option_a) = 4.5];
  optional string bar = 2;
  oneof qux {
    option (my_oneof_option_b) = 43;
    option (my_oneof_option_a) = 42;

    string quux = 3;
  }
}

enum MyEnum {
  option (my_enum_option_b) = false;
  option (my_enum_option_a) = true;

  FOO = 1 [(my_enum_value_option_b) = 432, (my_enum_value_option_a) = 321];
  BAR = 2;
}

message RequestType {}
message ResponseType {}

service MyService {
  option (my_service_option_b) = BAR;
  option (my_service_option_a) = FOO;

  rpc MyMethod(RequestType) returns(ResponseType) {
    // Note:  my_method_option_a has type MyMessage.  We can set each field
    //   within it using a separate "option" line.
    option (my_method_option_b).bar = "Some other string";
    option (my_method_option_b).foo = 678;
    option (my_method_option_a).foo = 567;
    option (my_method_option_a).bar = "Some string";
  }
}
)";

    auto sanitized = R"(syntax = "proto2";

import "google/protobuf/descriptor.proto";
option (.my_file_option_a) = "Hello world!";
option (.my_file_option_b) = "Some other string";
option (.my_repeated_file_option) = 2;
option (.my_repeated_file_option) = 1;
option (.my_repeated_file_option) = 3;

enum MyEnum {
  option (.my_enum_option_a) = true;
  option (.my_enum_option_b) = false;
  FOO = 1 [(.my_enum_value_option_a) = 321, (.my_enum_value_option_b) = 432];
  BAR = 2;
}

message MyMessage {
  option (.my_message_option_a) = 1234;
  option (.my_message_option_b) = 2345;
  optional int32 foo = 1 [(.my_field_option_a) = 4.5, (.my_field_option_b) = 5.5];
  optional string bar = 2;
  oneof qux {    option (.my_oneof_option_a) = 42;
    option (.my_oneof_option_b) = 43;

    string quux = 3;
  }
}

message RequestType {
}

message ResponseType {
}

service MyService {
  option (.my_service_option_a) = FOO;
  option (.my_service_option_b) = BAR;
  rpc MyMethod(.RequestType) returns (.ResponseType) {
    option (.my_method_option_a) = {
      foo: 567
      bar: "Some string"
    };
    option (.my_method_option_b) = {
      foo: 678
      bar: "Some other string"
    };
  }
}

extend .google.protobuf.FileOptions {
  optional string my_file_option_b = 50008 [(.my_field_option_a) = 4.5, (.my_field_option_b) = 5.5];
  optional string my_file_option_a = 50000;
  repeated uint32 my_repeated_file_option = 60000;
}

extend .google.protobuf.MessageOptions {
  optional int32 my_message_option_b = 50009;
  optional int32 my_message_option_a = 50001;
}

extend .google.protobuf.FileOptions {
  optional string my_file_option_c = 50015;
}

extend .google.protobuf.FieldOptions {
  optional float my_field_option_b = 50010;
  optional float my_field_option_a = 50002;
}

extend .google.protobuf.OneofOptions {
  optional int64 my_oneof_option_b = 50011;
  optional int64 my_oneof_option_a = 50003;
}

extend .google.protobuf.EnumOptions {
  optional bool my_enum_option_b = 50012;
  optional bool my_enum_option_a = 50004;
}

extend .google.protobuf.EnumValueOptions {
  optional uint32 my_enum_value_option_b = 50013;
  optional uint32 my_enum_value_option_a = 50005;
}

extend .google.protobuf.ServiceOptions {
  optional .MyEnum my_service_option_b = 50014;
  optional .MyEnum my_service_option_a = 50006;
}

extend .google.protobuf.MethodOptions {
  optional .MyMessage my_method_option_b = 50015;
  optional .MyMessage my_method_option_a = 50007;
}

)";

    auto normalized = R"(syntax = "proto2";

import "google/protobuf/descriptor.proto";
option (.my_file_option_a) = "Hello world!";
option (.my_file_option_b) = "Some other string";
option (.my_repeated_file_option) = 2;
option (.my_repeated_file_option) = 1;
option (.my_repeated_file_option) = 3;

enum MyEnum {
  option (.my_enum_option_a) = true;
  option (.my_enum_option_b) = false;
  FOO = 1 [(.my_enum_value_option_a) = 321, (.my_enum_value_option_b) = 432];
  BAR = 2;
}

message MyMessage {
  option (.my_message_option_a) = 1234;
  option (.my_message_option_b) = 2345;
  optional int32 foo = 1 [(.my_field_option_a) = 4.5, (.my_field_option_b) = 5.5];
  optional string bar = 2;
  oneof qux {    option (.my_oneof_option_a) = 42;
    option (.my_oneof_option_b) = 43;

    string quux = 3;
  }
}

message RequestType {
}

message ResponseType {
}

service MyService {
  option (.my_service_option_a) = FOO;
  option (.my_service_option_b) = BAR;
  rpc MyMethod(.RequestType) returns (.ResponseType) {
    option (.my_method_option_a) = {
      foo: 567
      bar: "Some string"
    };
    option (.my_method_option_b) = {
      foo: 678
      bar: "Some other string"
    };
  }
}

extend .google.protobuf.EnumOptions {
  optional bool my_enum_option_a = 50004;
  optional bool my_enum_option_b = 50012;
}

extend .google.protobuf.EnumValueOptions {
  optional uint32 my_enum_value_option_a = 50005;
  optional uint32 my_enum_value_option_b = 50013;
}

extend .google.protobuf.FieldOptions {
  optional float my_field_option_a = 50002;
  optional float my_field_option_b = 50010;
}

extend .google.protobuf.FileOptions {
  optional string my_file_option_a = 50000;
  optional string my_file_option_b = 50008 [(.my_field_option_a) = 4.5, (.my_field_option_b) = 5.5];
  optional string my_file_option_c = 50015;
  repeated uint32 my_repeated_file_option = 60000;
}

extend .google.protobuf.MessageOptions {
  optional int32 my_message_option_a = 50001;
  optional int32 my_message_option_b = 50009;
}

extend .google.protobuf.MethodOptions {
  optional .MyMessage my_method_option_a = 50007;
  optional .MyMessage my_method_option_b = 50015;
}

extend .google.protobuf.OneofOptions {
  optional int64 my_oneof_option_a = 50003;
  optional int64 my_oneof_option_b = 50011;
}

extend .google.protobuf.ServiceOptions {
  optional .MyEnum my_service_option_a = 50006;
  optional .MyEnum my_service_option_b = 50014;
}

)";
    BOOST_CHECK_EQUAL(ppstu::sanitize(schema), sanitized);
    BOOST_CHECK_EQUAL(ppstu::normalize(schema), normalized);
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_normalize_nested_custom_options) {
    auto schema = R"(import "google/protobuf/descriptor.proto";

extend google.protobuf.MessageOptions {
  optional int32 my_message_option = 50001;
}
extend google.protobuf.FieldOptions {
  optional float my_field_option = 50002;
}
extend google.protobuf.EnumOptions {
  optional bool my_enum_option = 50004;
}
extend google.protobuf.EnumValueOptions {
  optional uint32 my_enum_value_option = 50005;
}

message MyMessage {
  option (my_message_option) = 1234;

  optional int32 foo = 1 [(my_field_option) = 4.5];
  optional NestedMessage nested_msg = 2;
  optional NestedEnum nested_enum = 3;

  enum NestedEnum {
    option (my_enum_option) = true;

    FOO = 1 [(my_enum_value_option) = 432];
    BAR = 2;
  }
  message NestedMessage {
    option (my_message_option) = 2345;

    optional int32 foo = 1 [(my_field_option) = 6.5];
    optional string bar = 2;
  }
}

)";

    auto sanitized = R"(syntax = "proto2";

import "google/protobuf/descriptor.proto";
message MyMessage {
  option (.my_message_option) = 1234;
  message NestedMessage {
    option (.my_message_option) = 2345;
    optional int32 foo = 1 [(.my_field_option) = 6.5];
    optional string bar = 2;
  }
  enum NestedEnum {
    option (.my_enum_option) = true;
    FOO = 1 [(.my_enum_value_option) = 432];
    BAR = 2;
  }
  optional int32 foo = 1 [(.my_field_option) = 4.5];
  optional .MyMessage.NestedMessage nested_msg = 2;
  optional .MyMessage.NestedEnum nested_enum = 3;
}

extend .google.protobuf.MessageOptions {
  optional int32 my_message_option = 50001;
}

extend .google.protobuf.FieldOptions {
  optional float my_field_option = 50002;
}

extend .google.protobuf.EnumOptions {
  optional bool my_enum_option = 50004;
}

extend .google.protobuf.EnumValueOptions {
  optional uint32 my_enum_value_option = 50005;
}

)";

    auto normalized = R"(syntax = "proto2";

import "google/protobuf/descriptor.proto";
message MyMessage {
  option (.my_message_option) = 1234;
  message NestedMessage {
    option (.my_message_option) = 2345;
    optional int32 foo = 1 [(.my_field_option) = 6.5];
    optional string bar = 2;
  }
  enum NestedEnum {
    option (.my_enum_option) = true;
    FOO = 1 [(.my_enum_value_option) = 432];
    BAR = 2;
  }
  optional int32 foo = 1 [(.my_field_option) = 4.5];
  optional .MyMessage.NestedMessage nested_msg = 2;
  optional .MyMessage.NestedEnum nested_enum = 3;
}

extend .google.protobuf.EnumOptions {
  optional bool my_enum_option = 50004;
}

extend .google.protobuf.EnumValueOptions {
  optional uint32 my_enum_value_option = 50005;
}

extend .google.protobuf.FieldOptions {
  optional float my_field_option = 50002;
}

extend .google.protobuf.MessageOptions {
  optional int32 my_message_option = 50001;
}

)";
    BOOST_CHECK_EQUAL(ppstu::sanitize(schema), sanitized);
    BOOST_CHECK_EQUAL(ppstu::normalize(schema), normalized);
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_normalize_message_custom_options) {
    const auto schema = R"(syntax = "proto3";

import "google/protobuf/descriptor.proto";

enum MyEnum {
  VALUE_0 = 0;
  VALUE_1 = 1 [(metadata) = {
    some_bool: true,
    some_string: "test_string"
  }];
}

message Metadata {
    bool some_bool = 1;
    string some_string = 2;
}

extend google.protobuf.EnumValueOptions {
  Metadata metadata = 50001;
}

)";

    const auto sanitized = R"(syntax = "proto3";

import "google/protobuf/descriptor.proto";
enum MyEnum {
  VALUE_0 = 0;
  VALUE_1 = 1 [(.metadata) = {
    some_bool: true
    some_string: "test_string"
  }];
}

message Metadata {
  bool some_bool = 1;
  string some_string = 2;
}

extend .google.protobuf.EnumValueOptions {
  .Metadata metadata = 50001;
}

)";

    const auto normalized = R"(syntax = "proto3";

import "google/protobuf/descriptor.proto";
enum MyEnum {
  VALUE_0 = 0;
  VALUE_1 = 1 [(.metadata) = {
    some_bool: true
    some_string: "test_string"
  }];
}

message Metadata {
  bool some_bool = 1;
  string some_string = 2;
}

extend .google.protobuf.EnumValueOptions {
  .Metadata metadata = 50001;
}

)";

    BOOST_REQUIRE_EQUAL(ppstu::sanitize(schema), sanitized);
    BOOST_CHECK_EQUAL(ppstu::normalize(schema), normalized);
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_normalize_extension_ranges) {
    const auto schema = R"(syntax = "proto2";

import "google/protobuf/descriptor.proto";

message SimpleMessage {
  optional int32 foo = 1;
}
message ExtendableMessage {
  extensions 1 to 9;
  extensions 11 to 99 [verification = UNVERIFIED];
  extensions 111 to 222 [verification = DECLARATION];
  extensions 333 to 444 [
    declaration = {
      number: 334,
      full_name: ".some_int",
      type: "int32",
      repeated: true
    }
  ];
  extensions 555 to 666 [
    declaration = { full_name: ".some_other_int32", type: "int32", number: 555 },
    declaration = { full_name: ".some_double", type: "double", number: 556, reserved: true },
    declaration = { full_name: ".my_message", type: ".SimpleMessage", number: 557, reserved: true, repeated: false }
  ];
  extensions 777 [(my_range_option_b) = "some value", (my_range_option_a) = "some other value"];
}
extend ExtendableMessage {
  optional int32 some_int = 3;
}
extend google.protobuf.ExtensionRangeOptions {
  optional string my_range_option_b = 50008;
  optional string my_range_option_a = 50000;
}


)";

    const auto sanitized = R"(syntax = "proto2";

import "google/protobuf/descriptor.proto";
message SimpleMessage {
  optional int32 foo = 1;
}

message ExtendableMessage {
  extensions 1 to 9;
  extensions 11 to 99 [verification = UNVERIFIED];
  extensions 111 to 222 [verification = DECLARATION];
  extensions 333 to 444 [declaration = {
    number: 334
    full_name: ".some_int"
    type: "int32"
    repeated: true
  }];
  extensions 555 to 666 [declaration = {
    number: 555
    full_name: ".some_other_int32"
    type: "int32"
  }, declaration = {
    number: 556
    full_name: ".some_double"
    type: "double"
    reserved: true
  }, declaration = {
    number: 557
    full_name: ".my_message"
    type: ".SimpleMessage"
    reserved: true
    repeated: false
  }];
  extensions 777 [(.my_range_option_a) = "some other value", (.my_range_option_b) = "some value"];
}

extend .ExtendableMessage {
  optional int32 some_int = 3;
}

extend .google.protobuf.ExtensionRangeOptions {
  optional string my_range_option_b = 50008;
  optional string my_range_option_a = 50000;
}

)";

    const auto normalized = R"(syntax = "proto2";

import "google/protobuf/descriptor.proto";
message SimpleMessage {
  optional int32 foo = 1;
}

message ExtendableMessage {
  extensions 1 to 9;
  extensions 11 to 99 [verification = UNVERIFIED];
  extensions 111 to 222 [verification = DECLARATION];
  extensions 333 to 444 [declaration = {
    number: 334
    full_name: ".some_int"
    type: "int32"
    repeated: true
  }];
  extensions 555 to 666 [declaration = {
    number: 555
    full_name: ".some_other_int32"
    type: "int32"
  }, declaration = {
    number: 556
    full_name: ".some_double"
    type: "double"
    reserved: true
  }, declaration = {
    number: 557
    full_name: ".my_message"
    type: ".SimpleMessage"
    reserved: true
    repeated: false
  }];
  extensions 777 [(.my_range_option_a) = "some other value", (.my_range_option_b) = "some value"];
}

extend .ExtendableMessage {
  optional int32 some_int = 3;
}

extend .google.protobuf.ExtensionRangeOptions {
  optional string my_range_option_a = 50000;
  optional string my_range_option_b = 50008;
}

)";

    BOOST_CHECK_EQUAL(ppstu::sanitize(schema), sanitized);
    BOOST_CHECK_EQUAL(ppstu::normalize(schema), normalized);
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_sanitize_no_syntax) {
    BOOST_REQUIRE_EQUAL(
      ppstu::sanitize(R"(
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
      ppstu::sanitize(R"(syntax = "proto3";

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
      ppstu::sanitize(R"(

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
      ppstu::sanitize(R"(syntax = "proto3";
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
      ppstu::sanitize(R"(syntax = "proto3";

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

SEASTAR_THREAD_TEST_CASE(test_protobuf_normalize_imports) {
    auto schema = R"(syntax = "proto3";
package foo;
// sanitize should maintain relative ordering of imports per group,
// normalize should sort them
import "google/protobuf/timestamp.proto";
import public "google/protobuf/duration.proto";
import weak "google/protobuf/any.proto";
import "google/protobuf/api.proto";
)";

    BOOST_CHECK_EQUAL(ppstu::sanitize(schema), (R"(syntax = "proto3";
package foo;

import "google/protobuf/timestamp.proto";
import "google/protobuf/api.proto";
import weak "google/protobuf/any.proto";
import public "google/protobuf/duration.proto";


)"));
    BOOST_CHECK_EQUAL(ppstu::normalize(schema), (R"(syntax = "proto3";
package foo;

import "google/protobuf/api.proto";
import "google/protobuf/timestamp.proto";
import weak "google/protobuf/any.proto";
import public "google/protobuf/duration.proto";


)"));
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_normalize_map) {
    auto schema = R"(syntax = "proto3";
package foo;
import "google/protobuf/struct.proto";
import "google/protobuf/any.proto";
message Value {
  google.protobuf.Any any = 1;
}
message HasMap {
  map<string, Value> map_string_value = 1;
}
message HasGoogleMap {
  map<string, google.protobuf.Value> map_string_value = 1;
}
)";

    BOOST_CHECK_EQUAL(ppstu::sanitize(schema), (R"(syntax = "proto3";
package foo;

import "google/protobuf/struct.proto";
import "google/protobuf/any.proto";

message Value {
  .google.protobuf.Any any = 1;
}

message HasMap {
  map<string, .foo.Value> map_string_value = 1;
}

message HasGoogleMap {
  map<string, .google.protobuf.Value> map_string_value = 1;
}
)"));
    BOOST_CHECK_EQUAL(ppstu::normalize(schema), (R"(syntax = "proto3";
package foo;

import "google/protobuf/any.proto";
import "google/protobuf/struct.proto";

message Value {
  .google.protobuf.Any any = 1;
}

message HasMap {
  map<string, .foo.Value> map_string_value = 1;
}

message HasGoogleMap {
  map<string, .google.protobuf.Value> map_string_value = 1;
}
)"));
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_normalize_legacy_map) {
    auto schema = R"(syntax = "proto3";

import "google/protobuf/timestamp.proto";

message Value {
   string string = 1;
}

message HasMap {
  repeated PropertiesEntry properties = 1;
  repeated TimestampsEntry timestamps = 2;

  message PropertiesEntry {
    option map_entry = true;
    string key = 1;
    Value value = 2;
  }
  message TimestampsEntry {
    option map_entry = true;
    string key = 1;
    google.protobuf.Timestamp value = 2;
  }
}
)";

    BOOST_CHECK_EQUAL(ppstu::sanitize(schema), (R"(syntax = "proto3";

import "google/protobuf/timestamp.proto";
message Value {
  string string = 1;
}

message HasMap {
  map<string, .Value> properties = 1;
  map<string, .google.protobuf.Timestamp> timestamps = 2;
}

)"));
    BOOST_CHECK_EQUAL(ppstu::normalize(schema), (R"(syntax = "proto3";

import "google/protobuf/timestamp.proto";
message Value {
  string string = 1;
}

message HasMap {
  map<string, .Value> properties = 1;
  map<string, .google.protobuf.Timestamp> timestamps = 2;
}

)"));
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_normalize_group) {
    auto schema = R"(syntax = "proto2";
message SearchResponse {
  repeated group Result = 1 {
    optional string title = 2;
    optional string url = 1;
    repeated string snippets = 3;
    message SomeMessage {
      optional string string = 1;
    }
    optional SomeMessage msg = 4;
    repeated group InnerGroup = 5 {
      optional int32 int32 = 1;
    }
    oneof nested_oneof {
      string name = 6;
    }
  }
})";

    BOOST_CHECK_EQUAL(ppstu::sanitize(schema), (R"(syntax = "proto2";

message SearchResponse {
  repeated group Result = 1 {
    message SomeMessage {
      optional string string = 1;
    }
    optional string title = 2;
    optional string url = 1;
    repeated string snippets = 3;
    optional .SearchResponse.Result.SomeMessage msg = 4;
    repeated group InnerGroup = 5 {
      optional int32 int32 = 1;
    }
    oneof nested_oneof {
      string name = 6;
    }
  }
}

)"));
    BOOST_CHECK_EQUAL(ppstu::normalize(schema), (R"(syntax = "proto2";

message SearchResponse {
  repeated group Result = 1 {
    message SomeMessage {
      optional string string = 1;
    }
    optional string url = 1;
    optional string title = 2;
    repeated string snippets = 3;
    optional .SearchResponse.Result.SomeMessage msg = 4;
    repeated group InnerGroup = 5 {
      optional int32 int32 = 1;
    }
    oneof nested_oneof {
      string name = 6;
    }
  }
}

)"));
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_synthetic_oneof) {
    auto schema = R"(syntax = "proto3";
package foo;
message WithSynthetic {
  optional int32 int32 = 1;
}

message WithOneOf {
  oneof some_int {
    int32 int32 = 1;
  }
}

)";
    auto expected_sanitized = R"(syntax = "proto3";

package foo;

message WithSynthetic {
  optional int32 int32 = 1;
}

message WithOneOf {
  oneof some_int {
    int32 int32 = 1;
  }
}

)";
    BOOST_CHECK_EQUAL(ppstu::sanitize(schema), expected_sanitized);
    auto expected_normalized = R"(syntax = "proto3";

package foo;

message WithSynthetic {
  optional int32 int32 = 1;
}

message WithOneOf {
  oneof some_int {
    int32 int32 = 1;
  }
}

)";
    BOOST_CHECK_EQUAL(ppstu::normalize(schema), expected_normalized);
}

SEASTAR_THREAD_TEST_CASE(test_protobuf_normalize) {
    auto schema = R"(
syntax = "proto3";

package foo;

option java_package = "com.example.foo";
option java_outer_classname = "FooService";
option optimize_for = SPEED;
option go_package = "foo.example.com/fooservice";
option cc_enable_arenas = true;
option objc_class_prefix = "FS";
option csharp_namespace = "Foo.FooService";
option php_namespace = "my_php\ns";


// public should come last
import public "google/protobuf/duration.proto";
// imports should be sorted
import "google/protobuf/timestamp.proto";
import "google/protobuf/any.proto";
import "google/protobuf/descriptor.proto";

// Baz is lexicographically after Bar
message Baz {
  .google.protobuf.Any any = 1;
}

enum Numbers {
  ZERO=0;
  MINUS_ONE =-1;
  MINUS_TWO =-2;
  TWO = 2;
  ONE=1;
  ALIAS = 1 [deprecated = true, debug_redact = false];
  reserved 6;
  reserved 3 to 5;
  reserved "THREE", "FOUR", "FIVE";
  reserved "SIX";

  option allow_alias = true;
}


/**
 * Bar.timestamp type is not normalized
 * Bar.any should come second
 */
message Bar {
  .google.protobuf.Any any = 2;
  google.protobuf.Timestamp timestamp = 1;

  message NestedMessage {
    string value = 1;
  }

  // reserved should be sorted
  reserved 6;
  reserved 3 to 5;

  enum NestedEnum {
    FOO = 0;
    BAR = 1;
  }

  oneof string_or_byte {
    bytes bytes = 21;
    string string = 20;
  }

  oneof integral {
    double double = 7;
    float float = 8;
    int32 int32 = 9;
    int64 int64 = 10;
    uint32 uint32 = 11;
    uint64 uint64 = 12;
    sint32 sint32 = 13;
    sint64 sint64 = 14;
    fixed32 fixed32 = 15;
    fixed64 fixed64 = 16;
    sfixed32 sfixed32 = 17;
    sfixed64 sfixed64 = 18;
    bool bool = 19 [deprecated = false, retention = RETENTION_SOURCE];
  }

  repeated bool repeated_bool = 22 [packed = true];
  map<string, string> map_string_string = 23;
  NestedEnum repeated_nested_enum = 24 [deprecated = false, retention = RETENTION_SOURCE];

  message MessageOptions {
    option message_set_wire_format = false;
    option no_standard_descriptor_accessor = true;
    option deprecated = true;
  }

}
service FooService {
  rpc Foo(Bar) returns (Baz);
})";

    BOOST_CHECK_EQUAL(ppstu::sanitize(schema), (R"(syntax = "proto3";
package foo;

import "google/protobuf/timestamp.proto";
import "google/protobuf/any.proto";
import "google/protobuf/descriptor.proto";
import public "google/protobuf/duration.proto";

option java_package = "com.example.foo";
option java_outer_classname = "FooService";
option optimize_for = SPEED;
option go_package = "foo.example.com/fooservice";
option cc_enable_arenas = true;
option objc_class_prefix = "FS";
option csharp_namespace = "Foo.FooService";
option php_namespace = "my_php\ns";

enum Numbers {
  option allow_alias = true;
  ZERO = 0;
  MINUS_ONE = -1;
  MINUS_TWO = -2;
  TWO = 2;
  ONE = 1;
  ALIAS = 1 [deprecated = true, debug_redact = false];
  reserved 6, 3 to 5;
  reserved "THREE", "FOUR", "FIVE", "SIX";
}

message Baz {
  .google.protobuf.Any any = 1;
}

message Bar {
  message NestedMessage {
    string value = 1;
  }
  message MessageOptions {
    option message_set_wire_format = false;
    option no_standard_descriptor_accessor = true;
    option deprecated = true;
  }
  enum NestedEnum {
    FOO = 0;
    BAR = 1;
  }
  .google.protobuf.Any any = 2;
  .google.protobuf.Timestamp timestamp = 1;
  oneof string_or_byte {
    bytes bytes = 21;
    string string = 20;
  }
  oneof integral {
    double double = 7;
    float float = 8;
    int32 int32 = 9;
    int64 int64 = 10;
    uint32 uint32 = 11;
    uint64 uint64 = 12;
    sint32 sint32 = 13;
    sint64 sint64 = 14;
    fixed32 fixed32 = 15;
    fixed64 fixed64 = 16;
    sfixed32 sfixed32 = 17;
    sfixed64 sfixed64 = 18;
    bool bool = 19 [deprecated = false, retention = RETENTION_SOURCE];
  }
  repeated bool repeated_bool = 22 [packed = true];
  map<string, string> map_string_string = 23;
  .foo.Bar.NestedEnum repeated_nested_enum = 24 [deprecated = false, retention = RETENTION_SOURCE];
  reserved 6, 3 to 5;
}

service FooService {
  rpc Foo(.foo.Bar) returns (.foo.Baz);
}
)"));
    BOOST_CHECK_EQUAL(ppstu::normalize(schema), (R"(syntax = "proto3";
package foo;

import "google/protobuf/any.proto";
import "google/protobuf/descriptor.proto";
import "google/protobuf/timestamp.proto";
import public "google/protobuf/duration.proto";

option java_package = "com.example.foo";
option java_outer_classname = "FooService";
option optimize_for = SPEED;
option go_package = "foo.example.com/fooservice";
option cc_enable_arenas = true;
option objc_class_prefix = "FS";
option csharp_namespace = "Foo.FooService";
option php_namespace = "my_php\ns";

enum Numbers {
  option allow_alias = true;
  ZERO = 0;
  ALIAS = 1 [deprecated = true, debug_redact = false];
  ONE = 1;
  TWO = 2;
  MINUS_TWO = -2;
  MINUS_ONE = -1;
  reserved 3 to 5, 6;
  reserved "FIVE", "FOUR", "SIX", "THREE";
}

message Baz {
  .google.protobuf.Any any = 1;
}

message Bar {
  message NestedMessage {
    string value = 1;
  }
  message MessageOptions {
    option message_set_wire_format = false;
    option no_standard_descriptor_accessor = true;
    option deprecated = true;
  }
  enum NestedEnum {
    FOO = 0;
    BAR = 1;
  }
  .google.protobuf.Timestamp timestamp = 1;
  .google.protobuf.Any any = 2;
  repeated bool repeated_bool = 22 [packed = true];
  map<string, string> map_string_string = 23;
  .foo.Bar.NestedEnum repeated_nested_enum = 24 [deprecated = false, retention = RETENTION_SOURCE];
  oneof string_or_byte {
    string string = 20;
    bytes bytes = 21;
  }
  oneof integral {
    double double = 7;
    float float = 8;
    int32 int32 = 9;
    int64 int64 = 10;
    uint32 uint32 = 11;
    uint64 uint64 = 12;
    sint32 sint32 = 13;
    sint64 sint64 = 14;
    fixed32 fixed32 = 15;
    fixed64 fixed64 = 16;
    sfixed32 sfixed32 = 17;
    sfixed64 sfixed64 = 18;
    bool bool = 19 [deprecated = false, retention = RETENTION_SOURCE];
  }
  reserved 3 to 5, 6;
}

service FooService {
  rpc Foo(.foo.Bar) returns (.foo.Baz);
}
)"));
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

const pps::schema_definition proto2_old{
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

const pps::schema_definition proto2_new{
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

const pps::schema_definition proto3_old{
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

const pps::schema_definition proto3_new{
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
