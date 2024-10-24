// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "pandaproxy/schema_registry/protobuf.h"

namespace pp = pandaproxy;
namespace pps = pp::schema_registry;

const auto simple = pps::canonical_schema_definition{
  R"(
syntax = "proto3";

message Simple {
  string id = 1;
})",
  pps::schema_type::protobuf};

const auto imported_no_ref = pps::canonical_schema_definition{
  R"(
syntax = "proto3";

import "simple";

message Test2 {
  Simple id =  1;
})",
  pps::schema_type::protobuf};

const auto imported = pps::canonical_schema_definition{
  R"(
syntax = "proto3";

import "simple";

message Test2 {
  Simple id =  1;
})",
  pps::schema_type::protobuf,
  {{"simple", pps::subject{"simple.proto"}, pps::schema_version{1}}}};

const auto imported_again = pps::canonical_schema_definition{
  R"(
syntax = "proto3";

import "imported";

message Test3 {
  Test2 id =  1;
})",
  pps::schema_type::protobuf,
  {{"imported", pps::subject{"imported.proto"}, pps::schema_version{1}}}};

const auto imported_twice = pps::canonical_schema_definition{
  R"(
syntax = "proto3";

import "simple";
import "imported";

message Test3 {
  Test2 id =  1;
})",
  pps::schema_type::protobuf,
  {{"simple", pps::subject{"simple.proto"}, pps::schema_version{1}},
   {"imported", pps::subject{"imported.proto"}, pps::schema_version{1}}}};

const auto nested = pps::canonical_schema_definition{
  R"(
syntax = "proto3";

message A0 {}

message A1 {
  message B0 {
     message C0 {}
     message C1 {}
     message C2 {}
     message C3 {}
     message C4 {}
  }
})",
  pps::schema_type::protobuf};

// Binary encoded protobuf descriptor:
//
// syntax = "proto3";
//
// package com.redpanda;
//
// option go_package = "./;main";
// option java_multiple_files = true;
//
// import "google/protobuf/timestamp.proto";
//
// message Payload {
//     int32 val = 1;
//     google.protobuf.Timestamp timestamp = 2;
// }
//
// message A {
//     message B {
//         message C {
//             message D {
//                 message M00 {}
//                 message M01 {}
//                 message M02 {}
//                 message M03 {}
//                 message M04 {}
//                 message M05 {}
//                 message M06 {}
//                 message M07 {}
//                 message M08 {}
//                 message M09 {}
//                 message M10 {}
//                 message M11 {}
//                 message M12 {}
//                 message M13 {}
//                 message M14 {}
//                 message M15 {}
//                 message M16 {}
//                 message M17 {}
//                 message NestedPayload {
//                     int32 val = 1;
//                     google.protobuf.Timestamp timestamp = 2;
//                 }
//             }
//         }
//     }
// }
//
// message CompressiblePayload {
//     int32 val = 1;
//     google.protobuf.Timestamp timestamp = 2;
//     string message = 3;
// }
constexpr std::string_view base64_raw_proto{
  "Cg1wYXlsb2FkLnByb3RvEgxjb20ucmVkcGFuZGEaH2dvb2dsZS9wcm90b2J1Zi90aW1lc3RhbXAu"
  "cHJvdG8iRQoHUGF5bG9hZBILCgN2YWwYASABKAUSLQoJdGltZXN0YW1wGAIgASgLMhouZ29vZ2xl"
  "LnByb3RvYnVmLlRpbWVzdGFtcCLgAQoBQRraAQoBQhrUAQoBQxrOAQoBRBoFCgNNMDAaBQoDTTAx"
  "GgUKA00wMhoFCgNNMDMaBQoDTTA0GgUKA00wNRoFCgNNMDYaBQoDTTA3GgUKA00wOBoFCgNNMDka"
  "BQoDTTEwGgUKA00xMRoFCgNNMTIaBQoDTTEzGgUKA00xNBoFCgNNMTUaBQoDTTE2GgUKA00xNxpL"
  "Cg1OZXN0ZWRQYXlsb2FkEgsKA3ZhbBgBIAEoBRItCgl0aW1lc3RhbXAYAiABKAsyGi5nb29nbGUu"
  "cHJvdG9idWYuVGltZXN0YW1wImIKE0NvbXByZXNzaWJsZVBheWxvYWQSCwoDdmFsGAEgASgFEi0K"
  "CXRpbWVzdGFtcBgCIAEoCzIaLmdvb2dsZS5wcm90b2J1Zi5UaW1lc3RhbXASDwoHbWVzc2FnZRgD"
  "IAEoCUILUAFaBy4vO21haW5iBnByb3RvMw=="};

const pps::canonical_schema_definition base64_proto{
  pps::canonical_schema_definition{
    base64_raw_proto, pps::schema_type::protobuf}};
