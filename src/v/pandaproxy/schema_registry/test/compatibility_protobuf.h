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

const auto imported = pps::canonical_schema_definition{
  R"(
syntax = "proto3";

import "simple";

message Test2 {
  Simple id =  1;
})",
  pps::schema_type::protobuf};

const auto imported_again = pps::canonical_schema_definition{
  R"(
syntax = "proto3";

import "imported";

message Test3 {
  Test2 id =  1;
})",
  pps::schema_type::protobuf};

const auto imported_twice = pps::canonical_schema_definition{
  R"(
syntax = "proto3";

import "simple";
import "imported";

message Test3 {
  Test2 id =  1;
})",
  pps::schema_type::protobuf};
