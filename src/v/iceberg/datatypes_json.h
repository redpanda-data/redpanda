// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "iceberg/datatypes.h"
#include "iceberg/json_writer.h"
#include "json/document.h"

namespace iceberg {

struct_type parse_struct(const json::Value&);
list_type parse_list(const json::Value&);
map_type parse_map(const json::Value&);
field_type parse_type(const json::Value&);
nested_field_ptr parse_field(const json::Value&);

} // namespace iceberg

namespace json {

void rjson_serialize(iceberg::json_writer& w, const iceberg::nested_field& f);
void rjson_serialize(iceberg::json_writer& w, const iceberg::primitive_type& t);
void rjson_serialize(iceberg::json_writer& w, const iceberg::struct_type& t);
void rjson_serialize(iceberg::json_writer& w, const iceberg::list_type& t);
void rjson_serialize(iceberg::json_writer& w, const iceberg::map_type& t);
void rjson_serialize(iceberg::json_writer& w, const iceberg::field_type& t);

} // namespace json
