// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "iceberg/json_writer.h"
#include "iceberg/schema.h"
#include "json/document.h"

namespace iceberg {

schema parse_schema(const json::Value&);

} // namespace iceberg

namespace json {

void rjson_serialize(iceberg::json_writer& w, const iceberg::schema& s);

} // namespace json
