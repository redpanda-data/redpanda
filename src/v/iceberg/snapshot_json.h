// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "iceberg/snapshot.h"
#include "json/_include_first.h"
#include "json/document.h"
#include "json/stringbuffer.h"
#include "json/writer.h"

namespace iceberg {

snapshot parse_snapshot(const json::Value&);
snapshot_reference parse_snapshot_ref(const json::Value&);

} // namespace iceberg

namespace json {

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const iceberg::snapshot& s);
void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const iceberg::snapshot_reference& s);

} // namespace json
