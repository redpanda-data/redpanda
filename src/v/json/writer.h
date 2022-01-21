// Copyright 2022 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "json/allocator.h"

#include <rapidjson/encodings.h>
#include <rapidjson/writer.h>

namespace json {
template<
  typename OutputStream,
  typename SourceEncoding = rapidjson::UTF8<>,
  typename TargetEncoding = rapidjson::UTF8<>,
  unsigned writeFlags = rapidjson::kWriteDefaultFlags>
using Writer = rapidjson::Writer<
  OutputStream,
  SourceEncoding,
  TargetEncoding,
  throwing_allocator,
  writeFlags>;
}
