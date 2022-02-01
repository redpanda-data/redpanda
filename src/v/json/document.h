// Copyright 2022 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "json/allocator.h"

#include <rapidjson/document.h>
#include <rapidjson/encodings.h>

namespace json {
using Document = rapidjson::GenericDocument<
  rapidjson::UTF8<>,
  rapidjson::MemoryPoolAllocator<throwing_allocator>,
  throwing_allocator>;
}
