// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "json/_include_first.h"
#include "json/allocator.h"
#include "json/encodings.h"

#include <rapidjson/document.h>

namespace json {

template<typename Encoding = json::UTF8<>>
using GenericDocument = rapidjson::
  GenericDocument<Encoding, MemoryPoolAllocator, throwing_allocator>;

using Document = GenericDocument<>;

template<typename Encoding = json::UTF8<>>
using GenericValue = typename GenericDocument<Encoding>::ValueType;

using Value = GenericValue<>;

} // namespace json
