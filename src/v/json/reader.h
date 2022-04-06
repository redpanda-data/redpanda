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
#include "json/encodings.h"

#include <rapidjson/reader.h>

namespace json {

template<
  typename SourceEncoding = json::UTF8<>,
  typename TargetEncoding = json::UTF8<>>
using GenericReader = rapidjson::
  GenericReader<SourceEncoding, TargetEncoding, throwing_allocator>;

using Reader = GenericReader<>;

template<typename Encoding = UTF8<>, typename Derived = void>
using BaseReaderHandler = rapidjson::BaseReaderHandler<Encoding, Derived>;

} // namespace json
