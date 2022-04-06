// Copyright 2022 Vectorized, Inc.
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
#include "json/document.h"
#include "json/reader.h"

#include <rapidjson/schema.h>

namespace json {

using SchemaDocument
  = rapidjson::GenericSchemaDocument<Value, throwing_allocator>;

template<
  typename SchemaDocumentType,
  typename OutputHandler = json::BaseReaderHandler<
    typename SchemaDocumentType::SchemaType::EncodingType>>
using GenericSchemaValidator = rapidjson::
  GenericSchemaValidator<SchemaDocumentType, OutputHandler, throwing_allocator>;

using SchemaValidator = GenericSchemaValidator<SchemaDocument>;

} // namespace json
