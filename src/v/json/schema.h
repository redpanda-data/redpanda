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

template<
  typename InputStream,
  unsigned parseFlags = rapidjson::kParseDefaultFlags,
  typename SourceEncoding = UTF8<>>
using SchemaValidatingReader = rapidjson::SchemaValidatingReader<
  parseFlags,
  InputStream,
  SourceEncoding,
  SchemaDocument,
  json::throwing_allocator>;

} // namespace json
