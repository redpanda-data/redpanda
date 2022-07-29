/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include <json/schema.h>
#include <json/validator.h>

#include <string>

namespace json {

struct validator {
    explicit validator(const std::string& schema_text)
      : schema(make_schema_document(schema_text))
      , schema_validator(schema) {}

    static json::SchemaDocument
    make_schema_document(const std::string& schema) {
        json::Document doc;
        if (doc.Parse(schema).HasParseError()) {
            throw std::runtime_error(
              fmt::format("Invalid schema document: {}", schema));
        }
        return json::SchemaDocument(doc);
    }

    const json::SchemaDocument schema;
    json::SchemaValidator schema_validator;
};

} // namespace json
