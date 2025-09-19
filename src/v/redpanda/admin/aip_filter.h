/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "redpanda/admin/field_registry.h"

#include <memory>
#include <string>
#include <vector>

namespace admin {

enum class comparison_op : uint8_t { EQ, NE, LT, GT, LE, GE };

struct comparison {
    std::string field_path;
    comparison_op op;
    std::string value;
    bool value_is_quoted;
};

template<typename T>
struct ast_node {
    virtual ~ast_node() = default;
    virtual bool evaluate(T& obj) const = 0;
};

/// A filter predicate object that can be applied to objects of type T
template<typename T>
class filter_predicate {
public:
    explicit filter_predicate(std::unique_ptr<ast_node<T>> root);
    bool operator()(T& obj) const;

private:
    std::unique_ptr<ast_node<T>> _root;
};

/// aip_filter_parser parses AIP-160 compliant, human-readable filter expression
/// strings into filter_predicate's to allow filtering protobuf objects.
///
/// Supports a subset of the AIP-160 spec:
/// - Field comparisons (`=`, `!=`, `<`, `>`, `<=`, `>=`)
/// - Logical AND chaining: condition1 AND condition2
/// - Nested field access: parent.child = value
/// - Escape sequences: field = "string with \"quotes\""
/// - Enum types
/// - RFC3339 timestamps and ISO-like duration parsing via abseil
///
/// Ref: https://google.aip.dev/160
template<typename T>
class aip_filter_parser {
public:
    constexpr static size_t max_filter_length = 1024;

    explicit aip_filter_parser(const field_registry<T>& registry);

    filter_predicate<T> parse(std::string_view filter_expression) const;

private:
    std::unique_ptr<ast_node<T>>
    build_ast(const std::vector<comparison>& comparisons) const;
    std::unique_ptr<ast_node<T>> build_comparison(const comparison& comp) const;

    template<typename FieldType>
    FieldType convert_literal(
      std::string_view value,
      bool is_quoted,
      std::string_view field_path) const;

    const field_registry<T>& _registry;
};

} // namespace admin
