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

#include "redpanda/admin/aip_filter.h"

#include "absl/strings/numbers.h"
#include "absl/strings/str_split.h"
#include "absl/time/time.h"
#include "serde/protobuf/base.h"
#include "serde/protobuf/rpc.h"

#include <seastar/util/variant_utils.hh>

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <lexy/action/parse.hpp>
#include <lexy/callback.hpp>
#include <lexy/dsl.hpp>
#include <lexy/error.hpp>
#include <lexy/input/string_input.hpp>

#include <algorithm>
#include <cstdint>
#include <type_traits>
#include <variant>

namespace admin {
namespace {

enum class comparison_op : uint8_t { EQ, NE, LT, GT, LE, GE };

struct literal_string {
    std::string value;
    bool is_quoted;
};

struct comparison {
    std::string field_path;
    comparison_op op;
    literal_string value;
};

namespace grammar {
namespace dsl = lexy::dsl;

// Proto3 field identifier
// Ref: https://protobuf.dev/reference/protobuf/proto3-spec/
struct field_name_def {
    static constexpr auto rule = [] {
        auto leading_char = dsl::ascii::alpha;
        auto trailing_char = dsl::ascii::word / dsl::lit_c<'.'>;
        return dsl::identifier(leading_char, trailing_char);
    }();
    static constexpr auto value = lexy::as_string<std::string>;
};

struct comparison_operator_def {
    static constexpr auto mapping_table = lexy::symbol_table<comparison_op>
        .map<LEXY_SYMBOL("=")>(comparison_op::EQ)
        .map<LEXY_SYMBOL("!=")>(comparison_op::NE)
        .map<LEXY_SYMBOL("<")>(comparison_op::LT)
        .map<LEXY_SYMBOL("<=")>(comparison_op::LE)
        .map<LEXY_SYMBOL(">")>(comparison_op::GT)
        .map<LEXY_SYMBOL(">=")>(comparison_op::GE);

    static constexpr auto rule = dsl::symbol<mapping_table>;
    static constexpr auto value = lexy::construct<comparison_op>;
};

struct quoted_string_def {
    static constexpr auto escaped_symbols = lexy::symbol_table<char>
                                              .map<'"'>('"')
                                              .map<'\\'>('\\')
                                              .map<'/'>('/')
                                              .map<'b'>('\b')
                                              .map<'f'>('\f')
                                              .map<'n'>('\n')
                                              .map<'r'>('\r')
                                              .map<'t'>('\t');

    static constexpr auto rule = [] {
        // Escape sequences start with a backlash. They either map one of the
        // standard escape symbols or a UTF-8 \xNN escape sequence.
        auto escape = dsl::backslash_escape.symbol<escaped_symbols>().rule(
          LEXY_LIT("x") >> dsl::code_unit_id<lexy::utf8_encoding, 2>);
        return dsl::quoted(dsl::ascii::character, escape);
    }();

    static constexpr auto value
      = lexy::as_string<std::string> >> lexy::callback<literal_string>(
          [](std::string str) {
              return literal_string{.value = std::move(str), .is_quoted = true};
          });
};

struct unquoted_value_def {
    static constexpr auto rule = [] {
        auto chars = dsl::ascii::word / dsl::lit_c<'.'> / dsl::lit_c<'-'>
                     / dsl::lit_c<':'>;
        return dsl::identifier(chars);
    }();
    static constexpr auto value = lexy::callback<literal_string>([](auto lex) {
        return literal_string{
          .value = std::string(lex.begin(), lex.end()), .is_quoted = false};
    });
};

// Parse all literals as strings to allow converting types with well-defined
// formats (time, duration) using absl helpers instead of defining a complex
// grammar for them and converting them during parsing
struct value_def {
    static constexpr auto rule = dsl::p<quoted_string_def>
                                 | dsl::p<unquoted_value_def>;

    static constexpr auto value = lexy::forward<literal_string>;
};

struct comparison_def {
    static constexpr auto rule = dsl::p<field_name_def>
                                 + dsl::p<comparison_operator_def>
                                 + dsl::p<value_def>;
    static constexpr auto value = lexy::construct<comparison>;
};

struct expression_def {
    // For now, allow only ' ' as whitespace between any subsequent productions
    static constexpr auto whitespace = dsl::lit_c<' '>;
    static constexpr auto rule
      = dsl::list(dsl::p<comparison_def>, dsl::sep(LEXY_LIT("AND"))) + dsl::eof;
    static constexpr auto value = lexy::as_list<std::vector<comparison>>;
};

} // namespace grammar

std::vector<comparison> parse_expression(std::string_view input) {
    // Format and collect all lexy-generated parsing error types into a string
    auto format_tail =
      [](lexy::string_error_context<> ctx, const char* position) {
          auto input = std::string_view(ctx.input().data(), ctx.input().size());
          auto pos = static_cast<size_t>(position - ctx.input().data());
          auto len = input.size();

          return fmt::format(
            ": got {} at position {} of input '{}'",
            pos < len ? fmt::format("'{}'", input.substr(pos, 1)) : "<EOF>",
            pos,
            input);
      };
    auto error_collector = lexy::collect<std::vector<std::string>>(
      lexy::callback<std::string>(
        [&](
          lexy::string_error_context<> ctx,
          lexy::error<lexy::_prd, lexy::expected_char_class> error)
          -> std::string {
            return fmt::format(
              "expected '{}' character{}",
              error.name(),
              format_tail(ctx, error.position()));
        },

        [&](
          lexy::string_error_context<> ctx,
          lexy::error<lexy::_prd, void> error) -> std::string {
            return fmt::format(
              "{}{}", error.message(), format_tail(ctx, error.position()));
        },
        [&](lexy::string_error_context<> ctx, auto error) -> std::string {
            return fmt::format(
              "{}{}", error.string(), format_tail(ctx, error.position()));
        }));

    auto lexy_input = lexy::string_input{input};
    auto result = lexy::parse<grammar::expression_def>(
      lexy_input, error_collector);

    if (!result) {
        // We should always have an error here if parsing failed, but we handle
        // the unexpected case defensively
        auto err = result.errors().empty() ? std::string_view{"[unknown error]"}
                                           : result.errors().front();
        throw serde::pb::rpc::invalid_argument_exception(
          fmt::format("Failed to parse input: {}", err));
    }

    return result.value();
}

template<typename SerdeType, typename LiteralType>
auto get_field_value(
  serde::pb::base_message& obj, std::span<const int32_t> field_numbers)
  -> std::optional<LiteralType> {
    auto field_val = obj.lookup_field(field_numbers);
    if (!field_val) {
        // This should be impossible, since we could only get field numbers from
        // convert_field_path_to_numbers if the field exists, but handle it to
        // be defensive
        throw serde::pb::rpc::internal_exception(
          fmt::format(
            "Unexpected unknown field during filtering: {}", field_numbers));
    }
    if (!std::holds_alternative<SerdeType>(field_val->value)) {
        // TODO: unset optional fields should lead to this branch
        // For now, this should be unreachable code
        return std::nullopt;
    }
    auto res = std::get<SerdeType>(std::move(field_val->value));
    if constexpr (std::is_same_v<SerdeType, serde::pb::raw_enum_value>) {
        // Remap enum fields to their string name for comparison
        return ss::sstring{res.name};
    } else {
        return res;
    }
}

struct and_node : public ast_node {
    std::vector<std::unique_ptr<ast_node>> children;

    bool evaluate(serde::pb::base_message& obj) const override {
        return std::ranges::all_of(
          children, [&obj](const auto& child) { return child->evaluate(obj); });
    }
};

template<typename SerdeType, typename LiteralType>
struct comparison_node : public ast_node {
    std::vector<int32_t> field_numbers;
    comparison_op op;
    LiteralType literal_value;

    comparison_node(
      std::vector<int32_t> field_numbers, comparison_op oper, LiteralType value)
      : field_numbers(std::move(field_numbers))
      , op(oper)
      , literal_value(std::move(value)) {}

    bool evaluate(serde::pb::base_message& obj) const override {
        std::optional<LiteralType> typed_val
          = get_field_value<SerdeType, LiteralType>(obj, field_numbers);
        if (!typed_val) {
            return false;
        }
        switch (op) {
        case comparison_op::EQ:
            return typed_val.value() == literal_value;
        case comparison_op::NE:
            return typed_val.value() != literal_value;
        case comparison_op::LT:
            return typed_val.value() < literal_value;
        case comparison_op::GT:
            return typed_val.value() > literal_value;
        case comparison_op::LE:
            return typed_val.value() <= literal_value;
        case comparison_op::GE:
            return typed_val.value() >= literal_value;
        }
    }
};

// Helper for defining protobuf field type specific behaviour while mapping a
// comparison -> comparison_node (eg. what subset of comparison operators are
// allowed, remapping serde field type to the type of the literal)
template<typename SerdeType>
struct conversion_info {
    using literal_type = SerdeType;
    constexpr static bool supports_ordering_comparisons = true;
};
template<>
struct conversion_info<bool> {
    using literal_type = bool;
    constexpr static bool supports_ordering_comparisons = false;
    constexpr static std::string_view error_name = "Bool";
};
template<>
struct conversion_info<serde::pb::raw_enum_value> {
    using literal_type = ss::sstring;
    constexpr static bool supports_ordering_comparisons = false;
    constexpr static std::string_view error_name = "Enum";
};

template<typename T>
concept SupportedType = std::is_same_v<T, bool> || std::is_same_v<T, int64_t>
                        || std::is_same_v<T, uint64_t>
                        || std::is_same_v<T, int32_t>
                        || std::is_same_v<T, uint32_t>
                        || std::is_same_v<T, double> || std::is_same_v<T, float>
                        || std::is_same_v<T, ss::sstring>
                        || std::is_same_v<T, absl::Time>
                        || std::is_same_v<T, absl::Duration>
                        || std::is_same_v<T, serde::pb::raw_enum_value>;

template<SupportedType SerdeType>
using literal_type_t = conversion_info<SerdeType>::literal_type;

template<SupportedType SerdeType>
auto convert_literal(
  std::string_view value, bool is_quoted, std::string_view field_path)
  -> literal_type_t<SerdeType> {
    using literal_type = literal_type_t<SerdeType>;

    constexpr auto expects_quoted = std::is_same_v<SerdeType, ss::sstring>;
    if (expects_quoted && !is_quoted) {
        throw serde::pb::rpc::invalid_argument_exception(
          fmt::format(
            "Expected quoted literal for field '{}', got unquoted '{}'",
            field_path,
            value));
    } else if (!expects_quoted && is_quoted) {
        throw serde::pb::rpc::invalid_argument_exception(
          fmt::format(
            "Expected unquoted literal for field '{}', got quoted '{}'",
            field_path,
            value));
    }

    if constexpr (std::is_same_v<literal_type, bool>) {
        if (value != "true" && value != "false") {
            throw serde::pb::rpc::invalid_argument_exception(
              fmt::format(
                "Expected boolean literal for field '{}', got '{}'",
                field_path,
                value));
        }
        return value == "true";
    } else if constexpr (std::is_integral_v<literal_type>) {
        if constexpr (std::is_unsigned_v<literal_type>) {
            if (!value.empty() && value[0] == '-') {
                throw serde::pb::rpc::invalid_argument_exception(
                  fmt::format(
                    "Expected positive integer literal for field '{}', got "
                    "negative '{}'",
                    field_path,
                    value));
            }
        }

        literal_type result;
        if (!absl::SimpleAtoi(value, &result)) {
            // Try parsing as a larger type to see if it's a range issue
            absl::int128 temp;
            if (absl::SimpleAtoi(value, &temp)) {
                throw serde::pb::rpc::invalid_argument_exception(
                  fmt::format(
                    "Integer '{}' is out of range for field '{}' (valid "
                    "range: {} to {})",
                    value,
                    field_path,
                    std::numeric_limits<literal_type>::min(),
                    std::numeric_limits<literal_type>::max()));
            }

            throw serde::pb::rpc::invalid_argument_exception(
              fmt::format(
                "Invalid integer literal '{}' for field '{}'",
                value,
                field_path));
        }
        return result;
    } else if constexpr (std::is_floating_point_v<literal_type>) {
        if constexpr (std::is_same_v<literal_type, float>) {
            float result{};
            if (!absl::SimpleAtof(value, &result)) {
                throw serde::pb::rpc::invalid_argument_exception(
                  fmt::format(
                    "Invalid float literal '{}' for field '{}'",
                    value,
                    field_path));
            }
            return result;
        } else {
            double result{};
            if (!absl::SimpleAtod(value, &result)) {
                throw serde::pb::rpc::invalid_argument_exception(
                  fmt::format(
                    "Invalid floating point literal '{}' for field '{}'",
                    value,
                    field_path));
            }
            return result;
        }
    } else if constexpr (std::is_same_v<literal_type, ss::sstring>) {
        return ss::sstring{value};
    } else if constexpr (std::is_same_v<literal_type, absl::Duration>) {
        absl::Duration result;
        if (!absl::ParseDuration(value, &result)) {
            throw serde::pb::rpc::invalid_argument_exception(
              fmt::format(
                "Invalid duration format '{}' for field '{}' (expected format: "
                "e.g., '1.3s')",
                value,
                field_path));
        }
        return result;
    } else if constexpr (std::is_same_v<literal_type, absl::Time>) {
        absl::Time result;
        std::string error;
        if (!absl::ParseTime(absl::RFC3339_full, value, &result, &error)) {
            throw serde::pb::rpc::invalid_argument_exception(
              fmt::format(
                "Invalid timestamp format '{}' for field '{}': {} (expected "
                "RFC3339 format)",
                value,
                field_path,
                error));
        }
        return result;
    } else {
        static_assert(false, "Unsupported literal type");
    }
}

std::unique_ptr<ast_node>
build_comparison(const aip_filter_config& config, const comparison& comp) {
    std::vector<std::string_view> path_components = absl::StrSplit(
      comp.field_path, '.');
    auto field_numbers = config.field_path_converter(path_components);
    if (!field_numbers) {
        throw serde::pb::rpc::invalid_argument_exception(
          "Invalid field path: " + comp.field_path);
    }

    serde::pb::field::value_t field_type = config.field_type_getter(
      field_numbers.value());

    return ss::visit(
      field_type,
      [&](const SupportedType auto& val) -> std::unique_ptr<ast_node> {
          using serde_type = std::remove_cvref_t<decltype(val)>;
          using info = conversion_info<serde_type>;
          using literal_type = info::literal_type;
          if constexpr (!info::supports_ordering_comparisons) {
              if (
                comp.op != comparison_op::EQ && comp.op != comparison_op::NE) {
                  throw serde::pb::rpc::invalid_argument_exception(
                    fmt::format(
                      "{} field '{}' only supports = and != operators",
                      info::error_name,
                      comp.field_path));
              }
          }
          auto value = convert_literal<serde_type>(
            comp.value.value, comp.value.is_quoted, comp.field_path);
          return std::make_unique<comparison_node<serde_type, literal_type>>(
            field_numbers.value(), comp.op, std::move(value));
      },
      [&](const auto&) -> std::unique_ptr<ast_node> {
          throw serde::pb::rpc::invalid_argument_exception(
            fmt::format(
              "Unsupported field type for field path: {}", comp.field_path));
      });
}

std::unique_ptr<ast_node> build_ast(
  const aip_filter_config& config, const std::vector<comparison>& comparisons) {
    if (comparisons.empty()) {
        return nullptr;
    }

    if (comparisons.size() == 1) {
        return build_comparison(config, comparisons[0]);
    }

    auto and_node_obj = std::make_unique<and_node>();
    for (const auto& comp : comparisons) {
        and_node_obj->children.push_back(build_comparison(config, comp));
    }
    return and_node_obj;
}

} // namespace

filter_predicate::filter_predicate(std::unique_ptr<ast_node> root)
  : _root(std::move(root)) {}

bool filter_predicate::operator()(serde::pb::base_message& obj) const {
    return _root ? _root->evaluate(obj) : true;
}

filter_predicate
aip_filter_parser::create_aip_filter(aip_filter_config config) {
    if (config.filter_expression.empty()) {
        return filter_predicate(nullptr);
    }

    if (config.filter_expression.size() > max_filter_length) {
        // Limit the size of the expression to avoid overly expensive parsing
        throw serde::pb::rpc::invalid_argument_exception(
          fmt::format(
            "Filter expression exceeds maximum length of {} characters (got "
            "{}).",
            max_filter_length,
            config.filter_expression.size()));
    }
    auto comparisons = parse_expression(config.filter_expression);
    return filter_predicate(build_ast(config, comparisons));
}

} // namespace admin
