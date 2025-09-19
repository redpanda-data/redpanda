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

#include "absl/strings/escaping.h"
#include "absl/strings/numbers.h"
#include "absl/time/time.h"
#include "src/v/redpanda/admin/tests/aip_filter_test_messages.proto.h"

#include <boost/spirit/home/x3.hpp>
#include <fmt/format.h>

#include <algorithm>
#include <type_traits>

namespace admin {

namespace spirit_parser {
namespace x3 = boost::spirit::x3;

namespace {
std::string unescape_quoted_string(std::string_view escaped) {
    std::string result;
    std::string error;

    if (!absl::CUnescape(escaped, &result, &error)) {
        throw std::invalid_argument(
          fmt::format("Invalid escape sequence in quoted string: {}", error));
    }

    return result;
}
} // namespace

class expression_parser {
public:
    explicit expression_parser(std::string_view input)
      : _input(input) {}

private:
    std::string_view _input;

    auto grammar() const {
        // Handle escaped characters in quoted strings (see unit tests for
        // examples). We unescape the quoted string while parsing, since we
        // already handle their detection here, so the AST will have unescaped
        // strings already
        auto escaped_or_regular_char = ('\\' >> x3::char_) | (x3::char_ - '"');
        auto quoted_string_content = x3::rule<
          class quoted_string_content_tag,
          std::string>{"quoted_string_content"}
        = x3::raw[*escaped_or_regular_char];

        auto quoted_string
          = x3::rule<class quoted_string_tag, std::string>{"quoted_string"}
        = x3::lexeme['"' >> quoted_string_content >> '"'][([](auto& ctx) {
              x3::_val(ctx) = unescape_quoted_string(x3::_attr(ctx));
          })];

        auto field_name
          = x3::rule<class field_name_tag, std::string>{"field_name"}
        = x3::lexeme
          [(x3::alpha | x3::char_('_')) >> *(x3::alnum | x3::char_("_."))];

        auto operator_symbols = []() {
            x3::symbols<comparison_op> ops;
            ops.add("!=", comparison_op::NE)("<=", comparison_op::LE)(
              ">=", comparison_op::GE)("=", comparison_op::EQ)(
              "<", comparison_op::LT)(">", comparison_op::GT);
            return ops;
        }();

        auto unquoted_value
          = x3::rule<class unquoted_value_tag, std::string>{"unquoted_value"}
        = x3::lexeme
          [+(x3::char_ - x3::space - x3::char_("\"")) - x3::string("AND")];

        auto value
          = x3::rule<class value_tag, std::pair<std::string, bool>>{"value"}
        = (quoted_string[([](auto& ctx) {
              x3::_val(ctx) = std::make_pair(x3::_attr(ctx), true);
          })])
          | (unquoted_value[([](auto& ctx) {
                x3::_val(ctx) = std::make_pair(x3::_attr(ctx), false);
            })]);

        // <AIP-160> Unlike in most programming languages, field names must
        // appear on the left-hand side of a comparison operator; the right-hand
        // side only accepts literals and logical operators. </>
        auto comparison_rule
          = x3::rule<class comparison_tag, comparison>{"comparison"}
        = (field_name >> operator_symbols >> value)[([](auto& ctx) {
              auto& comp = x3::_val(ctx);
              auto& field = boost::fusion::at_c<0>(x3::_attr(ctx));
              auto& op = boost::fusion::at_c<1>(x3::_attr(ctx));
              auto& val = boost::fusion::at_c<2>(x3::_attr(ctx));

              comp.field_path = field;
              comp.op = op;
              comp.value = val.first;
              comp.value_is_quoted = val.second;
          })];

        return comparison_rule % x3::lit("AND");
    }

public:
    std::vector<comparison> parse() const {
        if (_input.empty()) {
            return {};
        }

        auto iter = _input.begin();
        auto end = _input.end();
        auto result = std::vector<comparison>{};

        auto success = x3::phrase_parse(
          iter, end, grammar(), x3::space, result);

        if (!success || iter != end) {
            auto remaining = std::string(iter, end);
            auto position = std::distance(_input.begin(), iter);

            if (!success) {
                throw std::invalid_argument(
                  fmt::format(
                    "Parse error at position {}: '{}'", position, remaining));
            } else {
                throw std::invalid_argument(
                  fmt::format(
                    "Unexpected characters at position {}: '{}'",
                    position,
                    remaining));
            }
        }

        return result;
    }
};

} // namespace spirit_parser

template<typename T>
struct and_node : public ast_node<T> {
    std::vector<std::unique_ptr<ast_node<T>>> children;

    bool evaluate(T& obj) const override {
        return std::ranges::all_of(
          children, [&obj](const auto& child) { return child->evaluate(obj); });
    }
};

template<typename T, typename F>
struct comparison_node : public ast_node<T> {
    std::function<F(T&)> getField;
    comparison_op op;
    F literalValue;

    comparison_node(std::function<F(T&)> accessor, comparison_op oper, F value)
      : getField(std::move(accessor))
      , op(oper)
      , literalValue(std::move(value)) {}

    bool evaluate(T& obj) const override {
        F fieldVal = getField(obj);
        switch (op) {
        case comparison_op::EQ:
            return fieldVal == literalValue;
        case comparison_op::NE:
            return fieldVal != literalValue;
        case comparison_op::LT:
            return fieldVal < literalValue;
        case comparison_op::GT:
            return fieldVal > literalValue;
        case comparison_op::LE:
            return fieldVal <= literalValue;
        case comparison_op::GE:
            return fieldVal >= literalValue;
        }
        return false;
    }
};

template<typename T>
filter_predicate<T>::filter_predicate(std::unique_ptr<ast_node<T>> root)
  : _root(std::move(root)) {}

template<typename T>
bool filter_predicate<T>::operator()(T& obj) const {
    return _root ? _root->evaluate(obj) : true;
}

template<typename T>
aip_filter_parser<T>::aip_filter_parser(const field_registry<T>& registry)
  : _registry(registry) {}

template<typename T>
filter_predicate<T>
aip_filter_parser<T>::parse(std::string_view filter_expression) const {
    if (filter_expression.empty()) {
        return filter_predicate<T>(nullptr);
    }

    if (filter_expression.size() > max_filter_length) {
        // Limit the size of the expression to avoid overly expensive parsing or
        // filter predicates
        throw std::invalid_argument(
          fmt::format(
            "Filter expression exceeds maximum length of {} characters (got "
            "{})",
            max_filter_length,
            filter_expression.size()));
    }

    spirit_parser::expression_parser parser(filter_expression);
    auto comparisons = parser.parse();
    return filter_predicate<T>(build_ast(comparisons));
}

template<typename T>
std::unique_ptr<ast_node<T>> aip_filter_parser<T>::build_ast(
  const std::vector<comparison>& comparisons) const {
    if (comparisons.empty()) {
        return nullptr;
    }

    if (comparisons.size() == 1) {
        return build_comparison(comparisons[0]);
    }

    auto and_node_obj = std::make_unique<and_node<T>>();
    for (const auto& comp : comparisons) {
        and_node_obj->children.push_back(build_comparison(comp));
    }
    return and_node_obj;
}

template<typename T>
std::unique_ptr<ast_node<T>>
aip_filter_parser<T>::build_comparison(const comparison& comp) const {
    auto validate_comparison_ops = [&](std::string_view type_name) {
        if (comp.op != comparison_op::EQ && comp.op != comparison_op::NE) {
            throw std::invalid_argument(
              fmt::format(
                "{} field '{}' only supports = and != operators",
                type_name,
                comp.field_path));
        }
    };

    auto field_info = _registry.get_field_info(comp.field_path);

    return std::visit(
      [&](const auto& getter) -> std::unique_ptr<ast_node<T>> {
          // Example:
          //   GetterType = int64_getter
          //   GetterFnType = std::function<int64_t(T&)>
          //   ReturnType = int64_t
          using GetterType = std::decay_t<decltype(getter)>;
          using GetterFnType = typename GetterType::type;
          using ReturnType = std::invoke_result_t<GetterFnType, T&>;

          if constexpr (std::is_same_v<
                          GetterType,
                          typename field_accessor_info<T>::bool_getter>) {
              validate_comparison_ops("Boolean");
          } else if constexpr (std::is_same_v<
                                 GetterType,
                                 typename field_accessor_info<
                                   T>::enum_getter>) {
              validate_comparison_ops("Enum");
          }

          auto value = convert_literal<ReturnType>(
            comp.value, comp.value_is_quoted, comp.field_path);
          return std::make_unique<comparison_node<T, ReturnType>>(
            GetterFnType(getter), comp.op, std::move(value));
      },
      field_info.getter);
}

template<typename T>
template<typename FieldType>
FieldType aip_filter_parser<T>::convert_literal(
  std::string_view value, bool is_quoted, std::string_view field_path) const {
    if constexpr (std::is_same_v<FieldType, bool>) {
        if (value != "true" && value != "false") {
            throw std::invalid_argument(
              fmt::format(
                "Expected boolean literal for field '{}'", field_path));
        }
        return value == "true";
    } else if constexpr (std::is_integral_v<FieldType>) {
        if (is_quoted) {
            throw std::invalid_argument(
              fmt::format(
                "Expected integer literal for field '{}'", field_path));
        }
        if constexpr (std::is_unsigned_v<FieldType>) {
            if (!value.empty() && value[0] == '-') {
                throw std::invalid_argument(
                  fmt::format(
                    "Expected positive integer literal for field '{}'",
                    field_path));
            }
        }
        FieldType result;
        if (!absl::SimpleAtoi(value, &result)) {
            throw std::invalid_argument(
              fmt::format(
                "Expected integer literal for field '{}'", field_path));
        }
        return result;
    } else if constexpr (std::is_same_v<FieldType, double>) {
        if (is_quoted) {
            throw std::invalid_argument(
              fmt::format(
                "Expected numeric literal for field '{}'", field_path));
        }
        double result{};
        if (!absl::SimpleAtod(value, &result)) {
            throw std::invalid_argument(
              fmt::format(
                "Expected numeric literal for field '{}'", field_path));
        }
        return result;
    } else if constexpr (std::is_same_v<FieldType, ss::sstring>) {
        return ss::sstring{value};
    } else if constexpr (std::is_same_v<FieldType, absl::Duration>) {
        absl::Duration result;
        if (!absl::ParseDuration(value, &result)) {
            throw std::invalid_argument(
              fmt::format(
                "Invalid duration format for field '{}': {}",
                field_path,
                value));
        }
        return result;
    } else if constexpr (std::is_same_v<FieldType, absl::Time>) {
        absl::Time result;
        std::string error;
        if (!absl::ParseTime(absl::RFC3339_full, value, &result, &error)) {
            throw std::invalid_argument(
              fmt::format(
                "Invalid timestamp format for field '{}': {} ({})",
                field_path,
                value,
                error));
        }
        return result;
    } else {
        throw std::invalid_argument(
          fmt::format("Unsupported conversion for field '{}'", field_path));
    }
}

// Explicitly instantiating for all supported protobuf types to avoid having to
// expose all the templated code in the header
template class filter_predicate<aip_filter_test::test_message>;
template class aip_filter_parser<aip_filter_test::test_message>;

} // namespace admin
