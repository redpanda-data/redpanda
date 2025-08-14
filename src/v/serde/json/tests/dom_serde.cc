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

#include "serde/json/parser.h"
#include "serde/json/tests/dom.h"

#include <seastar/core/coroutine.hh>

namespace serde::json::test::dom {

ss::future<value> parse_document_serde(iobuf buf) {
    auto parser = serde::json::parser(std::move(buf));

    enum class container_type {
        document,
        object,
        array,
    };

    struct stack_element {
        container_type type;
        chunked_vector<value> values;
    };

    chunked_vector<stack_element> stack{};
    stack.push_back({container_type::document, {}});

    while (co_await parser.next()) {
        switch (parser.token()) {
        case token::value_null:
            stack.back().values.emplace_back(null_t{});
            break;
        case token::value_true:
            stack.back().values.emplace_back(true);
            break;
        case token::value_false:
            stack.back().values.emplace_back(false);
            break;
        case token::value_double:
            stack.back().values.emplace_back(parser.value_double());
            break;
        case token::value_int:
            stack.back().values.emplace_back(parser.value_int());
            break;
        case token::value_string:
            stack.back().values.emplace_back(parser.value_string());
            break;
        case token::start_object:
            stack.push_back({container_type::object, {}});
            break;
        case token::key:
            vassert(
              stack.back().type == container_type::object,
              "Got a key token in a non-object");
            stack.back().values.emplace_back(parser.value_string());
            break;
        case token::end_object: {
            vassert(
              stack.back().type == container_type::object,
              "Got an end object token in a non-object");
            vassert(
              stack.back().values.size() % 2 == 0,
              "Expected even number of elements in object");

            // Move the values from the stack into a new object.
            auto tmp = json_object{};
            tmp.reserve(stack.back().values.size() / 2);
            for (size_t i = 0; i < stack.back().values.size(); i += 2) {
                tmp.emplace(
                  std::move(stack.back().values[i].string()),
                  std::move(stack.back().values[i + 1]));
            }
            stack.pop_back();
            stack.back().values.emplace_back(std::move(tmp));
            break;
        }
        case token::start_array:
            stack.push_back({container_type::array, {}});
            break;
        case token::end_array: {
            vassert(
              stack.back().type == container_type::array,
              "Got an end array token in a non-array");
            // Move the values from the stack into a new array.
            auto tmp = json_array{};
            tmp.reserve(stack.back().values.size());
            for (auto& v : stack.back().values) {
                tmp.push_back(std::move(v));
            }
            stack.pop_back();
            stack.back().values.emplace_back(std::move(tmp));
            break;
        }
        case token::error:
            throw std::runtime_error("Failed to parse JSON document");
        case token::eof:
            vassert(
              stack.back().type == container_type::document,
              "Got EOF in a non-document");
            vassert(
              stack.back().values.size() == 1,
              "Expected exactly one value in document");
            co_return std::move(stack.back().values[0]);
        }
    }

    vassert(false, "Expected EOF");
}

} // namespace serde::json::test::dom
