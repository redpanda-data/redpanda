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

#include "serde/json/tests/dom_rapidjson.h"

#include "bytes/streambuf.h"
#include "json/istreamwrapper.h"
#include "json/reader.h"

#include <seastar/core/coroutine.hh>

#include <rapidjson/error/en.h>

namespace serde::json::test::dom {

ss::future<value> parse_document_rapidjson(iobuf buf) {
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

    struct dom_handler {
        chunked_vector<stack_element>* stack;

        explicit dom_handler(chunked_vector<stack_element>* stack)
          : stack(stack) {};

        using Ch = rapidjson::UTF8<>::Ch;
        using SizeType = rapidjson::SizeType;

        bool Null() {
            stack->back().values.emplace_back(null_t{});
            return true;
        }
        bool Bool(bool v) {
            stack->back().values.emplace_back(v);
            return true;
        }
        bool Int(int v) {
            stack->back().values.emplace_back(int64_t(v));
            return true;
        }
        bool Uint(unsigned v) {
            stack->back().values.emplace_back(static_cast<int64_t>(v));
            return true;
        }
        bool Int64(int64_t v) {
            stack->back().values.emplace_back(v);
            return true;
        }
        bool Uint64(uint64_t v) {
            // if fits int64_t, store as int64_t
            // else store as double
            if (v <= std::numeric_limits<int64_t>::max()) {
                stack->back().values.emplace_back(static_cast<int64_t>(v));
            } else {
                stack->back().values.emplace_back(static_cast<double>(v));
            }
            return true;
        }
        bool Double(double v) {
            stack->back().values.emplace_back(v);
            return true;
        }
        /// enabled via kParseNumbersAsStringsFlag, string is not
        /// null-terminated (use length)
        bool RawNumber(const Ch*, SizeType, bool) {
            vassert(false, "unreachable");
        }
        bool String(const Ch* p, SizeType sz, bool) {
            iobuf buf;
            buf.append(p, sz);
            stack->back().values.emplace_back(std::move(buf));
            return true;
        }
        bool StartObject() {
            stack->push_back({container_type::object, {}});
            return true;
        }
        bool Key(const Ch* p, SizeType sz, bool) {
            vassert(
              stack->back().type == container_type::object,
              "Got a key token in a non-object");
            return String(p, sz, false);
        }
        bool EndObject(SizeType sz) {
            vassert(
              stack->back().type == container_type::object,
              "Got an end object token in a non-object");
            vassert(
              stack->back().values.size() % 2 == 0,
              "Expected even number of elements in object");
            vassert(
              sz == stack->back().values.size() / 2,
              "Expected half the number of values for object keys and values");

            // Move the values from the stack into a new object.
            auto tmp = json_object{};
            tmp.reserve(stack->back().values.size() / 2);
            for (size_t i = 0; i < stack->back().values.size(); i += 2) {
                tmp.emplace(
                  std::move(stack->back().values[i].string()),
                  std::move(stack->back().values[i + 1]));
            }
            stack->pop_back();
            stack->back().values.emplace_back(std::move(tmp));
            return true;
        }
        bool StartArray() {
            stack->push_back({container_type::array, {}});
            return true;
        }
        bool EndArray(SizeType sz) {
            vassert(
              stack->back().type == container_type::array,
              "Got an end array token in a non-array");
            vassert(
              sz == stack->back().values.size(),
              "Expected same number of values for array");
            // Move the values from the stack into a new array.
            auto tmp = json_array{};
            tmp.reserve(stack->back().values.size());
            for (auto& v : stack->back().values) {
                tmp.push_back(std::move(v));
            }
            stack->pop_back();
            stack->back().values.emplace_back(std::move(tmp));
            return true;
        }
    };

    iobuf_istreambuf ibuf(buf);
    std::istream stream(&ibuf);
    ::json::IStreamWrapper wrapper(stream);

    ::json::Reader reader;
    dom_handler handler(&stack);

    if (reader.Parse(wrapper, handler)) {
    } else {
        rapidjson::ParseErrorCode e = reader.GetParseErrorCode();
        size_t o = reader.GetErrorOffset();

        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "Failed to parse JSON document at offset {}: {}",
          o,
          rapidjson::GetParseError_En(e)));
    }

    vassert(stack.size() == 1, "Expected one element in stack");

    co_return std::move(stack.back().values[0]);
}

}; // namespace serde::json::test::dom
