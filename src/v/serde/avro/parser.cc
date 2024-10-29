/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "serde/avro/parser.h"

#include "bytes/iobuf_parser.h"

#include <seastar/util/defer.hh>

namespace serde::avro {
// an arbitrary limit for nesting depth (number of levels in the schema tree)
static constexpr int max_nested_depth = 100;
namespace {

struct parser_state {
    int level{0};
};

class parser {
public:
    parser(iobuf buf, const ::avro::ValidSchema& schema)
      : _initial_size(buf.size_bytes())
      , _parser(std::move(buf))
      , _schema(&schema) {}

    ss::future<std::unique_ptr<parsed::message>>
    parse_node(const ::avro::NodePtr& node, parser_state& state) {
        state.level++;
        if (state.level >= max_nested_depth) {
            throw std::invalid_argument(fmt::format(
              "max nested field depth of {} reached", max_nested_depth));
        }
        auto decrement_on_exit = ss::defer([&state] { state.level--; });
        switch (node->type()) {
        case ::avro::AVRO_STRING: {
            auto sz = parse_length();
            co_return std::make_unique<parsed::message>(_parser.share(sz));
        }
        case ::avro::AVRO_BYTES: {
            auto sz = parse_length();
            co_return std::make_unique<parsed::message>(
              parsed::primitive(_parser.share(sz)));
        }
        case ::avro::AVRO_INT: {
            auto [value, _] = _parser.read_varlong();
            using limits_t = std::numeric_limits<int32_t>;
            if (value < limits_t::min() || value > limits_t::max()) {
                throw std::invalid_argument(fmt::format(
                  "Value {} is outside of avro int limits: [{},{}]",
                  value,
                  limits_t::min(),
                  limits_t::max()));
            }
            co_return std::make_unique<parsed::message>(
              parsed::primitive(static_cast<int32_t>(value)));
        }
        case ::avro::AVRO_ENUM:
        case ::avro::AVRO_LONG: {
            auto [value, _] = _parser.read_varlong();
            co_return std::make_unique<parsed::message>(
              parsed::primitive(value));
        }
        case ::avro::AVRO_FLOAT:
            co_return std::make_unique<parsed::message>(
              _parser.consume_type<float>());
        case ::avro::AVRO_DOUBLE:
            co_return std::make_unique<parsed::message>(
              _parser.consume_type<double>());
        case ::avro::AVRO_BOOL:
            co_return std::make_unique<parsed::message>(_parser.read_bool());
        case ::avro::AVRO_NULL:
            co_return std::make_unique<parsed::message>(
              parsed::primitive{parsed::avro_null{}});
        case ::avro::AVRO_RECORD: {
            parsed::record record;

            for (auto i : boost::irange<size_t>(node->leaves())) {
                auto field_node = node->leafAt(i);
                record.fields.push_back(co_await parse_node(field_node, state));
            }
            co_return std::make_unique<parsed::message>(std::move(record));
        }
        case ::avro::AVRO_ARRAY: {
            auto element_count = decode_element_count();
            parsed::list ret;

            if (element_count > 0) {
                ret.elements.reserve(element_count);
                for (size_t i = 0; i < element_count; ++i) {
                    ret.elements.push_back(
                      co_await parse_node(node->leafAt(0), state));
                }
                // avro always writes 0 as the array footer, first element count
                // field is skipped if array is empty
                _parser.read_varlong();
            }
            co_return std::make_unique<parsed::message>(std::move(ret));
        }
        case ::avro::AVRO_MAP: {
            auto element_count = decode_element_count();
            parsed::map ret;
            if (element_count > 0) {
                ret.entries.reserve(element_count);
                for (size_t i = 0; i < element_count; ++i) {
                    auto key = parse_string();
                    ret.entries.emplace_back(
                      std::move(key),
                      co_await parse_node(node->leafAt(1), state));
                }
                // avro always writes 0 as the map footer, first element count
                // field is skipped if the map is empty
                _parser.read_varlong();
            }
            co_return std::make_unique<parsed::message>(std::move(ret));
        }
        case ::avro::AVRO_UNION: {
            auto [idx, _] = _parser.read_varlong();
            if (idx < 0 || idx >= static_cast<int64_t>(node->leaves())) {
                throw std::invalid_argument(fmt::format(
                  "Invalid union branch decoded, it must be greater than 0 and "
                  "smaller than number of leaves in union {}. current: {}",
                  node->leaves(),
                  idx));
            }

            auto union_branch = node->leafAt(idx);
            co_return std::make_unique<parsed::message>(parsed::avro_union{
              .branch = static_cast<size_t>(idx),
              .message = co_await parse_node(union_branch, state)});
        }
        case ::avro::AVRO_FIXED: {
            // fixed type is special as it does not have the size included in
            // the binary representation
            if (_parser.bytes_left() < node->fixedSize()) {
                throw std::invalid_argument(fmt::format(
                  "Error parsing fixed avro type. Expected fixed size: {}, "
                  "while only {} bytes left in the buffer",
                  node->fixedSize(),
                  _parser.bytes_left()));
            }

            co_return std::make_unique<parsed::message>(
              _parser.share(node->fixedSize()));
        }
        case ::avro::AVRO_SYMBOLIC:
            // this is a special case used when dealing with nested types, we
            // need to resolve the original type and parse it
            co_return co_await parse_node(::avro::resolveSymbol(node), state);
        case ::avro::AVRO_UNKNOWN:
            throw std::logic_error{
              "serde avro parser encountered an unknown type"};
        }
    }

    size_t parse_length() {
        auto [sz, _] = _parser.read_varlong();
        validate_length(sz);
        return sz;
    }

    iobuf parse_string() {
        auto sz = parse_length();
        return _parser.share(sz);
    }

    size_t decode_element_count() {
        auto [value, _] = _parser.read_varlong();
        if (value < 0) {
            _parser.read_varlong();
            value = -value;
        }
        // best effort length validation where we compare read value against the
        // left number of bytes not the element count assuming that we need at
        // least a byte per element.
        validate_length(value);
        return static_cast<size_t>(value);
    }

    ss::future<std::unique_ptr<parsed::message>> parse() {
        parser_state state;
        auto result = co_await parse_node(_schema->root(), state);
        if (_parser.bytes_left() > 0) {
            throw std::invalid_argument(fmt::format(
              "Bytes left in a buffer after parsing. Initial size: {}, left: "
              "{}",
              _initial_size,
              _parser.bytes_left()));
        }
        co_return result;
    }

private:
    void validate_length(int64_t candidate) const {
        if (candidate < 0) {
            throw std::invalid_argument(fmt::format(
              "Invalid element length: {}, length can not be negative.",
              candidate));
        }

        if (candidate > static_cast<int64_t>(_parser.bytes_left())) {
            throw std::invalid_argument(fmt::format(
              "Invalid length decoded. Decoded length: {}, bytes left: {}",
              candidate,
              _parser.bytes_left()));
        }
    }

    size_t _initial_size;
    iobuf_parser _parser;
    const ::avro::ValidSchema* _schema;
};

} // namespace

ss::future<std::unique_ptr<parsed::message>>
parse(iobuf buffer, const ::avro::ValidSchema& schema) {
    parser p(std::move(buffer), schema);
    co_return co_await p.parse();
}

} // namespace serde::avro
