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

#include "serde/protobuf/parser.h"

#include "bytes/iobuf_parser.h"
#include "utils/vint.h"

#include <seastar/util/variant_utils.hh>

#include <google/protobuf/descriptor.h>

#include <algorithm>
#include <iterator>
#include <limits>
#include <stdexcept>
#include <type_traits>
#include <variant>

namespace serde::pb {

namespace pb = google::protobuf;

enum class wire_type : uint8_t {
    variant = 0,
    i64 = 1,
    length = 2,
    group_start = 3,
    group_end = 4,
    i32 = 5,
    max = 5,
};

struct tag {
    wire_type wire_type;
    int32_t field_number;
};

int32_t decode_zigzag(uint32_t n) {
    // NOTE: unsigned types prevent undefined behavior
    return static_cast<int32_t>((n >> 1U) ^ (~(n & 1U) + 1));
}

enum class zigzag_decode { yes, no };

template<
  typename T,
  zigzag_decode NeedsDecoding = std::is_signed_v<T> ? zigzag_decode::yes
                                                    : zigzag_decode::no>
T read_varint(iobuf_parser_base& parser) {
    // Some int32_t types are sign extended in the wire protocol, but the upper
    // bits get discarded during parsing. If we're in that case then we need to
    // read all the sign extended bits.
    constexpr bool sign_extended = std::is_signed_v<T>
                                   && NeedsDecoding == zigzag_decode::no;
    constexpr size_t limit = sizeof(T) == 8 || sign_extended ? 10 : 5;
    auto buf = parser.peek_bytes(std::min(limit, parser.bytes_left()));
    if constexpr (std::is_same_v<T, uint32_t>) {
        auto [v, len] = unsigned_vint::detail::deserialize(buf, limit);
        parser.skip(len);
        return static_cast<uint32_t>(v);
    } else if constexpr (std::is_same_v<T, int32_t>) {
        auto [v, len] = unsigned_vint::detail::deserialize(buf, limit);
        parser.skip(len);
        if constexpr (NeedsDecoding == zigzag_decode::yes) {
            return decode_zigzag(static_cast<uint32_t>(v));
        }
        return static_cast<int32_t>(static_cast<uint32_t>(v));
    } else if constexpr (std::is_same_v<T, uint64_t>) {
        auto [v, len] = unsigned_vint::detail::deserialize(buf, limit);
        parser.skip(len);
        return v;
    } else {
        static_assert(std::is_same_v<T, int64_t>);
        auto [v, len] = unsigned_vint::detail::deserialize(buf, limit);
        parser.skip(len);
        if constexpr (NeedsDecoding == zigzag_decode::yes) {
            return vint::decode_zigzag(v);
        }
        return static_cast<int64_t>(v);
    }
}

class parser {
    static constexpr int32_t top_level_field_number = -1;

public:
    ss::future<std::unique_ptr<parsed::message>>
    parse(iobuf iobuf, const pb::Descriptor& descriptor) {
        stage_message(top_level_field_number, std::move(iobuf), descriptor);
        while (true) {
            co_await parse_until_current_done();
            auto field_number = current_->field_number;
            auto completed = std::move(current_->message);
            state_.pop_back();
            if (state_.empty()) {
                co_return completed;
            }
            current_ = &state_.back();
            update_field(field_number, std::move(completed));
        }
    }

private:
    // Parse the current message (which can change during this method) until
    // it's complete and we need to pop our stack.
    //
    // This method will descend into the serialized protobuf until it reaches a
    // leaf message or completes message.
    //
    // When this method returns current_ points to a message that has been
    // finished parsed.
    ss::future<> parse_until_current_done() {
        if (current_->parser.bytes_left() == 0) {
            return ss::now();
        }
        // Wrap in ss::repeat so that preemption is possible for big/complex
        // protocol buffers
        return ss::repeat([this] {
            auto tag = read_tag();
            switch (tag.wire_type) {
            case wire_type::variant:
                read_variant_field(tag.field_number);
                break;
            case wire_type::i64:
                read_fixed64_field(tag.field_number);
                break;
            case wire_type::length:
                read_length_field(tag.field_number);
                break;
            case wire_type::i32:
                read_fixed32_field(tag.field_number);
                break;
            case wire_type::group_start:
            case wire_type::group_end:
                throw std::runtime_error(fmt::format(
                  "legacy proto2 groups not supported (field={})",
                  tag.field_number));
            }
            return current_->parser.bytes_left() > 0 ? ss::stop_iteration::no
                                                     : ss::stop_iteration::yes;
        });
    }

    void read_variant_field(int32_t field_number) {
        auto* field_descriptor = current_->descriptor->FindFieldByNumber(
          field_number);
        if (field_descriptor == nullptr) {
            std::ignore = read_varint<uint64_t>(current_->parser);
            return;
        }
        switch (field_descriptor->type()) {
        case google::protobuf::FieldDescriptor::TYPE_BOOL:
            update_field(
              *field_descriptor,
              static_cast<bool>(read_varint<int32_t>(current_->parser)));
            break;
        case google::protobuf::FieldDescriptor::TYPE_ENUM:
            update_field(
              *field_descriptor,
              static_cast<int32_t>(read_varint<uint64_t>(current_->parser)));
            break;
        case google::protobuf::FieldDescriptor::TYPE_INT32:
            update_field(
              *field_descriptor,
              read_varint<int32_t, zigzag_decode::no>(current_->parser));
            break;
        case google::protobuf::FieldDescriptor::TYPE_SINT32:
            update_field(
              *field_descriptor, read_varint<int32_t>(current_->parser));
            break;
        case google::protobuf::FieldDescriptor::TYPE_UINT32:
            update_field(
              *field_descriptor, read_varint<uint32_t>(current_->parser));
            break;
        case google::protobuf::FieldDescriptor::TYPE_INT64:
            update_field(
              *field_descriptor,
              read_varint<int64_t, zigzag_decode::no>(current_->parser));
            break;
        case google::protobuf::FieldDescriptor::TYPE_SINT64:
            update_field(
              *field_descriptor, read_varint<int64_t>(current_->parser));
            break;
        case google::protobuf::FieldDescriptor::TYPE_UINT64:
            update_field(
              *field_descriptor, read_varint<uint64_t>(current_->parser));
            break;
        case google::protobuf::FieldDescriptor::TYPE_FIXED32:
        case google::protobuf::FieldDescriptor::TYPE_SFIXED32:
        case google::protobuf::FieldDescriptor::TYPE_FIXED64:
        case google::protobuf::FieldDescriptor::TYPE_SFIXED64:
        case google::protobuf::FieldDescriptor::TYPE_FLOAT:
        case google::protobuf::FieldDescriptor::TYPE_DOUBLE:
        case google::protobuf::FieldDescriptor::TYPE_STRING:
        case google::protobuf::FieldDescriptor::TYPE_BYTES:
        case google::protobuf::FieldDescriptor::TYPE_MESSAGE:
        case google::protobuf::FieldDescriptor::TYPE_GROUP:
            // Skip fields that are incomptiable with varint wire types.
            //
            // Note that throwing here like the other fields will cause test
            // failures because as far as I can tell, this matches the observed
            // behavior from the offical C++ protobuf library.
            //
            // And vice-versa if we don't throw for the fixed size wire types
            // then we will also not match the behavior of the offical C++
            // protobuf library. It may be that the actual compatiblilty story
            // is more sophisticated, but if we've hit this case there is either
            // data corruption, incompatible breaking changes (tag reuse), or
            // just straightup the wrong message type encoded.
            break;
        }
    }

    void read_fixed64_field(int32_t field_number) {
        auto* field_descriptor = current_->descriptor->FindFieldByNumber(
          field_number);
        if (field_descriptor == nullptr) {
            current_->parser.skip(sizeof(uint64_t));
            return;
        }
        switch (field_descriptor->type()) {
        case google::protobuf::FieldDescriptor::TYPE_FIXED64:
            update_field(
              *field_descriptor, current_->parser.consume_type<uint64_t>());
            break;
        case google::protobuf::FieldDescriptor::TYPE_SFIXED64:
            update_field(
              *field_descriptor, current_->parser.consume_type<int64_t>());
            break;
        case google::protobuf::FieldDescriptor::TYPE_DOUBLE:
            update_field(
              *field_descriptor, current_->parser.consume_type<double>());
            break;
        case google::protobuf::FieldDescriptor::TYPE_BOOL:
        case google::protobuf::FieldDescriptor::TYPE_ENUM:
        case google::protobuf::FieldDescriptor::TYPE_INT32:
        case google::protobuf::FieldDescriptor::TYPE_SINT32:
        case google::protobuf::FieldDescriptor::TYPE_UINT32:
        case google::protobuf::FieldDescriptor::TYPE_FIXED32:
        case google::protobuf::FieldDescriptor::TYPE_SFIXED32:
        case google::protobuf::FieldDescriptor::TYPE_INT64:
        case google::protobuf::FieldDescriptor::TYPE_SINT64:
        case google::protobuf::FieldDescriptor::TYPE_UINT64:
        case google::protobuf::FieldDescriptor::TYPE_FLOAT:
        case google::protobuf::FieldDescriptor::TYPE_STRING:
        case google::protobuf::FieldDescriptor::TYPE_BYTES:
        case google::protobuf::FieldDescriptor::TYPE_GROUP:
        case google::protobuf::FieldDescriptor::TYPE_MESSAGE:
            throw std::runtime_error(fmt::format(
              "invalid fixed64 type: {}", field_descriptor->type_name()));
        }
    }

    void read_fixed32_field(int32_t field_number) {
        auto* field_descriptor = current_->descriptor->FindFieldByNumber(
          field_number);
        if (field_descriptor == nullptr) {
            current_->parser.skip(sizeof(uint32_t));
            return;
        }
        switch (field_descriptor->type()) {
        case google::protobuf::FieldDescriptor::TYPE_FIXED32:
            update_field(
              *field_descriptor, current_->parser.consume_type<uint32_t>());
            break;
        case google::protobuf::FieldDescriptor::TYPE_SFIXED32:
            update_field(
              *field_descriptor, current_->parser.consume_type<int32_t>());
            break;
        case google::protobuf::FieldDescriptor::TYPE_FLOAT:
            update_field(
              *field_descriptor, current_->parser.consume_type<float>());
            break;
        case google::protobuf::FieldDescriptor::TYPE_BOOL:
        case google::protobuf::FieldDescriptor::TYPE_ENUM:
        case google::protobuf::FieldDescriptor::TYPE_INT32:
        case google::protobuf::FieldDescriptor::TYPE_SINT32:
        case google::protobuf::FieldDescriptor::TYPE_UINT32:
        case google::protobuf::FieldDescriptor::TYPE_INT64:
        case google::protobuf::FieldDescriptor::TYPE_SINT64:
        case google::protobuf::FieldDescriptor::TYPE_UINT64:
        case google::protobuf::FieldDescriptor::TYPE_FIXED64:
        case google::protobuf::FieldDescriptor::TYPE_SFIXED64:
        case google::protobuf::FieldDescriptor::TYPE_DOUBLE:
        case google::protobuf::FieldDescriptor::TYPE_STRING:
        case google::protobuf::FieldDescriptor::TYPE_BYTES:
        case google::protobuf::FieldDescriptor::TYPE_GROUP:
        case google::protobuf::FieldDescriptor::TYPE_MESSAGE:
            throw std::runtime_error(fmt::format(
              "invalid fixed32 type: {}", field_descriptor->type_name()));
        }
    }

    void
    update_field(int32_t field_number, std::unique_ptr<parsed::message> value) {
        auto* field = current_->descriptor->FindFieldByNumber(field_number);
        if (field == nullptr) {
            throw std::runtime_error(
              fmt::format("unknown field number: {}", field_number));
        }
        if (field->is_map()) {
            convert_to_map_and_update(*field, std::move(value));
        } else {
            update_field(*field, std::move(value));
        }
    }

    void convert_to_map_and_update(
      const pb::FieldDescriptor& field,
      std::unique_ptr<parsed::message> value) {
        const auto* key_descriptor = field.message_type()->map_key();
        parsed::map::key key = std::monostate{};
        auto it = value->fields.find(key_descriptor->number());
        if (it != value->fields.end()) {
            key = ss::visit(
              it->second,
              [](double val) -> parsed::map::key { return val; },
              [](float val) -> parsed::map::key { return val; },
              [](int32_t val) -> parsed::map::key { return val; },
              [](int64_t val) -> parsed::map::key { return val; },
              [](uint32_t val) -> parsed::map::key { return val; },
              [](uint64_t val) -> parsed::map::key { return val; },
              [](bool val) -> parsed::map::key { return val; },
              [](iobuf& val) -> parsed::map::key { return std::move(val); },
              [](const auto&) -> parsed::map::key {
                  throw std::runtime_error(
                    "invariant: unable to convert type to map key");
              });
        }
        const auto* val_descriptor = field.message_type()->map_value();
        parsed::map::value val = std::monostate{};
        it = value->fields.find(val_descriptor->number());
        if (it != value->fields.end()) {
            val = ss::visit(
              it->second,
              [](double val) -> parsed::map::value { return val; },
              [](float val) -> parsed::map::value { return val; },
              [](int32_t val) -> parsed::map::value { return val; },
              [](int64_t val) -> parsed::map::value { return val; },
              [](uint32_t val) -> parsed::map::value { return val; },
              [](uint64_t val) -> parsed::map::value { return val; },
              [](bool val) -> parsed::map::value { return val; },
              [](iobuf& val) -> parsed::map::value { return std::move(val); },
              [](std::unique_ptr<parsed::message>& val) -> parsed::map::value {
                  return std::move(val);
              },
              [](const auto&) -> parsed::map::value {
                  throw std::runtime_error(
                    "invariant: unable to convert type to map value");
              });
        }

        it = current_->message->fields.find(field.number());
        if (it == current_->message->fields.end()) {
            parsed::map map;
            map.entries.emplace(std::move(key), std::move(val));
            current_->message->fields.emplace(field.number(), std::move(map));
        } else {
            auto& map = std::get<parsed::map>(it->second);
            map.entries.emplace(std::move(key), std::move(val));
        }
    }

    template<typename T>
    void update_field(const pb::FieldDescriptor& field, T&& value) {
        if (field.is_repeated()) {
            append_repeated_field(field.number(), std::forward<T>(value));
        } else {
            set_field(field, std::forward<T>(value));
        }
    }

    template<typename T>
    void set_field(const pb::FieldDescriptor& field, T&& value) {
        current_->message->fields.insert_or_assign(
          field.number(), std::forward<T>(value));
        // Erase all but the last oneof field when there are multiple.
        auto* oneof = field.containing_oneof();
        if (oneof == nullptr) {
            return;
        }
        for (int i = 0; i < oneof->field_count(); ++i) {
            auto oneof_field = oneof->field(i);
            if (oneof_field->number() == field.number()) {
                continue;
            }
            current_->message->fields.erase(oneof_field->number());
        }
    }

    /**
     * Update a repeated field. If T is itself `repeated` then concat the
     * two sequences, otherwise append the last value.
     *
     * field_number must be of a repeated type.
     */
    template<typename T>
    void append_repeated_field(int32_t field_number, T&& value) {
        auto it = current_->message->fields.find(field_number);
        if (it == current_->message->fields.end()) {
            if constexpr (std::is_same_v<T, parsed::repeated>) {
                current_->message->fields.emplace(
                  field_number, std::forward<T>(value));
            } else {
                chunked_vector<T> vec;
                vec.push_back(std::forward<T>(value));
                parsed::repeated repeated{std::move(vec)};
                current_->message->fields.emplace(
                  field_number, std::move(repeated));
            }
        } else {
            auto& repeated = std::get<parsed::repeated>(it->second);
            if constexpr (std::is_same_v<T, parsed::repeated>) {
                std::visit(
                  [&value](auto& vec) {
                      using vec_t = std::decay_t<decltype(vec)>;
                      auto& elems = std::get<vec_t>(value.elements);
                      std::move(
                        std::make_move_iterator(elems.begin()),
                        std::make_move_iterator(elems.end()),
                        std::back_inserter(vec));
                  },
                  repeated.elements);
            } else {
                auto& vec = std::get<chunked_vector<T>>(repeated.elements);
                vec.push_back(std::forward<T>(value));
            }
        }
    }

    void stage_message(
      int32_t field_number, iobuf iobuf, const pb::Descriptor& descriptor) {
        // Matches the golang max message depth (the highest of all runtimes
        // as far as I understand it).
        constexpr size_t max_nested_message_depth = 10000;
        if (state_.size() >= max_nested_message_depth) {
            throw std::runtime_error("max nested message depth reached");
        }
        current_ = &state_.emplace_back(
          field_number,
          iobuf_parser(std::move(iobuf)),
          std::make_unique<parsed::message>(),
          &descriptor);
    }

    tag read_tag() {
        auto tag_value = read_varint<uint64_t>(current_->parser);
        constexpr uint64_t wire_mask = 0b111;
        uint64_t wtype = tag_value & wire_mask;
        if (wtype > static_cast<int64_t>(wire_type::max)) [[unlikely]] {
            throw std::runtime_error(
              fmt::format("invalid wire type: {}", wtype));
        }
        uint64_t field_number = tag_value >> 3ULL;
        constexpr auto max_field_number = static_cast<uint64_t>(
          std::numeric_limits<int32_t>::max());
        if (field_number > max_field_number || field_number == 0) [[unlikely]] {
            throw std::runtime_error(
              fmt::format("invalid field number: {}", field_number));
        }
        return {
          static_cast<wire_type>(wtype), static_cast<int32_t>(field_number)};
    }

    void read_packed_elements(
      const pb::FieldDescriptor& field, size_t amount, auto reader) {
        chunked_vector<std::invoke_result_t<decltype(reader)>> vec;
        if (amount > current_->parser.bytes_left()) {
            throw std::runtime_error(fmt::format(
              "invalid packed field field: (bytes_needed={}, "
              "bytes_left={})",
              amount,
              current_->parser.bytes_left()));
        }
        auto target = current_->parser.bytes_consumed() + amount;
        while (current_->parser.bytes_consumed() < target) {
            vec.push_back(reader());
        }
        if (field.is_packable()) {
            append_repeated_field(
              field.number(), parsed::repeated(std::move(vec)));
        }
    }

    void read_length_field(int32_t field_number) {
        auto length = read_varint<int32_t, zigzag_decode::no>(current_->parser);
        if (length < 0) {
            throw std::runtime_error(
              fmt::format("invalid length field: (length={})", length));
        }
        const auto* descriptor = current_->descriptor->FindFieldByNumber(
          field_number);
        if (!descriptor) {
            current_->parser.skip(length);
            return;
        }
        switch (descriptor->type()) {
        case pb::FieldDescriptor::TYPE_BOOL:
            read_packed_elements(*descriptor, length, [this] {
                return static_cast<bool>(
                  read_varint<int32_t>(current_->parser));
            });
            break;
        case pb::FieldDescriptor::TYPE_ENUM:
            read_packed_elements(*descriptor, length, [this] {
                return read_varint<int32_t, zigzag_decode::no>(
                  current_->parser);
            });
            break;
        case pb::FieldDescriptor::TYPE_INT32:
            read_packed_elements(*descriptor, length, [this] {
                return read_varint<int32_t, zigzag_decode::no>(
                  current_->parser);
            });
            break;
        case pb::FieldDescriptor::TYPE_SINT32:
            read_packed_elements(*descriptor, length, [this] {
                return read_varint<int32_t>(current_->parser);
            });
            break;
        case pb::FieldDescriptor::TYPE_UINT32:
            read_packed_elements(*descriptor, length, [this] {
                return read_varint<uint32_t>(current_->parser);
            });
            break;
        case pb::FieldDescriptor::TYPE_FIXED32:
            read_packed_elements(*descriptor, length, [this] {
                return current_->parser.consume_type<uint32_t>();
            });
            break;
        case pb::FieldDescriptor::TYPE_SFIXED32:
            read_packed_elements(*descriptor, length, [this] {
                return current_->parser.consume_type<int32_t>();
            });
            break;
        case pb::FieldDescriptor::TYPE_INT64:
            read_packed_elements(*descriptor, length, [this] {
                return read_varint<int64_t, zigzag_decode::no>(
                  current_->parser);
            });
            break;
        case pb::FieldDescriptor::TYPE_UINT64:
            read_packed_elements(*descriptor, length, [this] {
                return read_varint<uint64_t>(current_->parser);
            });
            break;
        case pb::FieldDescriptor::TYPE_SINT64:
            read_packed_elements(*descriptor, length, [this] {
                return read_varint<int64_t>(current_->parser);
            });
            break;
        case pb::FieldDescriptor::TYPE_FIXED64:
            read_packed_elements(*descriptor, length, [this] {
                return current_->parser.consume_type<uint64_t>();
            });
            break;
        case pb::FieldDescriptor::TYPE_SFIXED64:
            read_packed_elements(*descriptor, length, [this] {
                return current_->parser.consume_type<int64_t>();
            });
            break;
        case pb::FieldDescriptor::TYPE_FLOAT:
            read_packed_elements(*descriptor, length, [this] {
                return current_->parser.consume_type<float>();
            });
            break;
        case pb::FieldDescriptor::TYPE_DOUBLE:
            read_packed_elements(*descriptor, length, [this] {
                return current_->parser.consume_type<double>();
            });
            break;
        case pb::FieldDescriptor::TYPE_STRING:
        case pb::FieldDescriptor::TYPE_BYTES: {
            auto buf = current_->parser.share(length);
            update_field(*descriptor, std::move(buf));
            break;
        }
        case pb::FieldDescriptor::TYPE_MESSAGE: {
            auto buf = current_->parser.share(length);
            stage_message(
              descriptor->number(),
              std::move(buf),
              *descriptor->message_type());
            break;
        }
        case pb::FieldDescriptor::TYPE_GROUP:
            throw std::runtime_error(fmt::format(
              "legacy proto2 groups not supported (field={})",
              descriptor->number()));
        }
    }

    // The state of the current message being parsed. As we encounter new
    // messages we will push an entry onto state_ so that we are not stack
    // bound with respect to protobuf nested message depth.
    struct state {
        // The field_number of the proto to push back onto it's parent
        // message.
        int32_t field_number;
        // The parser for this current protobuf only
        iobuf_parser parser;
        // The partially constructed message that is the result of the parse
        // operation when the parser is empty.
        std::unique_ptr<parsed::message> message;
        // The descriptor describing the current message being parsed.
        const pb::Descriptor* descriptor;
    };

    chunked_vector<state> state_;
    // A shortcut pointer to the top of the stack (state_.back()).
    state* current_ = nullptr;
};

ss::future<std::unique_ptr<parsed::message>>
parse(iobuf iobuf, const pb::Descriptor& descriptor) {
    parser p;
    co_return co_await p.parse(std::move(iobuf), descriptor);
}

} // namespace serde::pb
