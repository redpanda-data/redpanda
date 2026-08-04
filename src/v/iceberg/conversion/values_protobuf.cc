/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "iceberg/conversion/values_protobuf.h"

#include "absl/time/time.h"
#include "bytes/iobuf.h"
#include "container/chunked_vector.h"
#include "iceberg/conversion/conversion_outcome.h"
#include "iceberg/conversion/protobuf_utils.h"
#include "iceberg/values.h"
#include "serde/json/writer.h"

#include <seastar/core/coroutine.hh>
#include <seastar/util/log.hh>
#include <seastar/util/variant_utils.hh>

#include <absl/container/inlined_vector.h>
#include <fmt/core.h>

#include <cmath>

namespace iceberg {
namespace pb = google::protobuf;
namespace parsed = serde::pb::parsed;
namespace {

value_conversion_exception
type_conversion_error(const pb::FieldDescriptor& fd) {
    return value_conversion_exception(
      fmt::format(
        "Protocol buffers type '{}' conversion is not supported",
        fd.type_name()));
}

std::optional<value_conversion_exception> check_recursion_depth(int depth) {
    if (depth > max_recursion_depth) {
        return value_conversion_exception(
          fmt::format(
            "Maximum recursion depth {} exceeded", max_recursion_depth));
    }
    return std::nullopt;
}

template<typename BaseT, typename IcebergT, typename DefaultF>
std::optional<IcebergT> convert(
  std::optional<parsed::message::field> f,
  const pb::FieldDescriptor& fd,
  DefaultF get_default) {
    if (f.has_value()) {
        return IcebergT(std::get<BaseT>(std::move(f.value())));
    }
    // When field has presence and it has no value it must explicitly be set to
    // nullopt
    if (fd.has_presence() && !fd.has_default_value()) {
        return std::nullopt;
    }

    if constexpr (std::is_same_v<IcebergT, iceberg::string_value>) {
        return IcebergT(iobuf::from(std::invoke(get_default, &fd)));
    } else {
        return IcebergT(std::invoke(get_default, &fd));
    }
}

template<typename DefaultF>
std::optional<iceberg::string_value> convert_u64_as_string(
  std::optional<parsed::message::field> f,
  const pb::FieldDescriptor& fd,
  DefaultF get_default) {
    if (f.has_value()) {
        auto n = std::get<uint64_t>(std::move(f.value()));
        return iceberg::string_value(iobuf::from(std::to_string(n)));
    }
    // When field has presence and it has no value it must explicitly be set to
    // nullopt
    if (fd.has_presence() && !fd.has_default_value()) {
        return std::nullopt;
    }

    return iceberg::string_value(
      iobuf::from(std::to_string(std::invoke(get_default, &fd))));
}

// converts a struct to an iceberg value
ss::future<optional_value_outcome> message_to_value(
  std::unique_ptr<parsed::message> message,
  const pb::Descriptor& descriptor,
  proto_descriptors_stack& stack);

// converts a primitive field to an iceberg value
optional_value_outcome primitive_field_to_value_impl(
  std::optional<parsed::message::field> field,
  const pb::FieldDescriptor& field_descriptor);

ss::future<optional_value_outcome> primitive_field_to_value(
  std::optional<parsed::message::field> field,
  const pb::FieldDescriptor& field_descriptor);

template<typename T>
std::optional<parsed::message::field> map_entry_to_field(T entry) {
    return ss::visit(
      std::move(entry),
      [](std::monostate) -> std::optional<parsed::message::field> {
          return std::nullopt;
      },
      [](auto value) -> std::optional<parsed::message::field> {
          return parsed::message::field{std::move(value)};
      });
}

struct field_work {
    std::optional<parsed::message::field> field;
    const pb::FieldDescriptor* descriptor;
    std::optional<iceberg::value>* output;
    bool is_repeated_element{false};
};

struct leave_message_work {};

using conversion_work = std::variant<field_work, leave_message_work>;

// Recursive coroutine calls prevent heap-allocation elision, so retain the
// message traversal state explicitly while coroutine leaves process fields.
class conversion_work_stack {
public:
    template<typename T>
    void emplace_back(T&& item) {
        if (_overflow.empty() && _inline_items.size() < inline_capacity) {
            _inline_items.emplace_back(std::forward<T>(item));
        } else {
            _overflow.emplace_back(std::forward<T>(item));
        }
    }

    bool empty() const { return _inline_items.empty() && _overflow.empty(); }

    conversion_work& back() {
        return _overflow.empty() ? _inline_items.back() : _overflow.back();
    }

    void pop_back() {
        if (_overflow.empty()) {
            _inline_items.pop_back();
        } else {
            _overflow.pop_back();
        }
    }

private:
    static constexpr size_t inline_capacity = 32;
    absl::InlinedVector<conversion_work, inline_capacity> _inline_items;
    chunked_vector<conversion_work> _overflow;
};

std::optional<value_conversion_exception> push_message_work(
  std::unique_ptr<parsed::message> message,
  const pb::Descriptor& descriptor,
  std::optional<iceberg::value>* output,
  proto_descriptors_stack& descriptor_stack,
  conversion_work_stack& work) {
    if (message == nullptr) {
        *output = std::nullopt;
        return std::nullopt;
    }

    if (is_recursive_type(descriptor, descriptor_stack)) {
        return value_conversion_exception(
          fmt::format(
            "Recursive message types are not supported. Descriptor: {}",
            descriptor.DebugString()));
    }
    if (descriptor_stack.size() > max_recursion_depth) {
        return value_conversion_exception(
          fmt::format(
            "Exceeded maximum recursion depth. Descriptor: {}",
            descriptor.DebugString()));
    }

    descriptor_stack.push_back(&descriptor);
    auto ret = std::make_unique<iceberg::struct_value>();
    ret->fields.reserve(descriptor.field_count());
    for (int i = 0; i < descriptor.field_count(); ++i) {
        ret->fields.emplace_back();
    }
    auto* ret_ptr = ret.get();
    *output = std::move(ret);

    work.emplace_back(leave_message_work{});
    for (int i = descriptor.field_count(); i > 0; --i) {
        const auto* field_descriptor = descriptor.field(i - 1);
        auto it = message->fields.find(field_descriptor->number());
        auto field = it == message->fields.end()
                       ? std::nullopt
                       : std::make_optional<parsed::message::field>(
                           std::move(it->second));
        if (
          !field_descriptor->is_map() && !field_descriptor->is_repeated()
          && field_descriptor->type() != pb::FieldDescriptor::TYPE_MESSAGE) {
            auto result = primitive_field_to_value_impl(
              std::move(field), *field_descriptor);
            if (result.has_error()) {
                return result.error();
            }
            ret_ptr->fields[i - 1] = std::move(result.value());
            continue;
        }
        work.emplace_back(
          field_work{
            .field = std::move(field),
            .descriptor = field_descriptor,
            .output = &ret_ptr->fields[i - 1]});
    }
    return std::nullopt;
}

void push_repeated_work(
  parsed::repeated repeated,
  const pb::FieldDescriptor& field_descriptor,
  std::optional<iceberg::value>* output,
  conversion_work_stack& work) {
    auto ret = std::make_unique<iceberg::list_value>();
    auto* ret_ptr = ret.get();
    *output = std::move(ret);

    ss::visit(
      std::move(repeated.elements),
      [&field_descriptor, ret_ptr, &work](auto elements) {
          ret_ptr->elements.reserve(elements.size());
          for (size_t i = 0; i < elements.size(); ++i) {
              ret_ptr->elements.emplace_back();
          }
          for (size_t i = elements.size(); i > 0; --i) {
              work.emplace_back(
                field_work{
                  .field = parsed::message::field{std::move(elements[i - 1])},
                  .descriptor = &field_descriptor,
                  .output = &ret_ptr->elements[i - 1],
                  .is_repeated_element = true});
          }
      });
}

ss::future<optional_value_outcome>
convert_timestamp(std::unique_ptr<parsed::message> message) {
    if (message == nullptr) {
        co_return std::nullopt;
    }
    constexpr int seconds_tag = 1;
    constexpr int nanos_tag = 2;
    auto it = message->fields.find(seconds_tag);
    absl::Time ts = absl::UnixEpoch();
    if (it != message->fields.end()) {
        ts += absl::Seconds(std::get<int64_t>(std::move(it->second)));
    }
    it = message->fields.find(nanos_tag);
    if (it != message->fields.end()) {
        ts += absl::Nanoseconds(std::get<int32_t>(std::move(it->second)));
    }
    co_return iceberg::timestamptz_value{absl::ToUnixMicros(ts)};
}

// Forward declaration for recursive calls
ss::future<result<std::monostate, value_conversion_exception>>
serialize_protobuf_value_to_json(
  serde::json::writer& writer, const parsed::message& value_msg, int depth);

ss::future<result<std::monostate, value_conversion_exception>>
serialize_protobuf_list_to_json(
  serde::json::writer& writer, const parsed::message& list, int depth) {
    if (auto err = check_recursion_depth(depth); err.has_value()) {
        co_return *err;
    }

    writer.begin_array();
    if (auto it = list.fields.find(1); it != list.fields.end()) {
        const auto& repeated_data = std::get<parsed::repeated>(it->second);
        const auto& messages
          = std::get<chunked_vector<std::unique_ptr<parsed::message>>>(
            repeated_data.elements);

        for (const auto& msg_ptr : messages) {
            auto result = co_await serialize_protobuf_value_to_json(
              writer, *msg_ptr, depth + 1);
            if (result.has_error()) {
                co_return result.error();
            }
        }
    }
    writer.end_array();
    co_return std::monostate{};
}

ss::future<result<std::monostate, value_conversion_exception>>
serialize_protobuf_map_to_json(
  serde::json::writer& writer, const parsed::message& map, int depth) {
    if (auto err = check_recursion_depth(depth); err.has_value()) {
        co_return *err;
    }

    writer.begin_object();
    if (auto it = map.fields.find(1); it != map.fields.end()) {
        const auto& map_data = std::get<parsed::map>(it->second);
        for (const auto& [key, value] : map_data.entries) {
            // google.protobuf.Struct maps always have string keys
            writer.key(std::get<iobuf>(key));
            // Convert value
            const auto& value_msg_ptr
              = std::get<std::unique_ptr<parsed::message>>(value);
            auto result = co_await serialize_protobuf_value_to_json(
              writer, *value_msg_ptr, depth + 1);
            if (result.has_error()) {
                co_return result.error();
            }
        }
    }
    writer.end_object();
    co_return std::monostate{};
}

ss::future<result<std::monostate, value_conversion_exception>>
serialize_protobuf_value_to_json(
  serde::json::writer& writer, const parsed::message& value_msg, int depth) {
    if (auto err = check_recursion_depth(depth); err.has_value()) {
        co_return *err;
    }

    for (const auto& [field_num, field_value] : value_msg.fields) {
        switch (field_num) {
        case 1: // null
            writer.null();
            co_return std::monostate{};
        case 2: { // number
            auto d = std::get<double>(field_value);
            if (std::isnan(d) || std::isinf(d)) {
                co_return value_conversion_exception(
                  "NaN and Infinity are not supported in JSON");
            }
            writer.number(d);
            co_return std::monostate{};
        }
        case 3: // string
            writer.string(std::get<iobuf>(field_value));
            co_return std::monostate{};
        case 4: // bool
            writer.boolean(std::get<bool>(field_value));
            co_return std::monostate{};
        case 5: { // struct
            const auto& map = std::get<std::unique_ptr<parsed::message>>(
              field_value);
            co_return co_await serialize_protobuf_map_to_json(
              writer, *map, depth + 1);
        }
        case 6: { // list
            const auto& list = std::get<std::unique_ptr<parsed::message>>(
              field_value);
            co_return co_await serialize_protobuf_list_to_json(
              writer, *list, depth + 1);
        }
        }
    }
    // If no field is set, default to null (per libprotobuf behavior)
    writer.null();
    co_return std::monostate{};
}

ss::future<optional_value_outcome> convert_struct_to_json(
  std::unique_ptr<parsed::message> message,
  const proto_descriptors_stack& stack) {
    if (message == nullptr) {
        co_return std::nullopt;
    }

    serde::json::writer writer;
    auto result = co_await serialize_protobuf_map_to_json(
      writer, *message, stack.size());
    if (result.has_error()) {
        co_return result.error();
    }
    co_return iceberg::string_value{std::move(writer).finish()};
}

ss::future<optional_value_outcome> convert_value_to_json(
  std::unique_ptr<parsed::message> message,
  const proto_descriptors_stack& stack) {
    if (message == nullptr) {
        co_return std::nullopt;
    }

    serde::json::writer writer;
    auto result = co_await serialize_protobuf_value_to_json(
      writer, *message, stack.size());
    if (result.has_error()) {
        co_return result.error();
    }
    co_return iceberg::string_value{std::move(writer).finish()};
}

ss::future<optional_value_outcome> convert_list_value_to_json(
  std::unique_ptr<parsed::message> message,
  const proto_descriptors_stack& stack) {
    if (message == nullptr) {
        co_return std::nullopt;
    }

    serde::json::writer writer;
    auto result = co_await serialize_protobuf_list_to_json(
      writer, *message, stack.size());
    if (result.has_error()) {
        co_return result.error();
    }
    co_return iceberg::string_value{std::move(writer).finish()};
}
ss::future<optional_value_outcome>
convert_date(std::unique_ptr<parsed::message> message) {
    if (message == nullptr) {
        co_return std::nullopt;
    }

    constexpr int date_tag = 1;
    auto it = message->fields.find(date_tag);
    if (it == message->fields.end()) {
        co_return value_conversion_exception(
          fmt::format("Date message missing 'date' field"));
    }
    int32_t date = std::get<int32_t>(std::move(it->second));

    co_return iceberg::date_value{date};
}

optional_value_outcome primitive_field_to_value_impl(
  std::optional<parsed::message::field> field,
  const pb::FieldDescriptor& field_descriptor) {
    switch (field_descriptor.type()) {
    case pb::FieldDescriptor::TYPE_DOUBLE:
        return convert<double, iceberg::double_value>(
          std::move(field),
          field_descriptor,
          &pb::FieldDescriptor::default_value_double);
    case pb::FieldDescriptor::TYPE_FLOAT:
        return convert<float, iceberg::float_value>(
          std::move(field),
          field_descriptor,
          &pb::FieldDescriptor::default_value_float);
    case pb::FieldDescriptor::TYPE_UINT32:
    case pb::FieldDescriptor::TYPE_FIXED32:
        // casting uint32 to long value to prevent overflow
        return convert<uint32_t, iceberg::long_value>(
          std::move(field),
          field_descriptor,
          &pb::FieldDescriptor::default_value_uint32);
    case pb::FieldDescriptor::TYPE_SFIXED64:
    case pb::FieldDescriptor::TYPE_INT64:
    case pb::FieldDescriptor::TYPE_SINT64:
        return convert<int64_t, iceberg::long_value>(
          std::move(field),
          field_descriptor,
          &pb::FieldDescriptor::default_value_int64);
    // unsigned 64 bit integers fallback to strings
    case pb::FieldDescriptor::TYPE_UINT64:
    case pb::FieldDescriptor::TYPE_FIXED64:
        return convert_u64_as_string(
          std::move(field),
          field_descriptor,
          &pb::FieldDescriptor::default_value_uint64);
    case pb::FieldDescriptor::TYPE_INT32:
    case pb::FieldDescriptor::TYPE_SFIXED32:
    case pb::FieldDescriptor::TYPE_SINT32:
        return convert<int32_t, iceberg::int_value>(
          std::move(field),
          field_descriptor,
          &pb::FieldDescriptor::default_value_int32);
    case pb::FieldDescriptor::TYPE_ENUM:
        if (!field.has_value()) {
            if (field_descriptor.has_presence()) {
                return std::nullopt;
            }
            return iceberg::string_value(
              iobuf::from(field_descriptor.default_value_enum()->name()));
        } else {
            auto enum_number = std::get<int32_t>(std::move(field.value()));
            auto enum_value_desc
              = field_descriptor.enum_type()->FindValueByNumber(enum_number);
            if (!enum_value_desc) {
                // Use the default for invalid enum values (closed enums).
                return iceberg::string_value(
                  iobuf::from(field_descriptor.default_value_enum()->name()));
            }
            return iceberg::string_value(iobuf::from(enum_value_desc->name()));
        }
    case pb::FieldDescriptor::TYPE_BOOL:
        return convert<bool, iceberg::boolean_value>(
          std::move(field),
          field_descriptor,
          &pb::FieldDescriptor::default_value_bool);
    case pb::FieldDescriptor::TYPE_STRING:
        return convert<iobuf, iceberg::string_value>(
          std::move(field),
          field_descriptor,
          &pb::FieldDescriptor::default_value_string);
    case pb::FieldDescriptor::TYPE_GROUP:
        return type_conversion_error(field_descriptor);
    case pb::FieldDescriptor::TYPE_MESSAGE:
        return value_conversion_exception(
          "Unexpected message field in primitive conversion");
    case pb::FieldDescriptor::TYPE_BYTES:
        if (!field.has_value()) {
            if (field_descriptor.has_presence()) {
                return std::nullopt;
            }
            return iceberg::binary_value{};
        }
        return iceberg::binary_value(std::get<iobuf>(std::move(field.value())));
    }
}

ss::future<optional_value_outcome> primitive_field_to_value(
  std::optional<parsed::message::field> field,
  const pb::FieldDescriptor& field_descriptor) {
    co_return primitive_field_to_value_impl(std::move(field), field_descriptor);
}

ss::future<result<std::monostate, value_conversion_exception>>
process_field_work(
  field_work item,
  proto_descriptors_stack& descriptor_stack,
  conversion_work_stack& work) {
    const auto& field_descriptor = *item.descriptor;

    if (field_descriptor.is_map()) {
        if (!item.field.has_value()) {
            *item.output = field_descriptor.has_presence()
                             ? std::nullopt
                             : std::optional<iceberg::value>{
                                 std::make_unique<iceberg::map_value>()};
            co_return std::monostate{};
        }

        auto parsed_map = std::get<parsed::map>(std::move(item.field.value()));
        auto ret = std::make_unique<iceberg::map_value>();
        ret->kvs.reserve(parsed_map.entries.size());
        auto* ret_ptr = ret.get();
        *item.output = std::move(ret);

        for (auto& [entry_k, entry_v] : parsed_map.entries) {
            auto key_result = co_await primitive_field_to_value(
              map_entry_to_field(std::move(entry_k)),
              *field_descriptor.message_type()->map_key());
            if (key_result.has_error()) {
                co_return key_result.error();
            }
            if (!key_result.value().has_value()) {
                co_return value_conversion_exception(
                  fmt::format(
                    "Map key must exist. Map field {}",
                    field_descriptor.DebugString()));
            }

            ret_ptr->kvs.push_back(
              iceberg::kv_value{
                .key = std::move(*key_result.value()), .val = std::nullopt});
            auto value_field = map_entry_to_field(std::move(entry_v));
            if (value_field.has_value()) {
                work.emplace_back(
                  field_work{
                    .field = std::move(value_field),
                    .descriptor = field_descriptor.message_type()->map_value(),
                    .output = &ret_ptr->kvs.back().val});
            }
        }
        co_return std::monostate{};
    }

    if (field_descriptor.is_repeated() && !item.is_repeated_element) {
        if (!item.field.has_value()) {
            *item.output = field_descriptor.has_presence()
                             ? std::nullopt
                             : std::optional<iceberg::value>{
                                 std::make_unique<iceberg::list_value>()};
            co_return std::monostate{};
        }
        push_repeated_work(
          std::get<parsed::repeated>(std::move(item.field.value())),
          field_descriptor,
          item.output,
          work);
        co_return std::monostate{};
    }

    if (field_descriptor.type() != pb::FieldDescriptor::TYPE_MESSAGE) {
        auto result = co_await primitive_field_to_value(
          std::move(item.field), field_descriptor);
        if (result.has_error()) {
            co_return result.error();
        }
        *item.output = std::move(result.value());
        co_return std::monostate{};
    }

    std::unique_ptr<parsed::message> msg_field = nullptr;
    if (item.field.has_value()) {
        msg_field = std::get<std::unique_ptr<parsed::message>>(
          std::move(item.field.value()));
    }
    optional_value_outcome result = std::nullopt;
    if (
      field_descriptor.message_type()->well_known_type()
      == pb::Descriptor::WELLKNOWNTYPE_TIMESTAMP) {
        result = co_await convert_timestamp(std::move(msg_field));
    } else if (
      field_descriptor.message_type()->well_known_type()
      == pb::Descriptor::WELLKNOWNTYPE_STRUCT) {
        result = co_await convert_struct_to_json(
          std::move(msg_field), descriptor_stack);
    } else if (
      field_descriptor.message_type()->well_known_type()
      == pb::Descriptor::WELLKNOWNTYPE_VALUE) {
        result = co_await convert_value_to_json(
          std::move(msg_field), descriptor_stack);
    } else if (
      field_descriptor.message_type()->well_known_type()
      == pb::Descriptor::WELLKNOWNTYPE_LISTVALUE) {
        result = co_await convert_list_value_to_json(
          std::move(msg_field), descriptor_stack);
    } else if (
      field_descriptor.message_type()->full_name()
      == protobuf::datalake_date_type) {
        result = co_await convert_date(std::move(msg_field));
    } else if (
      field_descriptor.message_type()->full_name().starts_with(
        protobuf::datalake_well_known_type_prefix)) {
        co_return value_conversion_exception(
          fmt::format(
            "Protocol buffer field {} not supported - unhandled "
            "redpanda.datalake type {}",
            field_descriptor.DebugString(),
            field_descriptor.message_type()->full_name()));
    } else {
        if (
          auto err = push_message_work(
            std::move(msg_field),
            *field_descriptor.message_type(),
            item.output,
            descriptor_stack,
            work);
          err.has_value()) {
            co_return std::move(*err);
        }
        co_return std::monostate{};
    }

    if (result.has_error()) {
        co_return result.error();
    }
    *item.output = std::move(result.value());
    co_return std::monostate{};
}

ss::future<optional_value_outcome> message_to_value(
  std::unique_ptr<parsed::message> message,
  const pb::Descriptor& descriptor,
  proto_descriptors_stack& descriptor_stack) {
    std::optional<iceberg::value> ret;
    conversion_work_stack work;
    if (
      auto err = push_message_work(
        std::move(message), descriptor, &ret, descriptor_stack, work);
      err.has_value()) {
        co_return std::move(*err);
    }

    while (!work.empty()) {
        auto item = std::move(work.back());
        work.pop_back();
        if (std::holds_alternative<leave_message_work>(item)) {
            descriptor_stack.pop_back();
            continue;
        }

        auto result = co_await process_field_work(
          std::get<field_work>(std::move(item)), descriptor_stack, work);
        if (result.has_error()) {
            co_return result.error();
        }
    }
    co_return ret;
}
} // namespace
ss::future<optional_value_outcome> proto_parsed_message_to_value(
  std::unique_ptr<parsed::message> message, const pb::Descriptor& descriptor) {
    proto_descriptors_stack stack;
    co_return co_await message_to_value(std::move(message), descriptor, stack);
}

ss::future<optional_value_outcome>
deserialize_protobuf(iobuf buffer, const pb::Descriptor& type_descriptor) {
    try {
        auto msg_ptr = co_await serde::pb::parse(
          std::move(buffer), type_descriptor);

        co_return co_await proto_parsed_message_to_value(
          std::move(msg_ptr), type_descriptor);
    } catch (...) {
        co_return value_conversion_exception(
          fmt::format(
            "exception thrown while parsing protobuf - {}",
            std::current_exception()));
    }
}

} // namespace iceberg
