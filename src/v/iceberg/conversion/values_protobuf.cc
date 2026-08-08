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

optional_value_outcome primitive_field_to_value_impl(
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
    std::unique_ptr<parsed::message> message;
    const pb::FieldDescriptor* descriptor;
    std::optional<iceberg::value>* output;
};

struct message_work {
    std::unique_ptr<parsed::message> message;
    const pb::Descriptor* descriptor;
    iceberg::struct_value* output;
    int next_field{0};
};

struct repeated_work {
    parsed::repeated repeated;
    const pb::FieldDescriptor* descriptor;
    iceberg::list_value* output;
    size_t next_element{0};
};

using parsed_map_entries = decltype(parsed::map::entries);
// Heap ownership keeps the iterator valid when the work item moves.
struct map_work {
    std::unique_ptr<parsed::map> map;
    parsed_map_entries::iterator next;
    const pb::FieldDescriptor* descriptor;
    iceberg::map_value* output;
};

struct leave_message_work {};

// Keep temporary traversal memory proportional to depth, not sibling count.
using conversion_work = std::variant<
  field_work,
  message_work,
  repeated_work,
  map_work,
  leave_message_work>;
using conversion_work_stack = absl::InlinedVector<conversion_work, 32>;

static constexpr size_t work_batch_size = 128;

std::optional<value_conversion_exception> process_message_work(
  message_work, proto_descriptors_stack&, conversion_work_stack&);

void push_repeated_work(
  parsed::repeated repeated,
  const pb::FieldDescriptor& field_descriptor,
  std::optional<iceberg::value>* output,
  conversion_work_stack& work) {
    const auto element_count = ss::visit(
      repeated.elements, [](const auto& elements) { return elements.size(); });

    auto ret = std::make_unique<iceberg::list_value>();
    ret->elements.reserve(element_count);
    auto* ret_ptr = ret.get();
    *output = std::move(ret);

    if (element_count > 0) {
        work.emplace_back(
          repeated_work{
            .repeated = std::move(repeated),
            .descriptor = &field_descriptor,
            .output = ret_ptr});
    }
}

void push_map_work(
  parsed::map parsed_map,
  const pb::FieldDescriptor& field_descriptor,
  std::optional<iceberg::value>* output,
  conversion_work_stack& work) {
    auto ret = std::make_unique<iceberg::map_value>();
    ret->kvs.reserve(parsed_map.entries.size());
    auto* ret_ptr = ret.get();
    *output = std::move(ret);

    if (parsed_map.entries.empty()) {
        return;
    }

    auto map = std::make_unique<parsed::map>(std::move(parsed_map));
    auto next = map->entries.begin();
    work.emplace_back(
      map_work{
        .map = std::move(map),
        .next = next,
        .descriptor = &field_descriptor,
        .output = ret_ptr});
}

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
    auto* ret_ptr = ret.get();
    *output = std::move(ret);

    return process_message_work(
      message_work{
        .message = std::move(message),
        .descriptor = &descriptor,
        .output = ret_ptr},
      descriptor_stack,
      work);
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

std::optional<value_conversion_exception> process_message_work(
  message_work item,
  proto_descriptors_stack& descriptor_stack,
  conversion_work_stack& work) {
    size_t processed = 0;
    while (item.next_field < item.descriptor->field_count()
           && processed++ < work_batch_size) {
        const auto* field_descriptor = item.descriptor->field(
          item.next_field++);
        auto it = item.message->fields.find(field_descriptor->number());
        auto field = it == item.message->fields.end()
                       ? std::nullopt
                       : std::make_optional<parsed::message::field>(
                           std::move(it->second));

        item.output->fields.emplace_back();
        auto* field_output = &item.output->fields.back();
        if (field_descriptor->is_map()) {
            if (!field.has_value()) {
                *field_output = field_descriptor->has_presence()
                                  ? std::nullopt
                                  : std::optional<iceberg::value>{
                                      std::make_unique<iceberg::map_value>()};
                continue;
            }
            if (item.next_field == item.descriptor->field_count()) {
                work.emplace_back(leave_message_work{});
            } else {
                work.emplace_back(std::move(item));
            }
            push_map_work(
              std::get<parsed::map>(std::move(field.value())),
              *field_descriptor,
              field_output,
              work);
            return std::nullopt;
        }
        if (field_descriptor->is_repeated()) {
            if (!field.has_value()) {
                *field_output = field_descriptor->has_presence()
                                  ? std::nullopt
                                  : std::optional<iceberg::value>{
                                      std::make_unique<iceberg::list_value>()};
                continue;
            }
            if (item.next_field == item.descriptor->field_count()) {
                work.emplace_back(leave_message_work{});
            } else {
                work.emplace_back(std::move(item));
            }
            push_repeated_work(
              std::get<parsed::repeated>(std::move(field.value())),
              *field_descriptor,
              field_output,
              work);
            return std::nullopt;
        }
        if (field_descriptor->type() == pb::FieldDescriptor::TYPE_MESSAGE) {
            std::unique_ptr<parsed::message> msg_field;
            if (field.has_value()) {
                msg_field = std::get<std::unique_ptr<parsed::message>>(
                  std::move(field.value()));
            }
            if (item.next_field == item.descriptor->field_count()) {
                work.emplace_back(leave_message_work{});
            } else {
                work.emplace_back(std::move(item));
            }
            work.emplace_back(
              field_work{
                .message = std::move(msg_field),
                .descriptor = field_descriptor,
                .output = field_output});
            return std::nullopt;
        }

        auto result = primitive_field_to_value_impl(
          std::move(field), *field_descriptor);
        if (result.has_error()) {
            return result.error();
        }
        *field_output = std::move(result.value());
    }

    if (item.next_field == item.descriptor->field_count()) {
        descriptor_stack.pop_back();
    } else {
        work.emplace_back(std::move(item));
    }
    return std::nullopt;
}

std::optional<value_conversion_exception>
process_repeated_work(repeated_work item, conversion_work_stack& work) {
    std::optional<value_conversion_exception> error;
    std::optional<field_work> child;
    size_t element_count = 0;
    ss::visit(item.repeated.elements, [&](auto& elements) {
        element_count = elements.size();
        using element_type =
          typename std::decay_t<decltype(elements)>::value_type;
        if constexpr (
          std::is_same_v<element_type, std::unique_ptr<parsed::message>>) {
            item.output->elements.emplace_back();
            child.emplace(
              field_work{
                .message = std::move(elements[item.next_element++]),
                .descriptor = item.descriptor,
                .output = &item.output->elements.back()});
        } else {
            const auto end = std::min(
              item.next_element + work_batch_size, element_count);
            while (item.next_element < end) {
                auto result = primitive_field_to_value_impl(
                  parsed::message::field{
                    std::move(elements[item.next_element++])},
                  *item.descriptor);
                if (result.has_error()) {
                    error = result.error();
                    return;
                }
                item.output->elements.push_back(std::move(result.value()));
            }
        }
    });

    if (error.has_value()) {
        return error;
    }
    if (item.next_element < element_count) {
        work.emplace_back(std::move(item));
    }
    if (child.has_value()) {
        work.emplace_back(std::move(*child));
    }
    return std::nullopt;
}

std::optional<value_conversion_exception>
process_map_work(map_work item, conversion_work_stack& work) {
    std::optional<field_work> child;
    size_t processed = 0;
    while (item.next != item.map->entries.end()
           && processed++ < work_batch_size) {
        auto& [entry_k, entry_v] = *item.next;
        ++item.next;

        auto key_result = primitive_field_to_value_impl(
          map_entry_to_field(std::move(entry_k)),
          *item.descriptor->message_type()->map_key());
        if (key_result.has_error()) {
            return key_result.error();
        }
        if (!key_result.value().has_value()) {
            return value_conversion_exception(
              fmt::format(
                "Map key must exist. Map field {}",
                item.descriptor->DebugString()));
        }

        item.output->kvs.push_back(
          iceberg::kv_value{
            .key = std::move(*key_result.value()), .val = std::nullopt});
        auto* value_output = &item.output->kvs.back().val;
        const auto* value_descriptor
          = item.descriptor->message_type()->map_value();
        if (value_descriptor->type() == pb::FieldDescriptor::TYPE_MESSAGE) {
            if (!std::holds_alternative<std::monostate>(entry_v)) {
                child.emplace(
                  field_work{
                    .message = std::get<std::unique_ptr<parsed::message>>(
                      std::move(entry_v)),
                    .descriptor = value_descriptor,
                    .output = value_output});
                break;
            }
            continue;
        }

        auto value_result = primitive_field_to_value_impl(
          map_entry_to_field(std::move(entry_v)), *value_descriptor);
        if (value_result.has_error()) {
            return value_result.error();
        }
        *value_output = std::move(value_result.value());
    }

    if (item.next != item.map->entries.end()) {
        work.emplace_back(std::move(item));
    }
    if (child.has_value()) {
        work.emplace_back(std::move(*child));
    }
    return std::nullopt;
}

ss::future<result<std::monostate, value_conversion_exception>>
process_field_work(
  field_work item,
  proto_descriptors_stack& descriptor_stack,
  conversion_work_stack& work) {
    const auto& field_descriptor = *item.descriptor;
    auto msg_field = std::move(item.message);
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
        if (std::holds_alternative<field_work>(item)) {
            auto result = co_await process_field_work(
              std::get<field_work>(std::move(item)), descriptor_stack, work);
            if (result.has_error()) {
                co_return result.error();
            }
            continue;
        }

        std::optional<value_conversion_exception> error;
        if (std::holds_alternative<message_work>(item)) {
            error = process_message_work(
              std::get<message_work>(std::move(item)), descriptor_stack, work);
        } else if (std::holds_alternative<repeated_work>(item)) {
            error = process_repeated_work(
              std::get<repeated_work>(std::move(item)), work);
        } else {
            error = process_map_work(std::get<map_work>(std::move(item)), work);
        }
        if (error.has_value()) {
            co_return std::move(*error);
        }
        co_await ss::maybe_yield();
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
