/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/values_protobuf.h"

#include "bytes/iobuf.h"
#include "container/fragmented_vector.h"
#include "datalake/conversion_outcome.h"
#include "datalake/protobuf_utils.h"
#include "iceberg/values.h"
#include "ssx/future-util.h"

#include <seastar/util/variant_utils.hh>

#include <fmt/core.h>

namespace datalake {
namespace pb = google::protobuf;
namespace parsed = serde::pb::parsed;
namespace {

value_conversion_exception
type_conversion_error(const pb::FieldDescriptor& fd) {
    return value_conversion_exception(fmt::format(
      "Protocol buffers type '{}' conversion is not supported",
      fd.type_name()));
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

// converts a struct field to an iceberg value, it supports maps and repeated
// fields
ss::future<optional_value_outcome> message_field_to_value(
  std::optional<parsed::message::field> field,
  const pb::FieldDescriptor& field_descriptor,
  proto_descriptors_stack& stack);

// converts a single element to iceberg value
ss::future<optional_value_outcome> single_field_to_value(
  std::optional<parsed::message::field> field,
  const pb::FieldDescriptor& field_descriptor,
  proto_descriptors_stack& stack);

template<typename SourceT>
ss::future<value_outcome> convert_repeated_elements(
  chunked_vector<SourceT> elements,
  const pb::FieldDescriptor& field_descriptor,
  proto_descriptors_stack& stack) {
    auto ret = std::make_unique<iceberg::list_value>();
    ret->elements.reserve(elements.size());
    for (typename decltype(elements)::reference element : elements) {
        auto result = co_await single_field_to_value(
          std::move(element), field_descriptor, stack);
        if (result.has_error()) {
            co_return result.error();
        }
        ret->elements.push_back(std::move(result.value()));
    };
    co_return value_outcome{std::move(ret)};
}

ss::future<optional_value_outcome> convert_repeated(
  std::optional<serde::pb::parsed::message::field> list_field,
  const pb::FieldDescriptor& field_descriptor,
  proto_descriptors_stack& stack) {
    if (!list_field.has_value()) {
        if (field_descriptor.has_presence()) {
            return ssx::now<optional_value_outcome>(std::nullopt);
        }
        return ssx::now<optional_value_outcome>(
          std::make_unique<iceberg::list_value>());
    }
    auto list_variant
      = std::get<parsed::repeated>(std::move(list_field.value())).elements;

    return ss::visit(
             std::move(list_variant),
             [&field_descriptor, &stack](auto& list) {
                 return convert_repeated_elements(
                   std::move(list), field_descriptor, stack);
             })
      .then([](value_outcome vo) {
          if (vo.has_error()) {
              return optional_value_outcome(vo.error());
          }

          return optional_value_outcome(std::move(vo.value()));
      });
}
ss::future<optional_value_outcome> convert_map(
  std::optional<parsed::message::field> field,
  const pb::FieldDescriptor& field_descriptor,
  proto_descriptors_stack& stack) {
    if (!field.has_value()) {
        if (field_descriptor.has_presence()) {
            co_return std::nullopt;
        }
        // if no presence is tracked return an empty map
        co_return std::make_unique<iceberg::map_value>();
    }
    auto ret = std::make_unique<iceberg::map_value>();
    auto parsed_map = std::get<parsed::map>(std::move(field.value()));
    ret->kvs.reserve(parsed_map.entries.size());

    for (auto& [entry_k, entry_v] : parsed_map.entries) {
        /**
         * Convert key, if a parsed key is represented by a monostate we assume
         * it has a default value
         */
        auto key_result = co_await ss::visit(
          std::move(entry_k),
          [&field_descriptor, &stack](std::monostate) {
              // default value
              return single_field_to_value(
                std::nullopt,
                *field_descriptor.message_type()->map_key(),
                stack);
          },
          [&field_descriptor, &stack](auto& value) {
              return single_field_to_value(
                std::move(value),
                *field_descriptor.message_type()->map_key(),
                stack);
          });

        if (key_result.has_error()) {
            co_return key_result.error();
        }

        if (!key_result.value().has_value()) {
            co_return value_conversion_exception(fmt::format(
              "Map key must exist. Map field {}",
              field_descriptor.DebugString()));
        }

        optional_value_outcome value_result = co_await ss::visit(
          std::move(entry_v),
          [](std::monostate) {
              return ssx::now<optional_value_outcome>(std::nullopt);
          },
          [&field_descriptor, &stack](auto& value) {
              return single_field_to_value(
                std::move(value),
                *field_descriptor.message_type()->map_value(),
                stack);
          });

        if (value_result.has_error()) {
            co_return value_result.error();
        }

        ret->kvs.push_back(iceberg::kv_value{
          .key = std::move(*key_result.value()),
          .val = std::move(value_result.value())});
    }

    co_return ret;
}

ss::future<optional_value_outcome> message_field_to_value(
  std::optional<parsed::message::field> field,
  const pb::FieldDescriptor& field_descriptor,
  proto_descriptors_stack& stack) {
    if (field_descriptor.is_map()) {
        return convert_map(std::move(field), field_descriptor, stack);
    }

    if (field_descriptor.is_repeated()) {
        return convert_repeated(std::move(field), field_descriptor, stack);
    }

    return single_field_to_value(std::move(field), field_descriptor, stack);
}

ss::future<optional_value_outcome> single_field_to_value(
  std::optional<parsed::message::field> field,
  const pb::FieldDescriptor& field_descriptor,
  proto_descriptors_stack& stack) {
    switch (field_descriptor.type()) {
    case pb::FieldDescriptor::TYPE_DOUBLE:
        co_return convert<double, iceberg::double_value>(
          std::move(field),
          field_descriptor,
          &pb::FieldDescriptor::default_value_double);
    case pb::FieldDescriptor::TYPE_FLOAT:
        co_return convert<float, iceberg::float_value>(
          std::move(field),
          field_descriptor,
          &pb::FieldDescriptor::default_value_float);
    // casting uint32 to long value to prevent overflow
    case pb::FieldDescriptor::TYPE_UINT32:
    case pb::FieldDescriptor::TYPE_FIXED32:
    case pb::FieldDescriptor::TYPE_SFIXED64:
    case pb::FieldDescriptor::TYPE_INT64:
    case pb::FieldDescriptor::TYPE_SINT64:
        co_return convert<int64_t, iceberg::long_value>(
          std::move(field),
          field_descriptor,
          &pb::FieldDescriptor::default_value_int64);
    // unsigned 64 bit integers fallback to strings
    case pb::FieldDescriptor::TYPE_UINT64:
    case pb::FieldDescriptor::TYPE_FIXED64:
        co_return convert_u64_as_string(
          std::move(field),
          field_descriptor,
          &pb::FieldDescriptor::default_value_uint64);
    case pb::FieldDescriptor::TYPE_INT32:
    case pb::FieldDescriptor::TYPE_SFIXED32:
    case pb::FieldDescriptor::TYPE_SINT32:
        co_return convert<int32_t, iceberg::int_value>(
          std::move(field),
          field_descriptor,
          &pb::FieldDescriptor::default_value_int32);
    case pb::FieldDescriptor::TYPE_ENUM:
        if (!field.has_value()) {
            if (field_descriptor.has_presence()) {
                co_return std::nullopt;
            }
            co_return iceberg::int_value(
              field_descriptor.default_value_enum()->number());
        }
        co_return iceberg::int_value(
          std::get<int32_t>(std::move(field.value())));
    case pb::FieldDescriptor::TYPE_BOOL:
        co_return convert<bool, iceberg::boolean_value>(
          std::move(field),
          field_descriptor,
          &pb::FieldDescriptor::default_value_bool);
    case pb::FieldDescriptor::TYPE_STRING:
        co_return convert<iobuf, iceberg::string_value>(
          std::move(field),
          field_descriptor,
          &pb::FieldDescriptor::default_value_string);
    case pb::FieldDescriptor::TYPE_GROUP:
        co_return type_conversion_error(field_descriptor);
    case pb::FieldDescriptor::TYPE_MESSAGE: {
        std::unique_ptr<parsed::message> msg_field = nullptr;
        if (field.has_value()) {
            msg_field = std::get<std::unique_ptr<parsed::message>>(
              std::move(field.value()));
        }

        co_return co_await message_to_value(
          std::move(msg_field), *field_descriptor.message_type(), stack);
    }
    case pb::FieldDescriptor::TYPE_BYTES:
        if (!field.has_value()) {
            if (field_descriptor.has_presence()) {
                co_return std::nullopt;
            }
            co_return iceberg::binary_value{};
        }
        co_return iceberg::binary_value(
          std::get<iobuf>(std::move(field.value())));
    }
}

ss::future<optional_value_outcome> message_to_value(
  std::unique_ptr<parsed::message> message,
  const pb::Descriptor& descriptor,
  proto_descriptors_stack& stack) {
    if (message == nullptr) {
        co_return std::nullopt;
    }

    if (is_recursive_type(descriptor, stack)) {
        co_return value_conversion_exception(fmt::format(
          "Recursive message types are not supported. Descriptor: {}",
          descriptor.DebugString()));
    }
    if (stack.size() >= max_recursion_depth) {
        co_return value_conversion_exception(fmt::format(
          "Reached maximum recursion depth. Descriptor: {}",
          descriptor.DebugString()));
    }
    stack.push_back(&descriptor);
    auto ret = std::make_unique<iceberg::struct_value>();
    ret->fields.reserve(descriptor.field_count());
    /**
     * The conversion is driven by walking through the message descriptor as
     * some field with default values are skipped in parsed result.
     */
    for (int i = 0; i < descriptor.field_count(); ++i) {
        auto field_descriptor = descriptor.field(i);
        auto it = message->fields.find(field_descriptor->number());
        auto field = it == message->fields.end()
                       ? std::nullopt
                       : std::make_optional<parsed::message::field>(
                           std::move(it->second));

        auto result = co_await message_field_to_value(
          std::move(field), *field_descriptor, stack);

        if (result.has_error()) {
            co_return result.error();
        }
        ret->fields.push_back(std::move(result.value()));
    }
    stack.pop_back();
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
        co_return value_outcome(value_conversion_exception(fmt::format(
          "exception thrown while parsing protobuf - {}",
          std::current_exception())));
    }
}

} // namespace datalake
