/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/conversion/schema_protobuf.h"

#include "iceberg/conversion/conversion_outcome.h"
#include "iceberg/conversion/protobuf_utils.h"
#include "iceberg/datatypes.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/defer.hh>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>

#include <ranges>

namespace iceberg {
namespace {
namespace pb = google::protobuf;

/**
 * Schema conversion from protocol buffers to internal Redpanda iceberg schema
 */
using field_outcome = conversion_outcome<iceberg::nested_field_ptr>;
using struct_outcome = conversion_outcome<iceberg::struct_type>;

field_outcome from_protobuf(
  const pb::FieldDescriptor& fd, bool is_repeated, proto_descriptors_stack&);

field_outcome success(const pb::FieldDescriptor& fd, iceberg::field_type ft) {
    return iceberg::nested_field::create(
      fd.number(),
      ss::sstring(fd.name()),
      iceberg::field_required::no,
      std::move(ft));
}

struct_outcome struct_from_protobuf(
  const pb::Descriptor& msg, proto_descriptors_stack& stack) {
    if (is_recursive_type(msg, stack)) {
        return conversion_exception(
          fmt::format(
            "Protocol buffer field not supported - recursive type detected, "
            "type "
            "hierarchy: {}, current type: {}",
            fmt::join(
              std::ranges::views::transform(stack, &pb::Descriptor::full_name),
              ", "),
            msg.DebugString()));
    }
    if (stack.size() > max_recursion_depth) {
        return conversion_exception(
          fmt::format(
            "Protocol buffer field {} not supported - max nested depth of {} "
            "exceeded",
            msg.DebugString(),
            max_recursion_depth));
    }

    stack.push_back(&msg);
    auto pop_stack = ss::defer([&stack] { stack.pop_back(); });
    iceberg::struct_type struct_t;
    struct_t.fields.reserve(msg.field_count());
    for (int i = 0; i < msg.field_count(); ++i) {
        auto field = msg.field(i);
        auto res = from_protobuf(*field, field->is_repeated(), stack);
        if (res.has_error()) {
            return res.error();
        }
        struct_t.fields.push_back(std::move(res).value());
    }
    return std::move(struct_t);
}

field_outcome from_protobuf(
  const pb::FieldDescriptor& fd,
  bool is_repeated,
  proto_descriptors_stack& stack) {
    if (fd.is_map()) {
        auto mt = fd.message_type();
        auto key_field = mt->map_key();
        auto value_field = mt->map_value();
        auto key_nested_res = from_protobuf(*key_field, false, stack);
        if (key_nested_res.has_error()) {
            return key_nested_res;
        }
        auto value_field_nested_res = from_protobuf(*value_field, false, stack);
        if (value_field_nested_res.has_error()) {
            return value_field_nested_res;
        }
        auto field_type = iceberg::map_type::create(
          key_field->number(),
          std::move(std::move(key_nested_res).assume_value()->type),
          value_field->number(),
          iceberg::field_required(value_field->is_required()),
          std::move(std::move(value_field_nested_res).assume_value()->type));
        return success(fd, std::move(field_type));
    }
    if (is_repeated) {
        auto field_type_res = from_protobuf(fd, false, stack);
        if (field_type_res.has_error()) {
            return field_type_res;
        }

        auto type = iceberg::list_type::create(
          fd.number(),
          iceberg::field_required::no,
          std::move(std::move(field_type_res).assume_value()->type));
        return success(fd, std::move(type));
    }

    switch (fd.type()) {
    case pb::FieldDescriptor::Type::TYPE_BOOL:
        return success(fd, iceberg::boolean_type{});
    case pb::FieldDescriptor::TYPE_DOUBLE:
        return success(fd, iceberg::double_type{});
    case pb::FieldDescriptor::TYPE_FLOAT:
        return success(fd, iceberg::float_type{});
    case pb::FieldDescriptor::TYPE_INT64:
    /**
     * We support 32 bits unsigned integers by casting them to the iceberg long
     * type, the long type can hold the max value of uint32_t without an
     * overflow.
     * This the original behavior of Parquet as in:
     * https://github.com/apache/iceberg/blob/79fd977f67592a16579cff31478e7ea98ef126e4/parquet/src/main/java/org/apache/iceberg/parquet/MessageTypeToType.java#L223
     *
     * For a better experience for users using uint64_t types that are not
     * normally supported in iceberg, we fallback to encoding them as strings.
     *
     */
    case pb::FieldDescriptor::TYPE_UINT32:
    case pb::FieldDescriptor::TYPE_FIXED32:
        return success(fd, iceberg::long_type{});
    case pb::FieldDescriptor::TYPE_UINT64:
    case pb::FieldDescriptor::TYPE_FIXED64:
        return success(fd, iceberg::string_type{});
    case pb::FieldDescriptor::TYPE_INT32:
        return success(fd, iceberg::int_type{});
    case pb::FieldDescriptor::TYPE_STRING:
        return success(fd, iceberg::string_type{});
    case pb::FieldDescriptor::TYPE_GROUP:
        return conversion_exception(
          fmt::format(
            "Protocol buffer field {} type {} not supported",
            fd.DebugString(),
            fd.type_name()));
    case pb::FieldDescriptor::TYPE_MESSAGE: {
        auto msg_t = fd.message_type();
        // special case for handling google.protobuf.Timestamp
        if (
          msg_t->well_known_type() == pb::Descriptor::WELLKNOWNTYPE_TIMESTAMP) {
            return success(fd, iceberg::timestamptz_type{});
        }
        // special case for handling google.protobuf.Struct, Value, and
        // ListValue - all serialize as JSON strings
        if (msg_t->well_known_type() == pb::Descriptor::WELLKNOWNTYPE_STRUCT) {
            return success(fd, iceberg::string_type{});
        }
        if (msg_t->well_known_type() == pb::Descriptor::WELLKNOWNTYPE_VALUE) {
            return success(fd, iceberg::string_type{});
        }
        if (
          msg_t->well_known_type() == pb::Descriptor::WELLKNOWNTYPE_LISTVALUE) {
            return success(fd, iceberg::string_type{});
        }
        if (msg_t->full_name() == protobuf::datalake_date_type) {
            return success(fd, iceberg::date_type{});
        }
        // Fail on any other redpanda.datalake.* types. This ensures that we
        // don't fallback to struct type for any custom types that we may add in
        // the future and break compatibility. We reserve the right to add
        // support for specific types under redpanda.datalake.* as needed.
        if (
          msg_t->full_name().starts_with(
            protobuf::datalake_well_known_type_prefix)) {
            return conversion_exception(
              fmt::format(
                "Protocol buffer field {} not supported - unhandled "
                "redpanda.datalake type {}",
                fd.DebugString(),
                msg_t->full_name()));
        }
        auto st_result = struct_from_protobuf(*msg_t, stack);
        if (st_result.has_error()) {
            return st_result.error();
        }

        return success(fd, std::move(st_result).assume_value());
    }
    case pb::FieldDescriptor::TYPE_BYTES:
        return success(fd, iceberg::binary_type{});
    case pb::FieldDescriptor::TYPE_ENUM:
        return success(fd, iceberg::string_type{});
    case pb::FieldDescriptor::TYPE_SFIXED32:
        return success(fd, iceberg::int_type{});
    case pb::FieldDescriptor::TYPE_SFIXED64:
        return success(fd, iceberg::long_type{});
    case pb::FieldDescriptor::TYPE_SINT32:
        return success(fd, iceberg::int_type{});
    case pb::FieldDescriptor::TYPE_SINT64:
        return success(fd, iceberg::long_type{});
    }
}
} // namespace

conversion_outcome<iceberg::struct_type>
type_to_iceberg(const pb::Descriptor& descriptor) {
    proto_descriptors_stack stack;
    return struct_from_protobuf(descriptor, stack);
}

} // namespace iceberg
