// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "datalake/schema_protobuf.h"

#include "datalake/conversion_outcome.h"
#include "datalake/protobuf_utils.h"
#include "iceberg/datatypes.h"

#include <seastar/core/sstring.hh>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>

#include <ranges>

namespace datalake {
namespace {
namespace pb = google::protobuf;

/**
 * Schema conversion from protocol buffers to internal Redpanda iceberg schema
 */
using field_outcome = conversion_outcome<iceberg::nested_field_ptr>;
using struct_outcome = conversion_outcome<iceberg::struct_type>;

static constexpr int max_recursion_depth = 100;

field_outcome from_protobuf(
  const pb::FieldDescriptor& fd, bool is_repeated, proto_descriptors_stack&);

field_outcome success(const pb::FieldDescriptor& fd, iceberg::field_type ft) {
    return iceberg::nested_field::create(
      fd.number(), fd.name(), iceberg::field_required::no, std::move(ft));
}

struct_outcome struct_from_protobuf(
  const pb::Descriptor& msg, proto_descriptors_stack& stack) {
    if (is_recursive_type(msg, stack)) {
        return schema_conversion_exception(fmt::format(
          "Protocol buffer field not supported - recursive type detected, type "
          "hierarchy: {}, current type: {}",
          fmt::join(
            std::ranges::views::transform(stack, &pb::Descriptor::full_name),
            ", "),
          msg.DebugString()));
    }
    if (stack.size() > max_recursion_depth) {
        return schema_conversion_exception(fmt::format(
          "Protocol buffer field {} not supported - max nested depth of {} "
          "reached",
          msg.DebugString(),
          max_recursion_depth));
    }
    stack.push_back(&msg);
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
          iceberg::field_required(!value_field->is_optional()),
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
        return schema_conversion_exception(fmt::format(
          "Protocol buffer field {} type {} not supported",
          fd.DebugString(),
          fd.type_name()));
    case pb::FieldDescriptor::TYPE_MESSAGE: {
        auto msg_t = fd.message_type();

        // special case for handling google.protobuf.Timestamp
        if (
          msg_t->well_known_type() == pb::Descriptor::WELLKNOWNTYPE_TIMESTAMP) {
            return success(fd, iceberg::timestamp_type{});
        }

        auto st_result = struct_from_protobuf(*msg_t, stack);
        stack.pop_back();
        if (st_result.has_error()) {
            return st_result.error();
        }

        return success(fd, std::move(st_result).assume_value());
    }
    case pb::FieldDescriptor::TYPE_BYTES:
        return success(fd, iceberg::binary_type{});
    case pb::FieldDescriptor::TYPE_ENUM:
        [[fallthrough]];
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

} // namespace datalake
