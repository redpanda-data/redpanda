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

#include "pandaproxy/schema_registry/compatibility.h"

namespace pandaproxy::schema_registry {

std::ostream& operator<<(std::ostream& os, const avro_incompatibility_type& t) {
    switch (t) {
    case avro_incompatibility_type::name_mismatch:
        return os << "NAME_MISMATCH";
    case avro_incompatibility_type::fixed_size_mismatch:
        return os << "FIXED_SIZE_MISMATCH";
    case avro_incompatibility_type::missing_enum_symbols:
        return os << "MISSING_ENUM_SYMBOLS";
    case avro_incompatibility_type::reader_field_missing_default_value:
        return os << "READER_FIELD_MISSING_DEFAULT_VALUE";
    case avro_incompatibility_type::type_mismatch:
        return os << "TYPE_MISMATCH";
    case avro_incompatibility_type::missing_union_branch:
        return os << "MISSING_UNION_BRANCH";
    case avro_incompatibility_type::unknown:
        return os << "UNKNOWN";
    };
    __builtin_unreachable();
}

std::string_view description_for_type(avro_incompatibility_type t) {
    switch (t) {
    case avro_incompatibility_type::name_mismatch:
        return "The name of the schema has changed (path '{path}')";
    case avro_incompatibility_type::fixed_size_mismatch:
        return "The size of FIXED type field at path '{path}' in the "
               "{{reader}} schema does not match with the {{writer}} schema";
    case avro_incompatibility_type::missing_enum_symbols:
        return "The {{reader}} schema is missing enum symbols '{additional}' "
               "at path '{path}' in the {{writer}} schema";
    case avro_incompatibility_type::reader_field_missing_default_value:
        return "The field '{additional}' at path '{path}' in the {{reader}} "
               "schema has "
               "no default value and is missing in the {{writer}}";
    case avro_incompatibility_type::type_mismatch:
        return "The type (path '{path}') of a field in the {{reader}} schema "
               "does not match with the {{writer}} schema";
    case avro_incompatibility_type::missing_union_branch:
        return "The {{reader}} schema is missing a type inside a union field "
               "at path '{path}' in the {{writer}} schema";
    case avro_incompatibility_type::unknown:
        return "{{reader}} schema is not compatible with {{writer}} schema: "
               "check '{path}'";
    };
    __builtin_unreachable();
}

std::ostream& operator<<(std::ostream& os, const avro_incompatibility& v) {
    fmt::print(
      os,
      "{{errorType:'{}', description:'{}', additionalInfo:'{}'}}",
      v._type,
      v.describe(),
      v._additional_info);
    return os;
}

ss::sstring avro_incompatibility::describe() const {
    return fmt::format(
      fmt::runtime(description_for_type(_type)),
      fmt::arg("path", _path.string()),
      fmt::arg("additional", _additional_info));
}

std::ostream&
operator<<(std::ostream& os, const proto_incompatibility_type& t) {
    switch (t) {
    case proto_incompatibility_type::message_removed:
        return os << "MESSAGE_REMOVED";
    case proto_incompatibility_type::field_kind_changed:
        return os << "FIELD_KIND_CHANGED";
    case proto_incompatibility_type::field_scalar_kind_changed:
        return os << "FIELD_SCALAR_KIND_CHANGED";
    case proto_incompatibility_type::field_named_type_changed:
        return os << "FIELD_NAMED_TYPE_CHANGED";
    case proto_incompatibility_type::required_field_added:
        return os << "REQUIRED_FIELD_ADDED";
    case proto_incompatibility_type::required_field_removed:
        return os << "REQUIRED_FIELD_REMOVED";
    case proto_incompatibility_type::oneof_field_removed:
        return os << "ONEOF_FIELD_REMOVED";
    case proto_incompatibility_type::multiple_fields_moved_to_oneof:
        return os << "MULTIPLE_FIELDS_MOVED_TO_ONEOF";
    case proto_incompatibility_type::unknown:
        return os << "UNKNOWN";
    }
    __builtin_unreachable();
}

std::string_view description_for_type(proto_incompatibility_type t) {
    switch (t) {
    case proto_incompatibility_type::message_removed:
        return "The {{reader}} schema is missing a field of type MESSAGE at "
               "path '{path}' in the {{writer}} schema";
    case proto_incompatibility_type::field_kind_changed:
        return "The type of a field at path '{path}' in the {{reader}} "
               "schema does  not match the {{writer}} schema";
    case proto_incompatibility_type::field_scalar_kind_changed:
        return "The kind of a SCALAR field at path '{path}' in the {{reader}} "
               "schema does not match its kind in the {{writer}} schema";
    case proto_incompatibility_type::field_named_type_changed:
        return "The type of a MESSAGE field at path '{path}' in the {{reader}} "
               "schema does not match its type in the {{writer}} schema ";
    case proto_incompatibility_type::required_field_added:
        return "A required field  at path '{path}' in the {{reader}} schema "
               "is missing in the {{writer}} schema";
    case proto_incompatibility_type::required_field_removed:
        return "The {{reader}} schema is missing a required field at path: "
               "'{path}' in the {{writer}} schema";
    case proto_incompatibility_type::oneof_field_removed:
        return "The {{reader}} schema is missing a oneof field at path "
               "'{path}' in the {{writer}} schema";
    case proto_incompatibility_type::multiple_fields_moved_to_oneof:
        return "Multiple fields in the oneof at path '{path}' in the "
               "{{reader}} schema are outside a oneof in the {{writer}} "
               "schema ";
    case proto_incompatibility_type::unknown:
        return "{{reader}} schema is not compatible with {{writer}} schema: "
               "check '{path}'";
    }
    __builtin_unreachable();
}

std::ostream& operator<<(std::ostream& os, const proto_incompatibility& v) {
    fmt::print(
      os, "{{errorType:'{}', description:'{}'}}", v._type, v.describe());
    return os;
}

ss::sstring proto_incompatibility::describe() const {
    return fmt::format(
      fmt::runtime(description_for_type(_type)),
      fmt::arg("path", _path.string()));
}

compatibility_result
raw_compatibility_result::operator()(verbose is_verbose) && {
    compatibility_result result = {.is_compat = !has_error()};
    if (is_verbose) {
        result.messages.reserve(_errors.size());
        std::transform(
          std::make_move_iterator(_errors.begin()),
          std::make_move_iterator(_errors.end()),
          std::back_inserter(result.messages),
          [](auto e) {
              return std::visit(
                [](auto&& e) { return fmt::format("{{{}}}", e); }, e);
          });
    }
    return result;
}

void raw_compatibility_result::merge(raw_compatibility_result&& other) {
    _errors.reserve(_errors.size() + other._errors.size());
    std::move(
      other._errors.begin(), other._errors.end(), std::back_inserter(_errors));
}

} // namespace pandaproxy::schema_registry
