// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/datatypes_json.h"

#include "iceberg/datatypes.h"
#include "iceberg/json_utils.h"
#include "json/document.h"
#include "strings/string_switch.h"

#include <stdexcept>
#include <string>

namespace iceberg {

namespace {

decimal_type parse_decimal(std::string_view type_str) {
    auto ps_str = extract_between('(', ')', type_str);
    size_t pos = ps_str.find(',');
    if (pos != std::string::npos) {
        // Extract substrings before and after the comma
        auto p_str = ps_str.substr(0, pos);
        auto s_str = ps_str.substr(pos + 1);
        return decimal_type{
          .precision = static_cast<uint32_t>(std::stoul(ss::sstring(p_str))),
          .scale = static_cast<uint32_t>(std::stoul(ss::sstring(s_str)))};
    }
    throw std::invalid_argument(fmt::format(
      "Decimal requires format decimal(uint32, uint32): {}", type_str));
}

fixed_type parse_fixed(std::string_view type_str) {
    auto l_str = extract_between('[', ']', type_str);
    auto l = std::stoull(ss::sstring(l_str));
    return fixed_type{l};
}

} // namespace

struct_type parse_struct(const json::Value& v) {
    struct_type ret;
    const auto& fields_json = parse_required(v, "fields");
    const auto& fields_array = fields_json.GetArray();
    ret.fields.reserve(fields_array.Size());
    for (const auto& field_json : fields_array) {
        ret.fields.emplace_back(parse_field(field_json));
    }
    return ret;
}

list_type parse_list(const json::Value& v) {
    const auto& element_json = parse_required(v, "element");
    return list_type::create(
      parse_required_i32(v, "element-id"),
      parse_required_bool(v, "element-required") ? field_required::yes
                                                 : field_required::no,
      parse_type(element_json));
}

map_type parse_map(const json::Value& v) {
    const auto& key_json = parse_required(v, "key");
    const auto& val_json = parse_required(v, "value");
    auto value_required = parse_required_bool(v, "value-required");
    return map_type::create(
      parse_required_i32(v, "key-id"),
      parse_type(key_json),
      parse_required_i32(v, "value-id"),
      value_required ? field_required::yes : field_required::no,
      parse_type(val_json));
}

nested_field_ptr parse_field(const json::Value& v) {
    auto id = parse_required_i32(v, "id");
    auto name = parse_required_str(v, "name");
    auto required = parse_required_bool(v, "required");
    const auto& type_json = parse_required(v, "type");
    auto type = parse_type(type_json);
    return nested_field::create(
      id,
      std::move(name),
      required ? field_required::yes : field_required::no,
      std::move(type));
}

field_type parse_type(const json::Value& v) {
    if (v.IsString()) {
        auto v_str = std::string_view{v.GetString(), v.GetStringLength()};
        if (v_str.starts_with("decimal")) {
            return parse_decimal(v_str);
        }
        if (v_str.starts_with("fixed")) {
            return parse_fixed(v_str);
        }
        return string_switch<field_type>(v_str)
          .match("boolean", boolean_type{})
          .match("int", int_type{})
          .match("long", long_type{})
          .match("float", float_type{})
          .match("double", double_type{})
          .match("date", date_type{})
          .match("time", time_type{})
          .match("timestamp", timestamp_type{})
          .match("timestamptz", timestamptz_type{})
          .match("string", string_type{})
          .match("uuid", uuid_type{})
          .match("binary", binary_type{});
    }
    if (!v.IsObject()) {
        throw std::invalid_argument("Expected string or object for type field");
    }
    auto type = parse_required_str(v, "type");
    if (type == "struct") {
        return parse_struct(v);
    } else if (type == "list") {
        return parse_list(v);
    } else if (type == "map") {
        return parse_map(v);
    }
    throw std::invalid_argument(fmt::format(
      "Expected type field of 'struct', 'list', or 'map': {}", type));
}

} // namespace iceberg

namespace json {

namespace {
class rjson_visitor {
public:
    explicit rjson_visitor(iceberg::json_writer& w)
      : w(w) {}
    void operator()(const iceberg::boolean_type&) { w.String("boolean"); }
    void operator()(const iceberg::int_type&) { w.String("int"); }
    void operator()(const iceberg::long_type&) { w.String("long"); }
    void operator()(const iceberg::float_type&) { w.String("float"); }
    void operator()(const iceberg::double_type&) { w.String("double"); }
    void operator()(const iceberg::decimal_type& t) {
        w.String("decimal({}, {})", t.precision, t.scale);
    }
    void operator()(const iceberg::date_type&) { w.String("date"); }
    void operator()(const iceberg::time_type&) { w.String("time"); }
    void operator()(const iceberg::timestamp_type&) { w.String("timestamp"); }
    void operator()(const iceberg::timestamptz_type&) {
        w.String("timestamptz");
    }
    void operator()(const iceberg::string_type&) { w.String("string"); }
    void operator()(const iceberg::uuid_type&) { w.String("uuid"); }
    void operator()(const iceberg::fixed_type& t) {
        w.String(fmt::format("fixed[{}]", t.length));
    }
    void operator()(const iceberg::binary_type&) { w.String("binary"); }

    void operator()(const iceberg::primitive_type& t) { rjson_serialize(w, t); }
    void operator()(const iceberg::struct_type& t) { rjson_serialize(w, t); }
    void operator()(const iceberg::list_type& t) { rjson_serialize(w, t); }
    void operator()(const iceberg::map_type& t) { rjson_serialize(w, t); }

private:
    iceberg::json_writer& w;
};
} // anonymous namespace

void rjson_serialize(iceberg::json_writer& w, const iceberg::nested_field& f) {
    w.StartObject();
    w.Key("id");
    w.Int(f.id());
    w.Key("name");
    w.String(f.name);
    w.Key("required");
    w.Bool(bool(f.required));
    w.Key("type");
    rjson_serialize(w, f.type);
    w.EndObject();
}

void rjson_serialize(
  iceberg::json_writer& w, const iceberg::primitive_type& t) {
    std::visit(rjson_visitor{w}, t);
}

void rjson_serialize(iceberg::json_writer& w, const iceberg::struct_type& t) {
    w.StartObject();
    w.Key("type");
    w.String("struct");
    w.Key("fields");
    w.StartArray();
    for (const auto& f : t.fields) {
        rjson_serialize(w, *f);
    }
    w.EndArray();
    w.EndObject();
}

void rjson_serialize(iceberg::json_writer& w, const iceberg::list_type& t) {
    w.StartObject();
    w.Key("type");
    w.String("list");
    w.Key("element-id");
    w.Int(t.element_field->id);
    w.Key("element-required");
    w.Bool(bool(t.element_field->required));
    w.Key("element");
    rjson_serialize(w, t.element_field->type);
    w.EndObject();
}

void rjson_serialize(iceberg::json_writer& w, const iceberg::map_type& t) {
    w.StartObject();
    w.Key("type");
    w.String("map");
    w.Key("key-id");
    w.Int(t.key_field->id);
    w.Key("value-id");
    w.Int(t.value_field->id);
    w.Key("value-required");
    w.Bool(bool(t.value_field->required));
    w.Key("key");
    rjson_serialize(w, t.key_field->type);
    w.Key("value");
    rjson_serialize(w, t.value_field->type);
    w.EndObject();
}

void rjson_serialize(iceberg::json_writer& w, const iceberg::field_type& t) {
    std::visit(rjson_visitor{w}, t);
}

} // namespace json
