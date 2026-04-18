/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "encryption/schema_annotation_parser.h"

#include "json/document.h"

namespace encryption {

namespace {

constexpr std::string_view kek_name_key = "encryption:kek_name";
constexpr std::string_view kms_key_id_key = "encryption:kms_key_id";

void collect_avro_annotations(
  const json::Value& record,
  std::vector<ss::sstring>& current_path,
  std::vector<field_encryption_annotation>& out) {
    auto fields_it = record.FindMember("fields");
    if (fields_it == record.MemberEnd() || !fields_it->value.IsArray()) {
        return;
    }

    for (const auto& field : fields_it->value.GetArray()) {
        if (!field.IsObject()) {
            continue;
        }
        auto name_it = field.FindMember("name");
        if (name_it == field.MemberEnd() || !name_it->value.IsString()) {
            continue;
        }
        ss::sstring field_name(
          name_it->value.GetString(), name_it->value.GetStringLength());

        // Check for encryption annotation on this field.
        auto kek_it = field.FindMember(kek_name_key.data());
        if (kek_it != field.MemberEnd() && kek_it->value.IsString()) {
            field_encryption_annotation ann;
            ann.path = current_path;
            ann.path.push_back(field_name);
            ann.kek_name = ss::sstring(
              kek_it->value.GetString(), kek_it->value.GetStringLength());

            auto kid_it = field.FindMember(kms_key_id_key.data());
            if (kid_it != field.MemberEnd() && kid_it->value.IsString()) {
                ann.kms_key_id = ss::sstring(
                  kid_it->value.GetString(), kid_it->value.GetStringLength());
            }
            out.push_back(std::move(ann));
        }

        // Recurse into the field's type if it is a record or a union
        // containing a record.
        auto type_it = field.FindMember("type");
        if (type_it == field.MemberEnd()) {
            continue;
        }

        const auto& type_val = type_it->value;

        if (type_val.IsObject()) {
            // Inline record definition.
            auto tt = type_val.FindMember("type");
            if (
              tt != type_val.MemberEnd() && tt->value.IsString()
              && std::string_view(
                   tt->value.GetString(), tt->value.GetStringLength())
                   == "record") {
                current_path.push_back(field_name);
                collect_avro_annotations(type_val, current_path, out);
                current_path.pop_back();
            }
        } else if (type_val.IsArray()) {
            // Union type -- check each branch for a record.
            for (const auto& branch : type_val.GetArray()) {
                if (!branch.IsObject()) {
                    continue;
                }
                auto tt = branch.FindMember("type");
                if (
                  tt != branch.MemberEnd() && tt->value.IsString()
                  && std::string_view(
                       tt->value.GetString(), tt->value.GetStringLength())
                       == "record") {
                    current_path.push_back(field_name);
                    collect_avro_annotations(branch, current_path, out);
                    current_path.pop_back();
                }
            }
        }
    }
}

void collect_json_schema_annotations(
  const json::Value& obj,
  std::vector<ss::sstring>& current_path,
  std::vector<field_encryption_annotation>& out) {
    auto props_it = obj.FindMember("properties");
    if (props_it == obj.MemberEnd() || !props_it->value.IsObject()) {
        return;
    }

    for (const auto& member : props_it->value.GetObject()) {
        if (!member.value.IsObject()) {
            continue;
        }
        ss::sstring prop_name(
          member.name.GetString(), member.name.GetStringLength());

        // Check for encryption annotation.
        auto kek_it = member.value.FindMember(kek_name_key.data());
        if (kek_it != member.value.MemberEnd() && kek_it->value.IsString()) {
            field_encryption_annotation ann;
            ann.path = current_path;
            ann.path.push_back(prop_name);
            ann.kek_name = ss::sstring(
              kek_it->value.GetString(), kek_it->value.GetStringLength());

            auto kid_it = member.value.FindMember(kms_key_id_key.data());
            if (
              kid_it != member.value.MemberEnd() && kid_it->value.IsString()) {
                ann.kms_key_id = ss::sstring(
                  kid_it->value.GetString(), kid_it->value.GetStringLength());
            }
            out.push_back(std::move(ann));
        }

        // Recurse into nested objects.
        current_path.push_back(prop_name);
        collect_json_schema_annotations(member.value, current_path, out);
        current_path.pop_back();
    }
}

} // namespace

std::vector<field_encryption_annotation>
parse_avro_encryption_annotations(const ss::sstring& schema_json) {
    json::Document doc;
    doc.Parse(schema_json.data(), schema_json.size());
    if (doc.HasParseError() || !doc.IsObject()) {
        return {};
    }

    std::vector<field_encryption_annotation> result;
    std::vector<ss::sstring> path;
    collect_avro_annotations(doc, path, result);
    return result;
}

std::vector<field_encryption_annotation>
parse_json_schema_encryption_annotations(const ss::sstring& schema_json) {
    json::Document doc;
    doc.Parse(schema_json.data(), schema_json.size());
    if (doc.HasParseError() || !doc.IsObject()) {
        return {};
    }

    std::vector<field_encryption_annotation> result;
    std::vector<ss::sstring> path;
    collect_json_schema_annotations(doc, path, result);
    return result;
}

} // namespace encryption
