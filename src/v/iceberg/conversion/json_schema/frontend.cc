/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/conversion/json_schema/frontend.h"

#include "container/chunked_vector.h"
#include "iceberg/conversion/json_schema/details/string_switch_table.h"
#include "iceberg/conversion/json_schema/ir.h"

#include <seastar/util/defer.hh>

#include <jsoncons/uri.hpp>

#include <array>
#include <memory>
#include <unordered_set>
#include <variant>

namespace iceberg::conversion::json_schema {

namespace {

jsoncons::uri parse_base_uri(const std::string& uri_str) {
    jsoncons::uri uri{uri_str};
    if (!uri.encoded_fragment().empty()) {
        throw std::runtime_error("The base URI must not contain a fragment");
    }
    return uri;
}

dialect dialect_from_schema_id(std::string_view schema_id) {
    return details::string_switch_table(schema_id, dialect_by_schema_id);
}

// Prefer using this instead of using `FindMember` directly, as it checks for
// duplicate keywords and throws an exception if a duplicate is found.
//
// This is important as user validator might have different behavior for how
// duplicate object keys are handled and we might infer a schema which would
// not fit user's data.
//
// For example, rapidjson `FindMember` returns the first key. But sourcemeta's
// jsonschema validator will use the last key so for a schema like this:
// ```json
// {
//   "type": "object",
//   "properties": {
//     "foo": { "type": "string" }
//   }
//   "properties": {
//     "foo": { "type": "integer" }
//   }
// }
// ```
// Without rejecting schemas with duplicate keywords, we would end up with a
// schema that accepts strings but the user data will contain integers if
// data was validated with i.e. sourcemeta's validator.
const json::Value* find_keyword(
  const json::Value& node,
  const char* keyword,
  std::vector<json_value_type> types = {}) {
    auto it = node.FindMember(keyword);
    if (it != node.MemberEnd()) {
        auto next = std::next(it);
        if (next != node.MemberEnd() && next->name == keyword) {
            throw std::runtime_error(
              fmt::format("Duplicate keyword: {}", keyword));
        }
        if (!types.empty()) {
            bool valid_type = false;
            for (const auto& type : types) {
                switch (type) {
                case json_value_type::object:
                    valid_type |= it->value.IsObject();
                    break;
                case json_value_type::array:
                    valid_type |= it->value.IsArray();
                    break;
                case json_value_type::string:
                    valid_type |= it->value.IsString();
                    break;
                case json_value_type::boolean:
                    valid_type |= it->value.IsBool();
                    break;
                case json_value_type::integer:
                    valid_type |= it->value.IsInt() || it->value.IsInt64();
                    break;
                case json_value_type::number:
                    valid_type |= it->value.IsNumber();
                    break;
                case json_value_type::null:
                    valid_type |= it->value.IsNull();
                    break;
                }
            }
            if (!valid_type) {
                throw std::runtime_error(fmt::format(
                  "Invalid type for keyword {}. Expected one of: [{}].",
                  keyword,
                  fmt::join(types, ", ")));
            }
        }
        return &it->value;
    }
    return nullptr;
}

class compile_context {
public:
    class resource_context {
        friend class compile_context;

    public:
        resource_context(jsoncons::uri base_uri, dialect d)
          : base_uri_(std::move(base_uri))
          , dialect_(d) {}

    private:
        jsoncons::uri base_uri_;
        dialect dialect_;
        std::unordered_map<std::string, ss::shared_ptr<subschema>> subschemas_;
        chunked_vector<std::string> path_stack_;
    };

public:
    explicit compile_context(
      jsoncons::uri base_uri, std::optional<dialect> default_dialect)
      : ctx_base_uri_(std::move(base_uri))
      , default_dialect_(default_dialect) {}

public:
    auto recurse_guard() {
        constexpr static size_t max_depth{32};
        if (depth_ >= max_depth) {
            throw std::runtime_error("Schema depth limit exceeded");
        }
        ++depth_;
        return ss::defer([this] { --depth_; });
    }

    template<typename... Args>
    void push(Args&&... args) {
        stack_.emplace_back(std::forward<Args>(args)...);

        const auto& id = stack_.back().base_uri_.string();
        if (!seen_ids_.insert(id).second) {
            std::string owned_id
              = id; // Copy to avoid dangling reference after pop_back.
            stack_.pop_back(); // Remove the context we just added.
            throw std::runtime_error(
              fmt::format("Duplicate schema ID: {}", owned_id));
        }
    }

    bool empty() const { return stack_.empty(); }

    const resource_context& top() const {
        vassert(!empty(), "Stack is empty");
        return stack_.back();
    }

    const jsoncons::uri& base_uri() const {
        return empty() ? ctx_base_uri_ : top().base_uri_;
    }

    dialect dialect() const {
        if (empty()) {
            if (!default_dialect_) {
                throw std::runtime_error(
                  "Schema dialect is not set (missing $schema keyword?)");
            }
            return *default_dialect_;
        } else {
            return top().dialect_;
        }
    }

    void pop() {
        vassert(!empty(), "Stack is empty");
        stack_.pop_back();
    }

private:
    jsoncons::uri ctx_base_uri_;
    std::optional<enum dialect> default_dialect_;
    chunked_vector<resource_context> stack_;
    std::unordered_set<std::string> seen_ids_;

    size_t depth_{0};
};

bool maybe_push_context(compile_context& ctx, const json::Value& node) {
    // Identify the dialect first so we know which keyword is used for base URI,
    // i.e. `id` (draft-04) or `$id` (draft-6).
    auto dialect_at_node = [&ctx, &node]() {
        if (
          auto dialect_node = find_keyword(
            node, "$schema", {json_value_type::string})) {
            return dialect_from_schema_id(dialect_node->GetString());
        } else {
            // If the $schema keyword is not present, we use the dialect of the
            // context.
            return ctx.dialect();
        }
    }();

    if (dialect_at_node != dialect::draft7) {
        throw unsupported_feature_error(
          fmt::format("Unsupported JSON Schema dialect: {}", dialect_at_node));
    }

    auto id_keyword = dialect_at_node == dialect::draft4 ? "id" : "$id";
    if (
      auto id_node = find_keyword(
        node, id_keyword, {json_value_type::string})) {
        // The $id keyword starts a new resource context.
        ctx.push(
          parse_base_uri(id_node->GetString()).resolve(ctx.base_uri()),
          dialect_at_node);

        return true;
    } else if (ctx.empty()) {
        // If the $id keyword is not present and the context is empty, we must
        // still push a new context with the base URI of the current context.
        ctx.push(ctx.base_uri(), dialect_at_node);
        return true;
    } else {
        return false;
    }
}

constexpr auto banned_keywords = std::to_array({
  // Do not allow $ref and $dynamicRef keywords as would change the semantics of
  // the schema but we haven't implemented them yet.
  // Note for implementer: you'll also need to add encoding for fields when the
  // subschemas map is built. Make sure to add test cases with refs containing
  // characters that need escaping.
  "$ref",
  "$dynamicRef",

  // Not implementing for now for simplicity and because I don't think anyone
  // relies on it due to peculiarities of what this keyword does.
  // See: https://github.com/json-schema-org/json-schema-spec/issues/867
  "default",

  // We can't use this in iceberg so don't spend time implementing it for now.
  "patternProperties",
  "dependencies",
  "if",
  "then",
  "else",
  "allOf",
  "anyOf",
  "oneOf",
});

}; // namespace

class frontend::frontend_impl {
    // Walk the tree recursively and set all bases to their closing schema
    // resource.
    static void
    fix_subschemas_base(subschema& sub, schema_resource* current_base) {
        sub.parent_base_ = current_base;

        if (auto rsc = dynamic_cast<schema_resource*>(&sub)) {
            current_base = rsc;
        }

        for (const auto& [k, v] : sub.subschemas_) {
            fix_subschemas_base(*v, current_base);
        }
    }

public:
    ss::shared_ptr<schema_resource> compile_document(
      const json::Document& doc,
      const std::string& initial_base_uri,
      std::optional<dialect> default_dialect) const {
        compile_context ctx{parse_base_uri(initial_base_uri), default_dialect};

        auto subschema = compile_subschema(ctx, doc);
        auto schema_rsc = ss::dynamic_pointer_cast<schema_resource>(subschema);

        vassert(
          schema_rsc != nullptr,
          "The root of the schema must be a schema resource");

        fix_subschemas_base(*subschema, schema_rsc.get());

        return schema_rsc;
    }

    ss::shared_ptr<subschema>
    compile_subschema(compile_context& ctx, const json::Value& node) const {
        auto recurse_guard = ctx.recurse_guard();

        // If the context is empty, we are compiling the root schema.
        if (ctx.empty()) {
            if (!node.IsObject()) {
                throw std::runtime_error(
                  "JSON Schema document must be an object");
            }
        } else {
            // If the context is not empty, we are compiling a subschema.
            if (!node.IsObject() && !node.IsBool()) {
                throw std::runtime_error(
                  "Subschema must be an object or a boolean");
            }
        }

        if (node.IsBool()) {
            auto sub = ss::make_shared<subschema>();
            sub->boolean_subschema_ = node.GetBool();
            return sub;
        }

        auto new_ctx = maybe_push_context(ctx, node);

        auto sub = new_ctx ? ss::make_shared<schema_resource>(
                               ctx.base_uri().string(), ctx.dialect())
                           : ss::make_shared<subschema>();

        // Check for banned keywords.
        for (const auto& keyword : banned_keywords) {
            if (node.HasMember(keyword)) {
                throw std::runtime_error(
                  fmt::format("The {} keyword is not allowed", keyword));
            }
        }

        if (auto types_node = find_keyword(node, "type")) {
            compile_types(ctx, *sub, *types_node);
        }

        if (
          const auto defs_node = find_keyword(
            node, "$defs", {json_value_type::object})) {
            for (const auto& [k, v] : defs_node->GetObject()) {
                if (!v.IsObject()) {
                    throw std::runtime_error(
                      "The $defs keyword must be an object");
                }

                auto [it, inserted] = sub->subschemas_.emplace(
                  fmt::format("$defs/{}", k.GetString()),
                  compile_subschema(ctx, v));
                if (!inserted) {
                    throw std::runtime_error(fmt::format(
                      "Duplicate subschema key: {}", k.GetString()));
                }
            }
        }

        if (
          auto props = find_keyword(
            node, "properties", {json_value_type::object})) {
            for (const auto& [k, v] : props->GetObject()) {
                compile_property(ctx, *sub, k.GetString(), v);
            }
        }

        if (
          const auto additional_properties_node = find_keyword(
            node, "additionalProperties")) {
            compile_additional_properties(
              ctx, *sub, *additional_properties_node);
        }

        if (auto items_node = find_keyword(node, "items")) {
            compile_items(ctx, *sub, *items_node);
        }

        if (
          const auto additional_items = find_keyword(node, "additionalItems")) {
            sub->additional_items_ = compile_subschema(ctx, *additional_items);
            auto [it, inserted] = sub->subschemas_.emplace(
              "additionalItems", sub->additional_items_);
            if (!inserted) {
                throw std::runtime_error(
                  "Duplicate additionalItems key in subschema");
            }
        }

        if (
          const auto format_node = find_keyword(
            node, "format", {json_value_type::string})) {
            auto format_str = format_node->GetString();
            std::optional<format> value_format
              = details::string_switch_table<std::nullopt>(
                format_str, format_by_name);
            if (!value_format) {
                throw std::runtime_error(
                  fmt::format("Unsupported format: {}", format_str));
            }
            sub->format_ = *value_format;
        }

        if (new_ctx) {
            ctx.pop();
        }

        return sub;
    }

    void compile_types(
      compile_context&, subschema& sub, const json::Value& node) const {
        if (node.IsString()) {
            sub.types_ = {parse_json_value_type(node.GetString())};
        } else if (node.IsArray()) {
            std::vector<json_value_type> types;
            for (const auto& type : node.GetArray()) {
                if (!type.IsString()) {
                    throw std::runtime_error("The type keyword must be a "
                                             "string or an array of strings");
                }
                types.push_back(parse_json_value_type(type.GetString()));
            }
            sub.types_ = std::move(types);
        } else {
            throw std::runtime_error(
              "The type keyword must be a string or an array of strings");
        }
    }

    void compile_property(
      compile_context& ctx,
      subschema& sub,
      const char* key,
      const json::Value& node) const {
        auto property_subschema = compile_subschema(ctx, node);
        {
            // todo encode
            auto [it, inserted] = sub.subschemas_.emplace(
              fmt::format("properties/{}", key), property_subschema);
            if (!inserted) {
                throw std::runtime_error(
                  fmt::format("Duplicate property key: {}", key));
            }
        }

        {
            auto [it, inserted] = sub.properties_.emplace(
              key, property_subschema);
            if (!inserted) {
                throw std::runtime_error(
                  fmt::format("Duplicate property key: {}", key));
            }
        }
    }

    void compile_additional_properties(
      compile_context& ctx, subschema& sub, const json::Value& node) const {
        if (sub.additional_properties_) {
            throw std::runtime_error(
              "The additionalProperties keyword can only be specified once");
        }

        auto additional_subschema = compile_subschema(ctx, node);
        auto [it, inserted] = sub.subschemas_.emplace(
          "additionalProperties", additional_subschema);
        if (!inserted) {
            throw std::runtime_error(
              "Duplicate additionalProperties key in subschema");
        }
        sub.additional_properties_ = std::move(additional_subschema);
    }

    void compile_items(
      compile_context& ctx, subschema& sub, const json::Value& node) const {
        if (!std::holds_alternative<std::monostate>(sub.items_)) {
            throw std::runtime_error(
              "The items keyword can only be specified once");
        }

        if (node.IsObject() || node.IsBool()) {
            auto items_subschema = compile_subschema(ctx, node);
            if (!std::holds_alternative<std::monostate>(sub.items_)) {
                throw std::runtime_error(
                  "The items keyword can only be specified once");
            }

            auto [it, inserted] = sub.subschemas_.emplace(
              "items", items_subschema);
            if (!inserted) {
                throw std::runtime_error(
                  fmt::format("Duplicate items key in subschema"));
            }

            sub.items_ = std::move(items_subschema);
        } else if (node.IsArray()) {
            std::vector<ss::shared_ptr<subschema>> items_subschemas;
            for (const auto& item : node.GetArray()) {
                items_subschemas.push_back(compile_subschema(ctx, item));
            }

            for (size_t i = 0; i < items_subschemas.size(); ++i) {
                auto& item_subschema = items_subschemas[i];
                auto [it, inserted] = sub.subschemas_.emplace(
                  fmt::format("items/{}", i), item_subschema);
                if (!inserted) {
                    throw std::runtime_error(
                      fmt::format("Duplicate items key: {}", i));
                }
            }

            sub.items_ = std::move(items_subschemas);
        } else {
            throw std::runtime_error(
              "The items keyword must be an object or an array of objects");
        }
    }
};

frontend::frontend()
  : impl_(std::make_unique<frontend_impl>()) {}

frontend::~frontend() = default;

schema frontend::compile(
  const json::Document& doc,
  const std::string& initial_base_uri,
  std::optional<dialect> default_dialect) const {
    return schema(
      impl_->compile_document(doc, initial_base_uri, default_dialect));
};

}; // namespace iceberg::conversion::json_schema
