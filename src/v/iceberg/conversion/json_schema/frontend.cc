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

#include "absl/container/btree_map.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"
#include "http/utils.h"
#include "iceberg/conversion/json_schema/details/string_switch_table.h"
#include "iceberg/conversion/json_schema/ir.h"
#include "json/pointer.h"
#include "json/uri.h"

#include <seastar/util/defer.hh>

#include <algorithm>
#include <array>
#include <memory>
#include <ranges>
#include <unordered_set>
#include <variant>

namespace iceberg::conversion::json_schema {

namespace {

json::Uri parse_base_uri(const std::string& uri_str) {
    json::Uri uri{uri_str};
    if (uri.GetFragStringLength() > 0) {
        throw std::runtime_error("The base URI must not contain a fragment");
    }
    return uri;
}

dialect dialect_from_schema_id(std::string_view schema_id) {
    return details::string_switch_table(schema_id, dialect_by_schema_id);
}

constexpr auto banned_keywords = std::to_array({
  // Do not allow $dynamicRef keywords as would change the semantics of
  // the schema but we haven't implemented them yet.
  // Note for implementer: you'll also need to add encoding for fields when the
  // subschemas map is built. Make sure to add test cases with refs containing
  // characters that need escaping.
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
});

enum class keyword : uint8_t {
    schema,                // "$schema"
    id,                    // "$id"
    id_draft4,             // "id"
    ref,                   // "$ref"
    type,                  // "type"
    definitions,           // "definitions"
    properties,            // "properties"
    additional_properties, // "additionalProperties"
    items,                 // "items"
    additional_items,      // "additionalItems"
    format,                // "format"
    one_of,                // "oneOf"
};

constexpr auto keyword_table
  = std::to_array<std::pair<std::string_view, keyword>>({
    {"$schema", keyword::schema},
    {"$id", keyword::id},
    {"id", keyword::id_draft4},
    {"$ref", keyword::ref},
    {"type", keyword::type},
    {"definitions", keyword::definitions},
    {"properties", keyword::properties},
    {"additionalProperties", keyword::additional_properties},
    {"items", keyword::items},
    {"additionalItems", keyword::additional_items},
    {"format", keyword::format},
    {"oneOf", keyword::one_of},
  });

// The size assert below references the last enumerator explicitly. If you add
// a new keyword after one_of, update the assert. Together with the ordering
// assert (which guarantees keyword_table[i] maps to enum value i), these two
// checks ensure a 1:1 mapping between the enum and the table.
static_assert(
  keyword_table.size() == static_cast<size_t>(keyword::one_of) + 1,
  "keyword_table must have an entry for every keyword enum value");

static_assert(
  []() consteval {
      for (size_t i = 0; i < keyword_table.size(); ++i) {
          if (static_cast<size_t>(keyword_table[i].second) != i) {
              return false;
          }
      }
      return true;
  }(),
  "keyword_table entries must be ordered by enum value");

static_assert(
  []() consteval {
      for (const auto& [name, _] : keyword_table) {
          for (const auto& banned : banned_keywords) {
              if (name == banned) {
                  return false;
              }
          }
      }
      return true;
  }(),
  "keyword_table and banned_keywords must be disjoint");

constexpr std::string_view keyword_name(keyword kw) {
    return keyword_table[static_cast<size_t>(kw)].first;
}

// Indexes known JSON Schema keywords from a single pass over a node's members.
// Detects duplicate and banned keywords during construction.
//
// This is important as user validators might have different behavior for how
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
class keyword_index {
public:
    explicit keyword_index(const json::Value& node) {
        for (auto it = node.MemberBegin(); it != node.MemberEnd(); ++it) {
            std::string_view name{
              it->name.GetString(), it->name.GetStringLength()};
            auto entry = std::ranges::find(
              keyword_table,
              name,
              &std::pair<std::string_view, keyword>::first);
            if (entry != keyword_table.end()) {
                auto& slot = index_[static_cast<size_t>(entry->second)];
                if (slot != nullptr) {
                    throw std::runtime_error(
                      fmt::format("Duplicate keyword: {}", name));
                }
                slot = &it->value;
            } else if (std::ranges::contains(banned_keywords, name)) {
                throw std::runtime_error(
                  fmt::format("The {} keyword is not allowed", name));
            }
        }
    }

    const json::Value*
    find(keyword kw, std::initializer_list<json_value_type> types = {}) const {
        auto* val = index_[static_cast<size_t>(kw)];
        if (!val) {
            return nullptr;
        }
        if (types.size() > 0) {
            bool valid_type = false;
            for (const auto& type : types) {
                switch (type) {
                case json_value_type::object:
                    valid_type |= val->IsObject();
                    break;
                case json_value_type::array:
                    valid_type |= val->IsArray();
                    break;
                case json_value_type::string:
                    valid_type |= val->IsString();
                    break;
                case json_value_type::boolean:
                    valid_type |= val->IsBool();
                    break;
                case json_value_type::integer:
                    valid_type |= val->IsInt() || val->IsInt64();
                    break;
                case json_value_type::number:
                    valid_type |= val->IsNumber();
                    break;
                case json_value_type::null:
                    valid_type |= val->IsNull();
                    break;
                }
            }
            if (!valid_type) {
                throw std::runtime_error(
                  fmt::format(
                    "Invalid type for keyword {}. Expected one of: [{}].",
                    keyword_name(kw),
                    fmt::join(types, ", ")));
            }
        }
        return val;
    }

private:
    /// Pointers to the values of keywords in the same order as the `keyword`
    /// enum. `nullptr` means the keyword is not present in the indexed node.
    std::array<const json::Value*, keyword_table.size()> index_{};
};

class compile_context {
public:
    class resource_context {
        friend class compile_context;

    public:
        resource_context(
          const json::Uri& base_uri, dialect d, const json::Value& node)
          : base_uri_(base_uri)
          , dialect_(d)
          , schema_(
              ss::make_shared<schema_resource>(
                json::Uri::Get(base_uri_), dialect_))
          , node_(&node) {}

        resource_context(resource_context&&) = default;
        resource_context& operator=(resource_context&&) = default;
        resource_context(const resource_context&) = default;
        resource_context& operator=(const resource_context&) = default;

        ss::shared_ptr<schema_resource> schema() { return schema_; }
        ss::shared_ptr<const schema_resource> schema() const { return schema_; }

        const json::Value& node() const { return *node_; }

    private:
        json::Uri base_uri_;
        dialect dialect_;
        ss::shared_ptr<schema_resource> schema_;
        const json::Value* node_;
    };

public:
    explicit compile_context(
      const json::Uri& base_uri, std::optional<dialect> default_dialect)
      : ctx_base_uri_(base_uri)
      , default_dialect_(default_dialect) {}

public:
    [[nodiscard]] auto recurse_guard() {
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

        const auto id = json::Uri::Get(stack_.back().base_uri_);
        if (!seen_ids_.insert(id).second) {
            stack_.pop_back(); // Remove the context we just added.
            throw std::runtime_error(
              fmt::format("Duplicate schema ID: {}", id));
        }

        [[maybe_unused]] auto [it, inserted] = ctx_by_id_.emplace(
          id, stack_.back());
        dassert(inserted, "unique insertion should have succeeded");
    }

    bool empty() const { return stack_.empty(); }

    const resource_context& top() const {
        vassert(!empty(), "Stack is empty");
        return stack_.back();
    }

    resource_context& top() {
        vassert(!empty(), "Stack is empty");
        return stack_.back();
    }

    const json::Uri& base_uri() const {
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

    const resource_context* find_resource_context(std::string_view id) const {
        auto it = ctx_by_id_.find(id);
        return it != ctx_by_id_.end() ? &it->second : nullptr;
    }

    const subschema* find_subschema(const json::Value* node) const {
        auto it = json_to_subschema_.find(node);
        return it != json_to_subschema_.end() ? it->second : nullptr;
    }

    void register_subschema(const json::Value* node, const subschema* sub) {
        json_to_subschema_[node] = sub;
    }

private:
    absl::btree_map<std::string, resource_context> ctx_by_id_;
    chunked_hash_map<const json::Value*, const subschema*> json_to_subschema_;
    json::Uri ctx_base_uri_;
    std::optional<enum dialect> default_dialect_;
    chunked_vector<resource_context> stack_;
    std::unordered_set<std::string> seen_ids_;

    size_t depth_{0};
};

bool maybe_push_context(
  compile_context& ctx,
  const json::Value& node,
  const keyword_index& keywords) {
    // Identify the dialect first so we know which keyword is used for base URI,
    // i.e. `id` (draft-04) or `$id` (draft-6).
    auto dialect_at_node = [&ctx, &keywords]() {
        if (
          auto dialect_node = keywords.find(
            keyword::schema, {json_value_type::string})) {
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

    auto id_kw = dialect_at_node == dialect::draft4 ? keyword::id_draft4
                                                    : keyword::id;
    if (auto id_node = keywords.find(id_kw, {json_value_type::string})) {
        // The $id keyword starts a new resource context.
        ctx.push(
          parse_base_uri(id_node->GetString()).Resolve(ctx.base_uri()),
          dialect_at_node,
          node);

        return true;
    } else if (ctx.empty()) {
        // If the $id keyword is not present and the context is empty, we must
        // still push a new context with the base URI of the current context.
        ctx.push(json::Uri{ctx.base_uri()}, dialect_at_node, node);
        return true;
    } else {
        return false;
    }
}

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

    static const subschema* resolve_pointer(
      const compile_context& ctx,
      const json::Value& resource_root,
      const json::Uri& resolved_uri,
      std::string_view ref_value) {
        const char* frag_cstr = resolved_uri.GetFragString();

        if (frag_cstr == nullptr || frag_cstr[0] == '\0') {
            auto* result = ctx.find_subschema(&resource_root);
            if (!result) {
                throw std::runtime_error(
                  fmt::format(
                    "$ref {} points to uncompiled keyword", ref_value));
            }
            return result;
        }

        // RapidJSON Pointer parsing fails if pointer is part of fragment
        // (starts with #) but does not have reserved characters
        // percent-encoded. To work around this, we decode the fragment first
        // and parse the pointer without the leading '#'.
        const auto ptr_str = http::uri_decode(
          std::string_view{
            frag_cstr + 1, resolved_uri.GetFragStringLength() - 1});

        auto ptr = json::Pointer(ptr_str.data());
        if (!ptr.IsValid()) {
            throw std::runtime_error(
              fmt::format("$ref {} is not a valid JSON Pointer", ref_value));
        }

        const json::Value* target = ptr.Get(resource_root);
        if (!target) {
            throw std::runtime_error(
              fmt::format(
                "$ref {} points to non-existent location", ref_value));
        }

        auto* result = ctx.find_subschema(target);
        if (!result) {
            throw std::runtime_error(
              fmt::format(
                "$ref {} points to uncompiled keyword (not supported)",
                ref_value));
        }
        return result;
    }

    /// \pre fix_subschemas_base already applied.
    static void resolve_refs(const compile_context& ctx, subschema& sub) {
        if (sub.ref_value_.has_value()) {
            json::Uri ref_uri{*sub.ref_value_};
            const json::Uri base_uri{sub.base().id()};
            const json::Uri resolved_uri = ref_uri.Resolve(base_uri);

            const std::string resource_id = json::Uri::GetBase(resolved_uri);

            const auto* rsc_ctx = ctx.find_resource_context(resource_id);
            if (!rsc_ctx) {
                throw std::runtime_error(
                  fmt::format(
                    "Unresolvable $ref: {}. Schema resource {} not available",
                    *sub.ref_value_,
                    resource_id));
            }

            const json::Value& resource_root = rsc_ctx->node();
            sub.ref_ = resolve_pointer(
              ctx, resource_root, resolved_uri, *sub.ref_value_);
        }

        for (const auto& [k, v] : sub.subschemas_) {
            resolve_refs(ctx, *v);
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
        resolve_refs(ctx, *subschema);

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
            ctx.register_subschema(&node, sub.get());
            return sub;
        }

        keyword_index keywords(node);

        if (
          const auto ref_node = keywords.find(
            keyword::ref, {json_value_type::string})) {
            if (ctx.empty()) {
                throw std::runtime_error(
                  "$ref at the root of the schema in draft-07 is not allowed "
                  "to avoid undefined behavior");
            }

            auto sub = ss::make_shared<subschema>();
            sub->ref_value_ = ref_node->GetString();
            ctx.register_subschema(&node, sub.get());
            return sub;
        }

        auto new_ctx = maybe_push_context(ctx, node, keywords);

        auto sub = new_ctx
                     ? ss::dynamic_pointer_cast<subschema>(ctx.top().schema())
                     : ss::make_shared<subschema>();

        if (auto types_node = keywords.find(keyword::type)) {
            compile_types(ctx, *sub, *types_node);
        }

        if (
          const auto defs_node = keywords.find(
            keyword::definitions, {json_value_type::object});
          new_ctx && defs_node) {
            for (const auto& [k, v] : defs_node->GetObject()) {
                if (!v.IsObject()) {
                    throw std::runtime_error(
                      "The definitions keyword must be an object");
                }

                auto [it, inserted] = sub->subschemas_.emplace(
                  fmt::format("definitions/{}", k.GetString()),
                  compile_subschema(ctx, v));
                if (!inserted) {
                    throw std::runtime_error(
                      fmt::format(
                        "Duplicate subschema key: {}", k.GetString()));
                }
            }
        }

        if (
          auto props = keywords.find(
            keyword::properties, {json_value_type::object})) {
            for (const auto& [k, v] : props->GetObject()) {
                compile_property(ctx, *sub, k.GetString(), v);
            }
        }

        if (
          const auto additional_properties_node = keywords.find(
            keyword::additional_properties)) {
            compile_additional_properties(
              ctx, *sub, *additional_properties_node);
        }

        if (auto items_node = keywords.find(keyword::items)) {
            compile_items(ctx, *sub, *items_node);
        }

        if (
          const auto additional_items = keywords.find(
            keyword::additional_items)) {
            sub->additional_items_ = compile_subschema(ctx, *additional_items);
            auto [it, inserted] = sub->subschemas_.emplace(
              "additionalItems", sub->additional_items_);
            if (!inserted) {
                throw std::runtime_error(
                  "Duplicate additionalItems key in subschema");
            }
        }

        if (
          const auto one_of_node = keywords.find(
            keyword::one_of, {json_value_type::array})) {
            compile_one_of(ctx, *sub, *one_of_node);
        }

        if (
          const auto format_node = keywords.find(
            keyword::format, {json_value_type::string})) {
            if (unlikely(ctx.dialect() != dialect::draft7)) {
                // When support for more dialects is added format parsing should
                // be revisited. We should explicitly handle all known formats
                // before defaulting to string conversion in Iceberg.
                throw unsupported_feature_error(
                  "The format keyword is only supported in draft-07 dialect");
            }

            auto format_str = format_node->GetString();
            sub->format_ = details::string_switch_table<std::nullopt>(
              format_str, format_by_name);
        }

        if (new_ctx) {
            ctx.pop();
        }

        ctx.register_subschema(&node, sub.get());
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
                    throw std::runtime_error(
                      "The type keyword must be a "
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

    void compile_one_of(
      compile_context& ctx, subschema& sub, const json::Value& node) const {
        vassert(
          sub.one_of_.empty(),
          "oneOf subschema vector should be empty at this point");

        if (!node.IsArray() || node.GetArray().Empty()) {
            throw std::runtime_error(
              "The oneOf keyword must be a non-empty array");
        }

        std::vector<ss::shared_ptr<subschema>> one_of_subschemas;
        one_of_subschemas.reserve(node.GetArray().Size());
        for (const auto& item : node.GetArray()) {
            one_of_subschemas.push_back(compile_subschema(ctx, item));
        }

        for (size_t i = 0; i < one_of_subschemas.size(); ++i) {
            auto& one_of_subschema = one_of_subschemas[i];
            auto [it, inserted] = sub.subschemas_.emplace(
              fmt::format("oneOf/{}", i), one_of_subschema);
            vassert(inserted, "unique insertion should have succeeded");
        }

        sub.one_of_ = std::move(one_of_subschemas);
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
