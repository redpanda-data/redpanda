/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"
#include "base/vassert.h"

#include <seastar/core/shared_ptr.hh>

#include <fmt/core.h>

#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

namespace iceberg::conversion::json_schema {

/// Known dialects of JSON Schema.
enum class dialect {
    draft4,
    draft6,
    draft7,
    draft201909,
    draft202012,
};

constexpr auto dialect_by_schema_id = std::to_array({
  std::pair{"http://json-schema.org/draft-04/schema#", dialect::draft4},
  std::pair{"http://json-schema.org/draft-06/schema#", dialect::draft6},
  std::pair{"http://json-schema.org/draft-07/schema#", dialect::draft7},
  std::pair{
    "https://json-schema.org/draft/2019-09/schema", dialect::draft201909},
  std::pair{
    "https://json-schema.org/draft/2020-12/schema", dialect::draft202012},
});

enum class format {
    date_time,
    date,
    time,
};

constexpr auto format_by_name = std::to_array({
  std::pair{"date-time", format::date_time},
  std::pair{"date", format::date},
  std::pair{"time", format::time},
});

/// Primitive data types.
/// https://json-schema.org/draft/2020-12/json-schema-core#name-instance-data-model
enum class json_value_type : uint8_t {
    null = 0,
    boolean,
    object,
    array,
    number,
    integer,
    string,
};

constexpr auto all_json_value_types = std::to_array({
  json_value_type::null,
  json_value_type::boolean,
  json_value_type::object,
  json_value_type::array,
  json_value_type::number,
  json_value_type::integer,
  json_value_type::string,
});

consteval bool json_value_types_contiguous() {
    for (size_t i = 0; i < all_json_value_types.size(); ++i) {
        if (static_cast<size_t>(all_json_value_types[i]) != i) {
            return false;
        }
    }
    return true;
}

static_assert(
  json_value_types_contiguous(),
  "json_value_type order/values drifted from all_json_value_types");

json_value_type parse_json_value_type(const std::string&);

class subschema;
class schema_resource;

using subschemas_map_t
  = std::unordered_map<std::string, ss::shared_ptr<subschema>>;

/// Proxy view for properties that exposes references to subschema instead
/// of shared pointers. Do not let the caller think it is safe to copy
/// shared pointers.
class const_map_view {
public:
    using map_type = std::unordered_map<std::string, ss::shared_ptr<subschema>>;
    using value_type = std::pair<const std::string&, const subschema&>;
    class iterator {
    public:
        using base_iter = map_type::const_iterator;
        using iterator_category = std::forward_iterator_tag;
        using difference_type = std::ptrdiff_t;
        using value_type = std::pair<const std::string&, const subschema&>;
        using pointer = void;
        using reference = value_type;

        iterator() = default;

        explicit iterator(base_iter it)
          : it_(it) {}

        /// Returns the current key-value pair.
        value_type operator*() const { return {it_->first, *it_->second}; }

        const std::string& key() const { return it_->first; }
        const subschema& value() const { return *it_->second; }

        iterator& operator++() {
            ++it_;
            return *this;
        }
        const iterator operator++(int) {
            iterator tmp = *this;
            ++it_;
            return tmp;
        }
        bool operator==(const iterator& o) const { return it_ == o.it_; }
        bool operator!=(const iterator& o) const { return it_ != o.it_; }

    private:
        base_iter it_;
    };

    explicit const_map_view(const map_type& m)
      : map_(&m) {}
    iterator begin() const { return iterator(map_->begin()); }
    iterator end() const { return iterator(map_->end()); }
    size_t size() const { return map_->size(); }
    bool empty() const { return map_->empty(); }

    const iterator find(const std::string& key) const {
        return iterator(map_->find(key));
    }

    const subschema& at(const std::string& key) const { return *map_->at(key); }

private:
    const map_type* map_;
};

static_assert(std::ranges::range<const_map_view>);

/// Proxy view for vectors that exposes references to subschema instead
/// of shared pointers. Do not let the caller think it is safe to copy
/// shared pointers.
class const_list_view {
public:
    using list_type = std::vector<ss::shared_ptr<subschema>>;
    using value_type = const subschema&;
    class iterator {
    public:
        using base_iter = list_type::const_iterator;
        using iterator_category = std::forward_iterator_tag;
        using difference_type = std::ptrdiff_t;
        using value_type = const subschema&;
        using pointer = void;
        using reference = value_type;

        iterator() = default;

        explicit iterator(base_iter it)
          : it_(it) {}

        /// Returns the current element.
        value_type operator*() const { return **it_; }

        iterator& operator++() {
            ++it_;
            return *this;
        }
        const iterator operator++(int) {
            iterator tmp = *this;
            ++it_;
            return tmp;
        }
        bool operator==(const iterator& o) const { return it_ == o.it_; }
        bool operator!=(const iterator& o) const { return it_ != o.it_; }

    private:
        base_iter it_;
    };

    explicit const_list_view(const list_type& l)
      : list_(&l) {}
    iterator begin() const { return iterator(list_->begin()); }
    iterator end() const { return iterator(list_->end()); }
    size_t size() const { return list_->size(); }
    bool empty() const { return list_->empty(); }

    const subschema& at(size_t index) const { return *list_->at(index); }

private:
    const list_type* list_;
};

static_assert(std::ranges::range<const_list_view>);

/// Represents a JSON Schema object contained within a surrounding schema.
class subschema {
    friend class frontend;
    friend class schema;
    friend class schema_resource;

    using items_t = std::variant<
      std::monostate,
      ss::shared_ptr<subschema>,
      std::vector<ss::shared_ptr<subschema>>>;

    using items_ref_t = std::variant<
      std::monostate,
      std::reference_wrapper<const subschema>,
      std::vector<std::reference_wrapper<const subschema>>>;

    using items_view_t = std::variant<
      std::monostate,
      std::reference_wrapper<const subschema>,
      const_list_view>;

public:
    subschema() = default;
    virtual ~subschema() = default;

    subschema(const subschema&) = delete;
    subschema& operator=(const subschema&) = delete;

    subschema(subschema&&) = delete;
    subschema& operator=(subschema&&) = delete;

public:
    /// Returns the base schema resource of this subschema.
    /// \return Pointer to the base schema resource.
    const schema_resource& base() const;
    const schema_resource& parent_base() const { return *parent_base_; }

    const_map_view subschemas() const { return const_map_view(subschemas_); }

public:
    /// \return Boolean subschema.
    const std::optional<bool>& boolean_subschema() const {
        return boolean_subschema_;
    }

    /// \return Types allowed by this schema object. Empty means
    /// unconstrained. See
    /// https://www.learnjsonschema.com/2020-12/validation/type/
    const std::vector<json_value_type>& types() const { return types_; }

    /// \return Properties allowed by this schema object as references.
    /// The returned view exposes references to the subschema objects.
    const_map_view properties() const { return const_map_view(properties_); }

    const std::optional<std::reference_wrapper<const subschema>>
    additional_properties() const {
        return additional_properties_
                 ? std::make_optional(
                     std::reference_wrapper<const subschema>(
                       *additional_properties_.get()))
                 : std::nullopt;
    }

    /// \return Items allowed by this schema object.
    /// See https://www.learnjsonschema.com/2020-12/applicator/items/
    const items_view_t items() const {
        if (std::holds_alternative<std::monostate>(items_)) {
            return items_view_t{};
        } else if (
          auto* item = std::get_if<ss::shared_ptr<subschema>>(&items_)) {
            return std::reference_wrapper<const subschema>(*item->get());
        } else {
            auto& vec = std::get<std::vector<ss::shared_ptr<subschema>>>(
              items_);
            return const_list_view(vec);
        }
    }

    /// \return Additional items allowed by this schema object.
    /// See https://www.learnjsonschema.com/2020-12/applicator/additional-items/
    const std::optional<std::reference_wrapper<const subschema>>
    additional_items() const {
        return additional_items_ ? std::make_optional(
                                     std::reference_wrapper<const subschema>(
                                       *additional_items_.get()))
                                 : std::nullopt;
    }

    /// \return Format of this schema object if specified.
    /// See https://www.learnjsonschema.com/2020-12/validation/format
    const std::optional<format>& format() const { return format_; }

    /// \return The raw $ref value.
    /// See
    /// https://json-schema.org/understanding-json-schema/structuring#dollarref
    const std::optional<std::string>& ref_value() const { return ref_value_; }

    /// \return The resolved subschema pointed to by $ref.
    const subschema* ref() const { return ref_; }

private:
    /// Base.
    schema_resource* parent_base_{};

    /// Immediate child subschemas within this schema object keyed by
    /// keyword.
    subschemas_map_t subschemas_;

    std::optional<bool> boolean_subschema_;

    std::vector<json_value_type> types_;

    std::unordered_map<std::string, ss::shared_ptr<subschema>> properties_;
    ss::shared_ptr<subschema> additional_properties_;

    items_t items_;
    ss::shared_ptr<subschema> additional_items_;

    std::optional<enum format> format_;

    std::optional<std::string> ref_value_;
    const subschema* ref_{nullptr};
};

/// Represents a JSON Schema resource identified by a canonical URI
/// (RFC6596).
///
/// The "$id" keyword identifies a schema resource with its canonical URI.
///
/// See:
/// https://json-schema.org/draft/2020-12/json-schema-core#name-root-schema-and-subschemas-
/// See:
/// https://json-schema.org/draft/2020-12/json-schema-core#name-the-id-keyword
/// See: https://www.rfc-editor.org/info/rfc6596
class schema_resource : public subschema {
    friend class subschema;

public:
    schema_resource(std::string id, dialect d)
      : id_(std::move(id))
      , dialect_(d) {}

public:
    /// \return The canonical URI of the schema resource as a string.
    const std::string& id() const { return id_; }
    dialect dialect() const { return dialect_; }

private:
    /// The canonical URI of the schema resource.
    std::string id_;
    enum dialect dialect_;
};

/// Root of the intermediate representation (IR) tree for JSON schema.
class schema {
public:
    explicit schema(ss::shared_ptr<schema_resource> root)
      : root_(std::move(root)) {
        vassert(root_ != nullptr, "Root schema resource cannot be null");
        root_->parent_base_ = root_.get();
    }

public:
    /// Returns the root schema resource.
    /// \return Reference to the root schema_resource.
    const schema_resource& root() const { return *root_; }

private:
    ss::shared_ptr<schema_resource> root_;
};

}; // namespace iceberg::conversion::json_schema

namespace fmt {

using namespace iceberg::conversion::json_schema;

template<>
struct formatter<json_value_type> final : formatter<string_view> {
    auto format(json_value_type c, format_context& ctx) const
      -> format_context::iterator;
};

template<>
struct formatter<dialect> final : formatter<string_view> {
    auto format(dialect c, format_context& ctx) const
      -> format_context::iterator;
};

} // namespace fmt
