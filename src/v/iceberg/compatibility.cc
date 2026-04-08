/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/compatibility.h"

#include "iceberg/compatibility_types.h"
#include "iceberg/compatibility_utils.h"
#include "iceberg/datatypes.h"

#include <ranges>
#include <stdexcept>
#include <variant>

namespace iceberg {

namespace {
struct primitive_type_promotion_policy_visitor {
    template<typename T, typename U>
    requires(!std::is_same_v<T, U>)
    type_check_result operator()(const T&, const U&) const {
        return compat_errc::mismatch;
    }

    template<typename T>
    type_check_result operator()(const T&, const T&) const {
        return type_promoted::no;
    }

    type_check_result
    operator()(const iceberg::int_type&, const iceberg::long_type&) const {
        return type_promoted::yes;
    }

    type_check_result operator()(
      const iceberg::date_type&, const iceberg::timestamp_type&) const {
        // TODO(iceberg): This is a v3-only type promotion that can affect
        // partitioning. When we introduce v3 support, we should reinstante this
        // promotion with the type_promoted::changes_partition result. return
        return compat_errc::mismatch;
    }

    type_check_result
    operator()(const iceberg::float_type&, const iceberg::double_type&) {
        return type_promoted::yes;
    }

    type_check_result operator()(
      const iceberg::decimal_type& src, const iceberg::decimal_type& dst) {
        if (iceberg::primitive_type{src} == iceberg::primitive_type{dst}) {
            return type_promoted::no;
        }
        if ((dst.scale == src.scale && dst.precision > src.precision)) {
            return type_promoted::yes;
        }
        return compat_errc::mismatch;
    }

    type_check_result
    operator()(const iceberg::fixed_type& src, const iceberg::fixed_type& dst) {
        if (iceberg::primitive_type{src} == iceberg::primitive_type{dst}) {
            return type_promoted::no;
        }
        return compat_errc::mismatch;
    }
};

constexpr auto primitive_type_promotion_policy_visitor_v
  = primitive_type_promotion_policy_visitor{};

struct field_type_check_visitor {
    explicit field_type_check_visitor(
      primitive_type_promotion_policy_visitor policy)
      : policy(policy) {}

    template<typename T, typename U>
    requires(!std::is_same_v<T, U>)
    type_check_result operator()(const T&, const U&) const {
        return compat_errc::mismatch;
    }

    // For non-primitives, type identity is sufficient to pass this check.
    // e.g. any two struct types will pass w/o indicating type promotion,
    // whereas a struct and, say, a list would produce a mismatch error code.
    // The member fields of two such structs (or the element fields of two
    // lists, k/v for a map, etc.) will be checked elsewhere.
    template<typename T>
    type_check_result operator()(const T&, const T&) const {
        return type_promoted::no;
    }

    type_check_result
    operator()(const primitive_type& src, const primitive_type& dest) {
        return std::visit(policy, src, dest);
    }

private:
    primitive_type_promotion_policy_visitor policy;
};

/**
 * update_field - Field appears in both source and dest schema and the two are
 * _structurally_ compatible. Requiredness & primitive type invariants are
 * checked downstream.
 *
 * Update the destination field metadata with field info about the source field:
 *   - The ID
 *   - The requiredness
 *   - The type of the source field (if primitive)
 *
 * Update the source field metadata to indicate that it was not dropped from the
 * schema.
 */
schema_transform_state
update_field(const nested_field& src, const nested_field& dest) {
    src.set_evolution_metadata(nested_field::removed::no);
    auto src_type = [](const field_type& t) -> std::optional<primitive_type> {
        if (std::holds_alternative<primitive_type>(t)) {
            return std::get<primitive_type>(t);
        }
        return std::nullopt;
    }(src.type);
    dest.set_evolution_metadata(
      nested_field::src_info{
        .id = src.id,
        .required = src.required,
        .type = src_type,
      });
    return {};
}

/**
 * add_field - This is a brand new field. Mark it as such.
 *
 * Metadata indicates to downstream systems that this field needs a fresh,
 * table-unique ID.
 */
schema_transform_state add_field(const nested_field& f) {
    f.set_evolution_metadata(nested_field::is_new{});
    return {.n_added = 1};
}

/**
 * remove_field - This field (from the source schema) does not appear in the
 * destination schema. Mark it as such.
 */
schema_transform_state
remove_field(const nested_field& f, const partition_spec& pspec) {
    f.set_evolution_metadata(nested_field::removed::yes);
    if (pspec.get_field(f.id) != nullptr) {
        return {.n_removed = 1, .n_removed_partition_fields = 1};
    }
    return {.n_removed = 1};
}

/**
 * annotate_schema_visitor - visit all the fields of two struct_types,
 * recursively, in lockstep.
 *
 * This visitor encodes _structural_ invariants. Type checking, requiredness
 * checking, and (eventually) write/initial default wiring are deferred to a
 * later step (see below).
 */
class annotate_schema_visitor {
public:
    explicit annotate_schema_visitor(const partition_spec& pspec)
      : pspec_(&pspec) {}
    schema_transform_result
    visit(const struct_type& source_t, const struct_type& dest_t) && {
        return std::invoke(*this, source_t, dest_t);
    }

    schema_transform_result operator()(
      const struct_type& source_struct, const struct_type& dest_struct) const {
        schema_transform_state state{};

        for (const auto& f : dest_struct.fields) {
            if (f == nullptr) {
                return schema_evolution_errc::null_nested_field;
            }
            if (
              auto res = std::invoke(*this, source_struct, *f);
              res.has_error()) {
                return res.error();
            } else {
                state += res.value();
            }
        }

        // for any nested field in the source struct not visited and mapped to
        // the dest struct, recursively mark that field and all nested fields
        // therein for removal. This is helpful for column ID accounting
        // downstream.
        if (
          auto res = for_each_field(
            source_struct,
            [this, &state](const nested_field* f) {
                state += remove_field(*f, *pspec_);
            },
            [](const nested_field* f) {
                // visit only those fields that were not already marked (i.e.
                // mapped correctly into the destination struct). assume that
                // if a field is marked already then all fields nested under
                // it are also marked
                return !f->has_evolution_metadata();
            });
          res.has_error()) {
            return res.error();
        }

        return state;
    }

    schema_transform_result operator()(
      const struct_type& source_parent, const nested_field& dest_field) const {
        // Note that column renaming is NOT supported
        auto matches = source_parent.fields
                       | std::views::filter(
                         [&dest_field](const nested_field_ptr& nf) {
                             return nf != nullptr
                                    && nf->name == dest_field.name;
                         });

        auto match_it = matches.begin();
        auto n_matches = std::distance(match_it, matches.end());

        schema_transform_state state{};

        if (n_matches == 0) {
            // Since this is a new field, we don't have any source schema
            // context to push down into the recursion. Instead, visit the
            // destination field directly, with nesting, calling add_field
            // for each.
            if (
              auto res = for_each_field(
                dest_field,
                [&state](const nested_field* f) { state += add_field(*f); });
              res.has_error()) {
                return res.error();
            }
        } else if (n_matches == 1) {
            const auto& source_field = *match_it;
            if (
              auto vt_res = std::visit(
                *this, source_field->type, dest_field.type);
              vt_res.has_error()) {
                return vt_res.error();
            } else {
                state += vt_res.value();
            }
            state += update_field(*source_field, dest_field);
        } else {
            return schema_evolution_errc::ambiguous;
        }

        return state;
    }

    schema_transform_result
    operator()(const list_type& source_list, const list_type& dest_list) const {
        const auto& source_element = source_list.element_field;
        auto& dest_element = dest_list.element_field;

        vassert(
          source_element != nullptr && dest_element != nullptr,
          "List element fields assumed to be non-NULL");

        schema_transform_state state{};

        if (
          auto ve_res = std::visit(
            *this, source_element->type, dest_element->type);
          ve_res.has_error()) {
            return ve_res.error();
        } else {
            state += ve_res.value();
        }

        state += update_field(*source_element, *dest_element);

        return state;
    }

    schema_transform_result
    operator()(const map_type& source_map, const map_type& dest_map) const {
        const auto& source_key = source_map.key_field;
        auto& dest_key = dest_map.key_field;

        vassert(
          source_key != nullptr && dest_key != nullptr,
          "Map key fields are assumed to be non-NULL");

        schema_transform_state state{};

        if (
          auto vk_res = std::visit(*this, source_key->type, dest_key->type);
          vk_res.has_error()) {
            return vk_res.error();
        } else if (vk_res.value().total() > 0) {
            return schema_evolution_errc::violates_map_key_invariant;
        }
        state += update_field(*source_key, *dest_key);

        const auto& source_value = source_map.value_field;
        auto& dest_value = dest_map.value_field;

        vassert(
          source_value != nullptr && dest_value != nullptr,
          "Map val fields are assumed to be non-NULL");

        if (
          auto vv_res = std::visit(*this, source_value->type, dest_value->type);
          vv_res.has_error()) {
            return vv_res.error();
        } else {
            state += vv_res.value();
        }

        state += update_field(*source_value, *dest_value);

        return state;
    }

    schema_transform_result
    operator()(const primitive_type&, const primitive_type&) const {
        // this is a leaf by definition, so all we care about is the overload
        // resolving. the underlying primitive types are resolved and checked
        // later

        // unused
        return schema_transform_state{};
    }

    template<typename S, typename D>
    requires(!std::is_same_v<S, D>)
    schema_transform_result operator()(const S&, const D&) const {
        return schema_evolution_errc::incompatible;
    }

private:
    const partition_spec* pspec_;
};

/**
 * validate_transform_visitor - validate evolution_metadata for some nested
 * field and assign field ID as appropriate. Note new fields and promoted types
 * on the input schema_transform_state.
 *
 * Considerations:
 *   - Adding a required column is allowed by the Iceberg spec but omitted
 *     here. We may implement it in the future.
 *   - Changing an optional column to required is not allowed, per spec.
 *   - The type check on src_info is for primitive types only. The annotation
 *     step checks structural type compatibility.
 *   - Assigning a fresh ID to a new column requires table metadata context,
 *     so that computation is deferred to a subsequent step..
 *   - TODO: support for initial-default and write-default
 */
struct validate_transform_visitor {
    explicit validate_transform_visitor(
      nested_field* f,
      schema_transform_state& state,
      const partition_spec& spec)
      : f_(f)
      , state_(&state)
      , spec_(&spec) {}
    schema_errc_result operator()(const nested_field::src_info& src) {
        bool promoted = false;
        if (src.type.has_value()) {
            if (
              auto ct_res = check_types(src.type.value(), f_->type);
              ct_res.has_error()) {
                return schema_evolution_errc::type_mismatch;
            } else if (
              ct_res.value() == type_promoted::changes_partition
              && spec_->get_field(src.id) != nullptr) {
                return schema_evolution_errc::partition_spec_conflict;
            } else {
                promoted = ct_res.value() != type_promoted::no;
            }
        }
        if (
          src.required == field_required::no
          && f_->required == field_required::yes) {
            return schema_evolution_errc::new_required_field;
        } else if (src.required != f_->required) {
            promoted = true;
        }
        *state_ += schema_transform_state{.n_promoted = promoted ? 1ul : 0ul};
        f_->id = src.id;
        return std::nullopt;
    }

    schema_errc_result operator()(const nested_field::is_new&) {
        if (f_->required == field_required::yes) {
            return schema_evolution_errc::new_required_field;
        }
        *state_ += schema_transform_state{.n_added = 1};
        return std::nullopt;
    }

    template<typename T>
    schema_errc_result operator()(const T&) {
        return schema_evolution_errc::invalid_state;
    }

private:
    nested_field* f_;
    schema_transform_state* state_;
    const partition_spec* spec_;
};

struct primitive_value_type_promoting_visitor {
    template<typename TVal, typename TType>
    primitive_value operator()(TVal v, const TType& t) const {
        if constexpr (std::is_same_v<TType, primitive_value_type_t<TVal>>) {
            return v;
        }
        if constexpr (
          std::is_same_v<TVal, int_value> && std::is_same_v<TType, long_type>) {
            return long_value{v.val};
        }
        if constexpr (
          std::is_same_v<TVal, float_value>
          && std::is_same_v<TType, double_type>) {
            return double_value{v.val};
        }
        throw std::invalid_argument(
          fmt::format("Cannot promote primitive value {} to type {}", v, t));
    }
};

} // namespace

type_check_result
check_types(const iceberg::field_type& src, const iceberg::field_type& dest) {
    return std::visit(
      field_type_check_visitor{primitive_type_promotion_policy_visitor{}},
      src,
      dest);
}

primitive_value promote_primitive_value_type(
  primitive_value value, const primitive_type& dest_type) {
    return std::visit(
      primitive_value_type_promoting_visitor{}, std::move(value), dest_type);
}

schema_transform_result annotate_schema_transform(
  const struct_type& source,
  const struct_type& dest,
  const partition_spec& spec) {
    return annotate_schema_visitor{spec}.visit(source, dest);
}

schema_transform_result validate_schema_transform(
  const schema_transform_result& annotate_res,
  struct_type& dest,
  const partition_spec& spec) {
    if (annotate_res.has_error()) {
        return annotate_res.error();
    }
    if (annotate_res.value().n_removed_partition_fields > 0) {
        return schema_evolution_errc::partition_spec_conflict;
    }
    auto state = annotate_res.value();
    if (
      auto res = for_each_field(
        dest,
        [&state, &spec](nested_field* f) {
            vassert(
              f->has_evolution_metadata(),
              "Should have visited every destination field");
            return std::visit(
              validate_transform_visitor{f, state, spec}, f->meta);
        });
      res.has_error()) {
        return res.error();
    }
    return state;
}

namespace {
schema_transform_result do_visit_schemas(
  const struct_type& source, struct_type& dest, const partition_spec& spec) {
    auto annotate_res = annotate_schema_transform(source, dest, spec);
    return validate_schema_transform(annotate_res, dest, spec);
}
} // namespace

schema_evolution_result evolve_schema(
  const struct_type& source, struct_type& dest, const partition_spec& spec) {
    if (auto res = do_visit_schemas(source, dest, spec); res.has_error()) {
        return res.error();
    } else {
        return schema_changed{res.value().total() > 0};
    }
}

namespace {

nested_field*
get_exactly_one_field_by_name(const struct_type& s, const ss::sstring& name) {
    auto host_matches = s.fields
                        | std::views::filter(
                          [&name](const nested_field_ptr& f) {
                              return f != nullptr && f->name == name;
                          });

    auto host_match_it = host_matches.begin();
    auto n_matches = std::distance(host_match_it, host_matches.end());

    switch (n_matches) {
    case 0:
        return nullptr;
    case 1:
        return host_match_it->get();
    default:
        // This should never happen and we can't handle it anyway.
        ss::throw_with_backtrace<std::runtime_error>(fmt::format(
          "Ambiguous field name: {}. Matches: {}.", name, n_matches));
    }
}
} // namespace

namespace {

/// Visitor that maps field IDs from a host struct schema to a writer schema by
/// matching field names and validating type compatibility.
///
/// This is used when writing data to an Iceberg table: the host struct type
/// represents the existing table schema (with assigned field IDs), and the
/// writer struct type represents the data we want to write (without IDs).
///
/// The visitor follows Iceberg schema evolution rules:
/// - Writer fields must exist in the host schema (by name)
/// - Writer can be a subset of host (not all host fields required)
/// - Field types must be identical or promotable (e.g., int32 -> int64)
/// - Required fields in host cannot become optional in writer
///
/// On success, writer fields are assigned the corresponding host field
/// IDs. On failure, writer struct is in an undefined state and should be
/// thrown away.
struct ids_filling_visitor {
    ids_filled operator()(
      const struct_type& host_struct, const struct_type& writer_struct) const {
        for (const auto& writer_field : writer_struct.fields) {
            auto host_field = get_exactly_one_field_by_name(
              host_struct, writer_field->name);

            if (!host_field) {
                return ids_filled::no;
            } else {
                // Check properties.
                if (
                  host_field->required == field_required::yes
                  && writer_field->required == field_required::no) {
                    // If the host field is required then writer can not
                    // be nullable (i.e. contain nulls).
                    return ids_filled::no;
                }

                // Check nested types.
                auto nested_filled = std::visit(
                  *this, host_field->type, writer_field->type);
                if (nested_filled == ids_filled::no) {
                    return ids_filled::no;
                } else {
                    writer_field->id = host_field->id;
                }
            }
        }

        // All fields visited.
        return ids_filled::yes;
    }

    ids_filled operator()(
      const list_type& host_list_type,
      const list_type& writer_list_type) const {
        const auto& host_element = host_list_type.element_field;
        auto& writer_element = writer_list_type.element_field;

        auto nested_filled = std::visit(
          *this, host_element->type, writer_element->type);
        if (nested_filled == ids_filled::yes) {
            writer_element->id = host_element->id;
            return ids_filled::yes;
        } else {
            return ids_filled::no;
        }
    }

    ids_filled operator()(
      const map_type& host_map_type, const map_type& writer_map_type) const {
        const auto& host_key = host_map_type.key_field;
        auto& writer_key = writer_map_type.key_field;
        const auto& host_value = host_map_type.value_field;
        auto& writer_value = writer_map_type.value_field;

        vassert(
          host_key != nullptr && writer_key != nullptr,
          "Map key fields assumed to be non-NULL");
        vassert(
          host_value != nullptr && writer_value != nullptr,
          "Map value fields assumed to be non-NULL");

        // No changes allowed to map key type so check that before trying to
        // assign IDs.
        if (
          auto vk_res = std::visit(
            annotate_schema_visitor{partition_spec{}},
            make_copy(host_key->type),
            make_copy(writer_key->type));
          vk_res.has_error() || vk_res.value().total() > 0) {
            return ids_filled::no;
        }

        auto key_ids_filled = std::visit(
          *this, host_key->type, writer_key->type);
        if (key_ids_filled == ids_filled::yes) {
            writer_key->id = host_key->id;
        } else {
            return ids_filled::no;
        }

        auto value_ids_filled = std::visit(
          *this, host_value->type, writer_value->type);
        if (value_ids_filled == ids_filled::yes) {
            writer_value->id = host_value->id;
        } else {
            return ids_filled::no;
        }

        return ids_filled::yes;
    }

    ids_filled operator()(
      const primitive_type& host_type, primitive_type& writer_type) const {
        // If we can promote from destination (i.e. inserted data) to source
        // (i.e. schema) then it is ok. In other words, destination field can
        // inherit the id of the source field.
        //
        // Here we can't fill any ids but the result signals the parent that the
        // types are compatible.
        auto res = std::visit(
          primitive_type_promotion_policy_visitor_v, writer_type, host_type);
        if (res.has_error()) {
            return ids_filled::no;
        }

        switch (res.value()) {
        case type_promoted::no:
            return ids_filled::yes;
        case type_promoted::yes:
            return ids_filled::yes;
        case type_promoted::changes_partition:
            ss::throw_with_backtrace<std::runtime_error>(fmt::format(
              "Unexpected result from check_types for {} and {}",
              writer_type,
              host_type));
        }
    }

    template<typename S, typename D>
    requires(!std::is_same_v<S, D>)
    ids_filled operator()(const S&, const D&) const {
        return ids_filled::no;
    }
};

} // namespace

ids_filled try_fill_field_ids(
  const struct_type& host_struct_type, struct_type& writer_struct_type) {
    return std::invoke(
      ids_filling_visitor{}, host_struct_type, writer_struct_type);
}

namespace {
struct merging_schema_visitor {
    schema_merge_result operator()(
      const struct_type& writer_struct_type,
      struct_type& host_struct_type) const {
        for (const auto& writer_field : writer_struct_type.fields) {
            auto host_field = get_exactly_one_field_by_name(
              host_struct_type, writer_field->name);

            if (!host_field) {
                // Add the field to the host struct since no matching field was
                // found.
                host_struct_type.fields.push_back(writer_field->copy());
                host_struct_type.fields.back()->id = nested_field::id_t{};
            } else {
                // Match found. Check field compatibility (recursively if
                // needed).
                auto field_merge_res = std::visit(
                  *this, writer_field->type, host_field->type);
                if (field_merge_res.has_error()) {
                    return field_merge_res.error();
                }

                // Merge field properties.
                if (host_field->required == field_required::yes) {
                    // If the host field is required the writer field is
                    // allowed to relax nullability.
                    host_field->required = writer_field->required;
                }
            }
        }

        return outcome::success();
    }

    schema_merge_result operator()(
      const primitive_type& writer_type, primitive_type& host_type) const {
        // Find the super type between the writer and host types and assign it
        // to the host type.

        if (writer_type == host_type) {
            // If the types are already the same there is nothing to be done.
            return outcome::success();
        } else {
            // ... otherwise, we find the supertype by trying to promote first
            // the writer to the host type. If promotion is allowed, we're done.
            // If promotion fails, we check if host_type can be promoted to the
            // writer type. If it can, then we set host_type to the writer type.
            auto res = std::visit(
              primitive_type_promotion_policy_visitor_v,
              writer_type,
              host_type);

            if (res.has_value()) {
                switch (res.value()) {
                case type_promoted::no:
                    // Both types are the same.
                    return outcome::success();
                case type_promoted::yes:
                    // Writer type is promotable to/compatible with the host
                    // type.
                    return outcome::success();
                case type_promoted::changes_partition:
                    // TODO: Update the return type so that this impossible
                    // value does not need to be handled.
                    return schema_evolution_errc::incompatible;
                }
            } else {
                // writer -> host promotion fails, let's check the opposite
                // direction and upgrade/promote the field if needed.
                auto res = std::visit(
                  primitive_type_promotion_policy_visitor_v,
                  host_type,
                  writer_type);
                if (res.has_error()) {
                    // Errors in both directions. The fields are incompatible at
                    // all/unrelated types.
                    return schema_evolution_errc::incompatible;
                } else {
                    switch (res.value()) {
                    case type_promoted::no:
                        // Both types are the same.
                        return outcome::success();
                    case type_promoted::yes:
                        host_type = writer_type; // promote the type
                        return outcome::success();
                    case type_promoted::changes_partition:
                        // TODO: Update the return type so that this impossible
                        // value does not need to be handled.
                        return schema_evolution_errc::incompatible;
                    }
                }
            }
        }
    }

    schema_merge_result operator()(
      const list_type& writer_list_type,
      const list_type& host_list_type) const {
        const auto& writer_element = writer_list_type.element_field;
        auto& host_element = host_list_type.element_field;

        // Merge list element type recursively.
        vassert(
          writer_element != nullptr && host_element != nullptr,
          "List element fields assumed to be non-NULL");
        if (
          auto ve_res = std::visit(
            *this, writer_element->type, host_element->type);
          ve_res.has_error()) {
            return ve_res.error();
        }
        return outcome::success();
    }

    schema_merge_result operator()(
      const map_type& writer_map_type, const map_type& host_map_type) const {
        const auto& writer_key = writer_map_type.key_field;
        auto& host_key = host_map_type.key_field;
        const auto& writer_value = writer_map_type.value_field;
        auto& host_value = host_map_type.value_field;

        vassert(
          writer_key != nullptr && host_key != nullptr,
          "Map key fields assumed to be non-NULL");
        vassert(
          writer_value != nullptr && host_value != nullptr,
          "Map value fields assumed to be non-NULL");

        // No changes allowed to map key type as doing so would change the
        // equality. The limitation is not in the spec but is in all
        // implementations.
        // https://iceberg.apache.org/docs/1.9.0/evolution/#schema-evolution
        if (
          auto vk_res = std::visit(
            annotate_schema_visitor{partition_spec{}},
            make_copy(writer_key->type),
            make_copy(host_key->type));
          vk_res.has_error()) {
            return vk_res.error();
        } else if (vk_res.value().total() > 0) {
            return schema_evolution_errc::violates_map_key_invariant;
        }

        auto value_merge_res = std::visit(
          *this, writer_value->type, host_value->type);
        if (value_merge_res.has_error()) {
            return value_merge_res.error();
        }

        return outcome::success();
    }

    template<typename S, typename D>
    requires(!std::is_same_v<S, D>)
    schema_merge_result operator()(const S&, const D&) const {
        return schema_evolution_errc::incompatible;
    }
};

} // namespace

schema_merge_result merge_struct_types(
  const struct_type& writer_struct_type, struct_type& host_struct_type) {
    return std::invoke(
      merging_schema_visitor{}, writer_struct_type, host_struct_type);
}

} // namespace iceberg
