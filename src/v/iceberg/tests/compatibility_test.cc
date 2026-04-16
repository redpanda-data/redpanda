/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "absl/container/btree_map.h"
#include "bytes/bytes.h"
#include "datalake/partition_spec_parser.h"
#include "iceberg/compatibility.h"
#include "iceberg/compatibility_types.h"
#include "iceberg/compatibility_utils.h"
#include "iceberg/datatypes.h"
#include "iceberg/partition.h"
#include "random/generators.h"

#include <boost/range/irange.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h>

#include <iostream>
#include <unordered_set>
#include <vector>

using namespace iceberg;

namespace {

void reset_field_ids(struct_type& type) {
    std::ignore = for_each_field(type, [](nested_field* f) {
        f->id = nested_field::id_t{0};
        f->meta = std::nullopt;
    });
}

/**
 * Less strict than an equality check.
 *   - Collects the fields for each param in sorted order by ID
 *   - Checks that IDs are unique in both input structs
 *   - Checks that input structs have the same number of fields
 *   - Checks that corresponding (by ID) lhs fields are equivalent to rhs
 *     fields, matching name, type, nullability
 */
bool structs_equivalent(const struct_type& lhs, const struct_type& rhs) {
    using field_map_t
      = absl::btree_map<nested_field::id_t, const nested_field*>;

    auto collect_fields =
      [](const struct_type& s) -> std::pair<field_map_t, bool> {
        field_map_t fields;
        bool unique_ids = true;
        std::ignore = for_each_field(
          s, [&fields, &unique_ids](const nested_field* f) {
              auto res = fields.emplace(f->id, f);
              unique_ids = unique_ids && res.second;
          });
        return std::make_pair(std::move(fields), unique_ids);
    };

    auto [lhs_fields, lhs_uniq] = collect_fields(lhs);
    auto [rhs_fields, rhs_uniq] = collect_fields(rhs);

    if (!lhs_uniq || !rhs_uniq) {
        return false;
    }
    if (lhs_fields.size() != rhs_fields.size()) {
        return false;
    }

    static constexpr auto fields_equivalent =
      [](const nested_field* lf, const nested_field* rf) {
          if (
            lf->id != rf->id || lf->name != rf->name
            || lf->required != rf->required) {
              return false;
          }
          auto res = check_types(lf->type, rf->type);
          return !res.has_error() && res.value() == type_promoted::no;
      };

    return std::ranges::all_of(lhs_fields, [&rhs_fields](const auto lhs_pr) {
        auto rhs_it = rhs_fields.find(lhs_pr.first);
        if (rhs_it == rhs_fields.end()) {
            return false;
        }
        return fields_equivalent(lhs_pr.second, rhs_it->second);
    });
}

struct unique_id_generator {
    static constexpr int max = 100000;

    nested_field::id_t get_one() {
        int id = random_generators::get_int(1, max);
        while (used.contains(id)) {
            id = random_generators::get_int(1, max);
        }
        used.insert(id);
        return nested_field::id_t{id};
    }
    std::unordered_set<int> used;
};

bool updated(const nested_field& src, const nested_field& dest) {
    return std::holds_alternative<nested_field::src_info>(dest.meta)
           && std::get<nested_field::src_info>(dest.meta).id == dest.id
           && dest.id == src.id;
}

bool updated(const nested_field& dest) {
    return std::holds_alternative<nested_field::src_info>(dest.meta);
}

bool added(const nested_field& f) {
    return std::holds_alternative<nested_field::is_new>(f.meta);
}

bool removed(const nested_field& f) {
    return std::holds_alternative<nested_field::removed>(f.meta)
           && std::get<nested_field::removed>(f.meta)
                == nested_field::removed::yes;
}

template<typename T>
T& get(const nested_field_ptr& f) {
    vassert(
      std::holds_alternative<T>(f->type),
      "Unexpected variant type: {}",
      f->type.index());
    return std::get<T>(f->type);
}

using compat = ss::bool_class<struct compat_tag>;

struct field_test_case {
    field_test_case(
      field_type source, field_type dest, type_check_result expected)
      : source(std::move(source))
      , dest(std::move(dest))
      , expected(expected) {}

    field_test_case(const field_test_case& other)
      : source(make_copy(other.source))
      , dest(make_copy(other.dest))
      , expected(
          other.expected.has_error()
            ? type_check_result{other.expected.error()}
            : type_check_result{other.expected.value()}) {}

    field_test_case(field_test_case&&) = default;
    field_test_case& operator=(const field_test_case& other) = delete;
    field_test_case& operator=(field_test_case&&) = delete;
    ~field_test_case() = default;

    field_type source;
    field_type dest;
    type_check_result expected{compat_errc::mismatch};
};

std::ostream& operator<<(std::ostream& os, const field_test_case& ftc) {
    fmt::print(
      os,
      "{}->{} [expected: {}]",
      ftc.source,
      ftc.dest,
      ftc.expected.has_error() ? std::string{"ERROR"}
                               : fmt::format("{}", ftc.expected.value()));
    return os;
}
} // namespace

std::vector<field_test_case> generate_test_cases() {
    std::vector<field_test_case> test_data{};

    test_data.emplace_back(int_type{}, long_type{}, type_promoted::yes);
    test_data.emplace_back(int_type{}, boolean_type{}, compat_errc::mismatch);

    // TODO(iceberg): date -> timestamp is v3-only. When/if we support v3, we'll
    // want to reinstate this promotion.
    test_data.emplace_back(
      date_type{}, timestamp_type{}, compat_errc::mismatch);
    test_data.emplace_back(date_type{}, long_type{}, compat_errc::mismatch);

    test_data.emplace_back(float_type{}, double_type{}, type_promoted::yes);
    test_data.emplace_back(
      float_type{}, fixed_type{.length = 64}, compat_errc::mismatch);

    test_data.emplace_back(
      decimal_type{.precision = 10, .scale = 2},
      decimal_type{.precision = 20, .scale = 2},
      type_promoted::yes);
    test_data.emplace_back(
      decimal_type{.precision = 10, .scale = 2},
      decimal_type{.precision = 10, .scale = 2},
      type_promoted::no);
    test_data.emplace_back(
      decimal_type{.precision = 20, .scale = 2},
      decimal_type{.precision = 10, .scale = 2},
      compat_errc::mismatch);

    test_data.emplace_back(
      fixed_type{.length = 32}, fixed_type{.length = 32}, type_promoted::no);
    test_data.emplace_back(
      fixed_type{.length = 32},
      fixed_type{.length = 64},
      compat_errc::mismatch);
    test_data.emplace_back(
      fixed_type{.length = 64},
      fixed_type{.length = 32},
      compat_errc::mismatch);

    struct_type s1{};
    struct_type s2{};
    s2.fields.emplace_back(
      nested_field::create(0, "foo", field_required::yes, int_type{}));
    field_type l1 = list_type::create(0, field_required::yes, int_type{});
    field_type l2 = list_type::create(0, field_required::no, string_type{});
    field_type m1 = map_type::create(
      0, int_type{}, 0, field_required::yes, date_type{});
    field_type m2 = map_type::create(
      0, string_type{}, 0, field_required::no, timestamptz_type{});

    // NOTE: basic type check doesn't descend into non-primitive types
    // Checking stops at type ID - i.e. compat(struct, struct) == true,
    // compat(struct, list) == false.
    test_data.emplace_back(s1.copy(), s1.copy(), type_promoted::no);
    test_data.emplace_back(s1.copy(), s2.copy(), type_promoted::no);
    test_data.emplace_back(make_copy(l1), make_copy(l1), type_promoted::no);
    test_data.emplace_back(make_copy(l1), make_copy(l2), type_promoted::no);
    test_data.emplace_back(make_copy(m1), make_copy(m1), type_promoted::no);
    test_data.emplace_back(make_copy(m1), make_copy(m2), type_promoted::no);

    std::vector<field_type> non_promotable_types;
    non_promotable_types.emplace_back(boolean_type{});
    non_promotable_types.emplace_back(long_type{});
    non_promotable_types.emplace_back(double_type{});
    non_promotable_types.emplace_back(time_type{});
    non_promotable_types.emplace_back(timestamp_type{});
    non_promotable_types.emplace_back(timestamptz_type{});
    non_promotable_types.emplace_back(string_type{});
    non_promotable_types.emplace_back(uuid_type{});
    non_promotable_types.emplace_back(binary_type{});
    non_promotable_types.emplace_back(s1.copy());
    non_promotable_types.emplace_back(make_copy(l1));
    non_promotable_types.emplace_back(make_copy(m1));

    for (const auto& fta : non_promotable_types) {
        for (const auto& ftb : non_promotable_types) {
            if (fta == ftb) {
                continue;
            }
            test_data.emplace_back(
              make_copy(fta), make_copy(ftb), compat_errc::mismatch);
        }
    }

    return test_data;
}

template<typename T>
struct CompatibilityTest
  : ::testing::Test
  , testing::WithParamInterface<T> {};

using PrimitiveCompatibilityTest = CompatibilityTest<field_test_case>;

INSTANTIATE_TEST_SUITE_P(
  PrimitiveTypeCompatibilityTest,
  PrimitiveCompatibilityTest,
  ::testing::ValuesIn(generate_test_cases()));

TEST_P(PrimitiveCompatibilityTest, CompatibleTypesAreCompatible) {
    const auto& p = GetParam();

    auto res = check_types(p.source, p.dest);
    ASSERT_EQ(res.has_error(), p.expected.has_error());
    if (res.has_error()) {
        ASSERT_EQ(res.error(), p.expected.error());
    } else {
        ASSERT_EQ(res.value(), p.expected.value());
    }
}

namespace {

struct_type nested_test_struct() {
    unique_id_generator ids{};

    struct_type nested_struct;
    struct_type key_struct;
    key_struct.fields.emplace_back(
      nested_field::create(
        ids.get_one(), "baz", field_required::yes, int_type{}));

    struct_type nested_value_struct;
    nested_value_struct.fields.emplace_back(
      nested_field::create(
        ids.get_one(), "nmv1", field_required::yes, int_type{}));
    nested_value_struct.fields.emplace_back(
      nested_field::create(
        ids.get_one(), "nmv2", field_required::yes, string_type{}));

    nested_struct.fields.emplace_back(
      nested_field::create(
        ids.get_one(),
        "quux",
        field_required::yes,
        map_type::create(
          ids.get_one(),
          std::move(key_struct),
          ids.get_one(),
          field_required::yes,
          map_type::create(
            ids.get_one(),
            string_type{},
            ids.get_one(),
            field_required::yes,
            std::move(nested_value_struct)))));

    struct_type location_struct;
    location_struct.fields.emplace_back(
      nested_field::create(
        ids.get_one(), "latitude", field_required::yes, float_type{}));
    location_struct.fields.emplace_back(
      nested_field::create(
        ids.get_one(), "longitude", field_required::yes, float_type{}));
    nested_struct.fields.emplace_back(
      nested_field::create(
        ids.get_one(),
        "location",
        field_required::yes,
        list_type::create(
          ids.get_one(), field_required::yes, std::move(location_struct))));

    return nested_struct;
}
} // namespace

struct struct_evolution_test_case {
    std::string_view description{};
    std::function<struct_type(unique_id_generator&)> generator;
    std::function<void(struct_type&)> update;
    checked<std::nullopt_t, schema_evolution_errc> annotate_err{std::nullopt};
    checked<std::nullopt_t, schema_evolution_errc> validate_err{std::nullopt};
    std::function<bool(const struct_type&, const struct_type&)> validator =
      [](const struct_type&, const struct_type&) { return true; };
    schema_changed any_change{true};
    std::optional<ss::sstring> pspec{};
};

std::ostream&
operator<<(std::ostream& os, const struct_evolution_test_case& tc) {
    return os << tc.description;
}

static const std::vector<struct_evolution_test_case> valid_cases{
  struct_evolution_test_case{
    .description = "valid primitive type promotion is OK",
    .generator =
      [](unique_id_generator& ids) {
          struct_type s{};
          s.fields.emplace_back(
            nested_field::create(
              ids.get_one(), "foo", field_required::no, int_type{}));
          return s;
      },
    .update = [](struct_type& s) { s.fields[0]->type = long_type{}; },
    .validator =
      [](const struct_type& src, const struct_type& dst) {
          return updated(*src.fields.back(), *dst.fields.back());
      },
    .pspec = "(foo)",
  },
  struct_evolution_test_case{
    .description = "list elements are subject to type promotion rules (valid)",
    .generator =
      [](unique_id_generator& ids) {
          struct_type s{};
          s.fields.emplace_back(
            nested_field::create(
              ids.get_one(),
              "qux",
              field_required::yes,
              list_type::create(
                ids.get_one(), field_required::yes, int_type{})));
          return s;
      },
    .update =
      [](struct_type& s) {
          get<list_type>(s.fields[0]).element_field->type = long_type{};
      },
    .validator =
      [](const struct_type& src, const struct_type& dst) {
          auto& src_list = get<list_type>(src.fields.back());
          auto& dst_list = get<list_type>(dst.fields.back());
          return updated(*src_list.element_field, *dst_list.element_field);
      },
  },
  struct_evolution_test_case{
    .description = "evolving a list-element struct is allowed",
    .generator =
      [](unique_id_generator& ids) {
          struct_type s{};
          s.fields.emplace_back(
            nested_field::create(
              ids.get_one(),
              "qux",
              field_required::yes,
              list_type::create(
                ids.get_one(), field_required::yes, struct_type{})));
          return s;
      },
    .update =
      [](struct_type& s) {
          get<struct_type>(get<list_type>(s.fields.back()).element_field)
            .fields.emplace_back(
              nested_field::create(0, "int", field_required::no, int_type{}));
      },
    .validator =
      [](const struct_type& src, const struct_type& dest) {
          auto& src_list = get<list_type>(src.fields.back());
          auto& dst_list = get<list_type>(dest.fields.back());
          return updated(*src_list.element_field, *dst_list.element_field);
      },
  },
  struct_evolution_test_case{
    .description
    = "map keys & values are subject to type promotion rules (valid)",
    .generator =
      [](unique_id_generator& ids) {
          struct_type s{};
          s.fields.emplace_back(
            nested_field::create(
              ids.get_one(),
              "a_map",
              field_required::no,
              map_type::create(
                ids.get_one(),
                int_type{},
                ids.get_one(),
                field_required::no,
                float_type{})));
          return s;
      },
    .update =
      [](struct_type& s) {
          auto& map_t = get<map_type>(s.fields[0]);
          map_t.key_field->type = long_type{};
          map_t.value_field->type = double_type{};
      },
    .validator =
      [](const struct_type& src, const struct_type& dst) {
          const auto& src_map = get<map_type>(src.fields.back());
          const auto& dst_map = get<map_type>(dst.fields.back());
          return updated(*src_map.key_field, *dst_map.key_field)
                 && updated(*src_map.value_field, *dst_map.value_field);
      },
  },
  struct_evolution_test_case{
    .description = "we can 'add' nested fields",
    .generator = [](unique_id_generator&) { return struct_type{}; },
    .update =
      [](struct_type& s) {
          struct_type list_element{};
          list_element.fields.emplace_back(
            nested_field::create(0, "f1", field_required::no, int_type{}));
          struct_type nested_struct{};
          nested_struct.fields.emplace_back(
            nested_field::create(
              0,
              "nested_list",
              field_required::no,
              list_type::create(0, field_required::no, date_type{})));
          list_element.fields.emplace_back(
            nested_field::create(
              0, "f2", field_required::no, std::move(nested_struct)));
          s.fields.emplace_back(
            nested_field::create(
              0,
              "nested",
              field_required::no,
              list_type::create(
                0, field_required::no, std::move(list_element))));
      },
    .validator =
      [](const struct_type&, const struct_type& dest) {
          bool all_assigned = true;
          chunked_vector<nested_field*> stk;
          bool err
            = for_each_field(dest, [&all_assigned](const nested_field* f) {
                  all_assigned = all_assigned && (updated(*f) || added(*f));
              }).has_error();

          return !err && all_assigned;
      },
  },
  struct_evolution_test_case{
    .description = "we can add nested fields in the middle of a schema",
    .generator =
      [](unique_id_generator& ids) {
          struct_type s{};
          s.fields.emplace_back(
            nested_field::create(
              ids.get_one(), "foo", field_required::yes, int_type{}));
          s.fields.emplace_back(
            nested_field::create(
              ids.get_one(), "bar", field_required::yes, float_type{}));
          return s;
      },
    .update =
      [](struct_type& s) {
          s.fields.emplace_back(
            nested_field::create(0, "baz", field_required::no, string_type{}));
          std::swap(s.fields[1], s.fields[2]);
      },
    .validator =
      [](const struct_type& src, const struct_type& dest) {
          auto orig_match = *src.fields[0] == *dest.fields[0]
                            && *src.fields[1] == *dest.fields[2];
          return orig_match && updated(*dest.fields[0])
                 && updated(*dest.fields[2]) && added(*dest.fields[1]);
      },
    .pspec = "(foo,bar)",
  },
  struct_evolution_test_case{
    .description = "removing a required field works",
    .generator =
      [](unique_id_generator& ids) {
          struct_type s{};
          s.fields.emplace_back(
            nested_field::create(
              ids.get_one(), "foo", field_required::yes, int_type{}));
          return s;
      },
    .update = [](struct_type& s) { s.fields.pop_back(); },
    .validator =
      [](const struct_type& src, const struct_type& dst) {
          return dst.fields.empty() && removed(*src.fields.back());
      },
  },
  struct_evolution_test_case{
    .description = "field removal respects the original nesting",
    .generator =
      [](unique_id_generator& ids) {
          struct_type s{};
          struct_type nested{};
          nested.fields.emplace_back(
            nested_field::create(
              ids.get_one(), "foo", field_required::yes, int_type{}));
          s.fields.emplace_back(
            nested_field::create(
              ids.get_one(), "nested", field_required::yes, std::move(nested)));

          return s;
      },
    .update =
      [](struct_type& s) {
          get<struct_type>(s.fields.back()).fields.pop_back();
      },
    .validator =
      [](const struct_type& src, const struct_type& dest) {
          auto& dst_nested = get<struct_type>(dest.fields.back());
          auto& src_nested = get<struct_type>(src.fields.back());
          return src.fields.size() == dest.fields.size()
                 && dst_nested.fields.empty()
                 && removed(*src_nested.fields.back());
      },
  },
  struct_evolution_test_case{
    .description
    = "grouping multiple fields into a struct won't produce any errors,"
      "but the struct and contents are all treated as new fields with"
      "new IDs, and the original fields are removed",
    .generator =
      [](unique_id_generator& ids) {
          struct_type s;
          s.fields.emplace_back(
            nested_field::create(
              ids.get_one(), "foo", field_required::no, int_type{}));
          s.fields.emplace_back(
            nested_field::create(
              ids.get_one(), "bar", field_required::no, string_type{}));
          s.fields.emplace_back(
            nested_field::create(
              ids.get_one(), "baz", field_required::no, double_type{}));
          return s;
      },
    .update =
      [](struct_type& s) {
          struct_type foobarbaz{};
          std::move(
            s.fields.begin(),
            s.fields.end(),
            std::back_inserter(foobarbaz.fields));
          s.fields.clear();
          s.fields.emplace_back(
            nested_field::create(
              0, "foobarbaz", field_required::no, std::move(foobarbaz)));
      },
    .validator =
      [](const struct_type& src, const struct_type& dest) {
          auto& new_struct = get<struct_type>(dest.fields[0]);
          // the three struct fields have the same names as but different IDs
          // from the three fields in the source struct, which are removed
          bool struct_fields_ok = std::ranges::all_of(
            boost::irange(0UL, new_struct.fields.size()),
            [&new_struct, &src](auto i) {
                if (i > new_struct.fields.size() || i > src.fields.size()) {
                    return false;
                }
                auto& new_f = new_struct.fields[i];
                auto& orig_f = src.fields[i];
                return new_f->name == orig_f->name && new_f->id != orig_f->id
                       && added(*new_f) && removed(*orig_f);
            });

          return struct_fields_ok && dest.fields.size() == 1;
      },
  },
  struct_evolution_test_case{
    .description = "a map value can be manipulated as usual",
    .generator = [](unique_id_generator&) { return nested_test_struct(); },
    .update =
      [](struct_type& s) {
          auto& map = get<map_type>(s.fields[0]);
          auto& nested_map = get<map_type>(map.value_field);
          auto& val = get<struct_type>(nested_map.value_field);
          val.fields.front()->type = long_type{};
          val.fields.emplace_back(
            nested_field::create(0, "nmv3", field_required::no, double_type{}));
      },
    .validator =
      [](const struct_type& src, const struct_type& dst) {
          auto& src_map = get<map_type>(
            get<map_type>(src.fields[0]).value_field);
          auto& dst_map = get<map_type>(
            get<map_type>(dst.fields[0]).value_field);
          auto& dst_val = get<struct_type>(dst_map.value_field);
          return updated(*src_map.value_field, *dst_map.value_field)
                 && added(*dst_val.fields.back());
      },
  },
  struct_evolution_test_case{
    .description = "promoting a field from required to optional is allowed",
    .generator =
      [](unique_id_generator& ids) {
          struct_type s{};
          s.fields.emplace_back(
            nested_field::create(
              ids.get_one(), "foo", field_required::yes, int_type{}));
          return s;
      },
    .update =
      [](struct_type& s) {
          s.fields.back()->required = field_required::no;
          s.fields.back()->type = long_type{};
      },
    .validator =
      [](const struct_type& src, const struct_type& dst) {
          auto& src_f = src.fields.back();
          auto& dst_f = dst.fields.back();
          return src_f->required != dst_f->required && updated(*src_f, *dst_f);
      },
    .pspec = "(foo)",
  },
  struct_evolution_test_case{
    .description = "reordering fields is legal",
    .generator = [](unique_id_generator&) { return nested_test_struct(); },
    .update =
      [](struct_type& s) {
          auto& quux = get<map_type>(s.fields.front());
          auto& quux_val = get<map_type>(quux.value_field);
          auto& quux_val_val = get<struct_type>(quux_val.value_field);
          std::swap(quux_val_val.fields.front(), quux_val_val.fields.back());

          auto& location = get<list_type>(s.fields.back());
          auto& location_elt = get<struct_type>(location.element_field);
          std::swap(location_elt.fields.front(), location_elt.fields.back());
          std::swap(s.fields.front(), s.fields.back());
      },
    .validator =
      [](const struct_type& src, const struct_type& dst) {
          bool all_updated = true;

          if (
            auto res = for_each_field(
              dst,
              [&all_updated](const nested_field* f) {
                  all_updated = all_updated && updated(*f);
              });
            res.has_error()) {
              return false;
          }

          return all_updated && structs_equivalent(src, dst);
      },
    // TODO(oren): do we need to detect field reordering? or just support it?
    // should reordered fields in a schema correspond to some parquet layout
    // change on disk?
    .any_change = schema_changed::no,
  },
  struct_evolution_test_case{
    .description
    = "renaming a field is not ID preserving. the renamed field is "
      "'added' and the old one 'removed'",
    .generator =
      [](unique_id_generator& ids) {
          struct_type s{};
          s.fields.emplace_back(
            nested_field::create(
              ids.get_one(), "foo", field_required::no, int_type{}));
          return s;
      },
    .update = [](struct_type& s) { s.fields.back()->name = "bar"; },
    .validator =
      [](const struct_type& src, const struct_type& dest) {
          const auto& s = src.fields.back();
          const auto& d = dest.fields.back();
          return s->name != d->name && s->id != d->id && removed(*s)
                 && added(*d);
      },
  },
  struct_evolution_test_case{
    .description = "removing a field marks all nested fields as removed",
    .generator =
      [](unique_id_generator& ids) {
          struct_type s{};
          struct_type nested{};
          nested.fields.emplace_back(
            nested_field::create(
              ids.get_one(), "foo", field_required::no, int_type{}));
          nested.fields.emplace_back(
            nested_field::create(
              ids.get_one(),
              "bar",
              field_required::no,
              list_type::create(
                ids.get_one(), field_required::no, int_type{})));
          s.fields.emplace_back(
            nested_field::create(
              ids.get_one(), "nested", field_required::no, std::move(nested)));
          return s;
      },
    .update = [](struct_type& s) { s.fields.pop_back(); },
    .validator =
      [](const struct_type& src, const struct_type&) {
          bool all_removed = true;
          bool err = for_each_field(src, [&all_removed](const nested_field* f) {
                         all_removed = all_removed && removed(*f);
                     }).has_error();

          return !err && all_removed;
      },
  },
};

static const std::vector<struct_evolution_test_case> invalid_cases{
  struct_evolution_test_case{
    .description = "invalid primitive type promotions are rejected",
    .generator =
      [](unique_id_generator& ids) {
          struct_type s{};
          s.fields.emplace_back(
            nested_field::create(
              ids.get_one(), "foo", field_required::no, int_type{}));
          return s;
      },
    .update = [](struct_type& s) { s.fields[0]->type = string_type{}; },
    .validate_err = schema_evolution_errc::type_mismatch,
  },
  struct_evolution_test_case{
    .description
    = "list elements are subject to type promotion rules (invalid)",
    .generator =
      [](unique_id_generator& ids) {
          struct_type s{};
          s.fields.emplace_back(
            nested_field::create(
              ids.get_one(),
              "qux",
              field_required::yes,
              list_type::create(
                ids.get_one(), field_required::yes, int_type{})));
          return s;
      },
    .update =
      [](struct_type& s) {
          get<list_type>(s.fields[0]).element_field->type = string_type{};
      },
    .validate_err = schema_evolution_errc::type_mismatch,
  },
  struct_evolution_test_case{
    .description
    = "introducing a required field to a list-element struct is not allowed",
    .generator =
      [](unique_id_generator& ids) {
          struct_type s{};
          s.fields.emplace_back(
            nested_field::create(
              ids.get_one(),
              "qux",
              field_required::yes,
              list_type::create(
                ids.get_one(), field_required::yes, struct_type{})));
          return s;
      },
    .update =
      [](struct_type& s) {
          get<struct_type>(get<list_type>(s.fields[0]).element_field)
            .fields.emplace_back(
              nested_field::create(0, "int", field_required::yes, int_type{}));
      },
    .validate_err = schema_evolution_errc::new_required_field,

  },
  struct_evolution_test_case{
    .description = "map values are subject to type promotion rules (invalid)",
    .generator =
      [](unique_id_generator& ids) {
          struct_type s{};
          s.fields.emplace_back(
            nested_field::create(
              ids.get_one(),
              "a_map",
              field_required::no,
              map_type::create(
                ids.get_one(),
                int_type{},
                ids.get_one(),
                field_required::no,
                float_type{})));
          return s;
      },
    .update =
      [](struct_type& s) {
          get<map_type>(s.fields[0]).value_field->type = string_type{};
      },
    .validate_err = schema_evolution_errc::type_mismatch,
  },
  struct_evolution_test_case{
    .description = "map keys are subject to type promotion rules (invalid)",
    .generator =
      [](unique_id_generator& ids) {
          struct_type s{};
          s.fields.emplace_back(
            nested_field::create(
              ids.get_one(),
              "a_map",
              field_required::no,
              map_type::create(
                ids.get_one(),
                int_type{},
                ids.get_one(),
                field_required::no,
                float_type{})));
          return s;
      },
    .update =
      [](struct_type& s) {
          get<map_type>(s.fields[0]).key_field->type = double_type{};
      },
    .validate_err = schema_evolution_errc::type_mismatch,
  },
  struct_evolution_test_case{
    .description = "evolving a primitive field into a struct is illegal",
    .generator =
      [](unique_id_generator& ids) {
          struct_type s{};
          s.fields.emplace_back(
            nested_field::create(
              ids.get_one(), "foo", field_required::no, int_type{}));
          return s;
      },
    .update =
      [](struct_type& s) {
          struct_type foo{};
          foo.fields.emplace_back(std::move(s.fields.back()));
          s.fields.clear();
          // note that the top-level name of the struct (that now contains
          // 'foo') is also 'foo'
          s.fields.emplace_back(
            nested_field::create(0, "foo", field_required::no, std::move(foo)));
      },
    .annotate_err = schema_evolution_errc::incompatible,
  },
  struct_evolution_test_case{
    .description = "evolving a single field struct into a primitive is illegal",
    .generator =
      [](unique_id_generator& ids) {
          struct_type s{};
          struct_type foo{};
          foo.fields.emplace_back(
            nested_field::create(
              ids.get_one(), "bar", field_required::no, int_type{}));
          s.fields.emplace_back(
            nested_field::create(
              ids.get_one(), "foo", field_required::no, std::move(foo)));
          return s;
      },
    .update =
      [](struct_type& s) {
          auto bar = std::move(get<struct_type>(s.fields.back()).fields.back());
          bar->name = "foo";
          s.fields.clear();
          s.fields.emplace_back(std::move(bar));
      },
    .annotate_err = schema_evolution_errc::incompatible,
  },
  struct_evolution_test_case{
    .description
    = "ambiguous (name-wise) type promotions are logged as such. even though"
      "we made a valid promotion of the first field, there's no way to"
      "distinguish between the two if the change came through SR",
    .generator =
      [](unique_id_generator& ids) {
          struct_type s{};
          s.fields.emplace_back(
            nested_field::create(
              ids.get_one(), "foo", field_required::no, int_type{}));
          s.fields.emplace_back(
            nested_field::create(
              ids.get_one(), "foo", field_required::no, float_type{}));
          return s;
      },
    .update = [](struct_type& s) { s.fields.back()->type = long_type{}; },
    .annotate_err = schema_evolution_errc::ambiguous,
  },
  struct_evolution_test_case{
    .description = "adding fields to a map key is illegal",
    .generator = [](unique_id_generator&) { return nested_test_struct(); },
    .update =
      [](struct_type& s) {
          auto& map = get<map_type>(s.fields[0]);
          auto& key = get<struct_type>(map.key_field);
          key.fields.emplace_back(
            nested_field::create(0, "qux", field_required::no, int_type{}));
      },
    .annotate_err = schema_evolution_errc::violates_map_key_invariant,
  },
  struct_evolution_test_case{
    .description = "dropping fields from a map key struct is illegal",
    .generator = [](unique_id_generator&) { return nested_test_struct(); },
    .update =
      [](struct_type& s) {
          auto& map = get<map_type>(s.fields[0]);
          auto& key = get<struct_type>(map.key_field);
          key.fields.pop_back();
      },
    .annotate_err = schema_evolution_errc::violates_map_key_invariant,
  },
  struct_evolution_test_case{
    .description
    = "promoting a field from optional to required is strictly illegal"
      "even if the field type promotion would be allowed",
    .generator =
      [](unique_id_generator& ids) {
          struct_type s{};
          s.fields.emplace_back(
            nested_field::create(
              ids.get_one(), "foo", field_required::no, int_type{}));
          return s;
      },
    .update =
      [](struct_type& s) {
          s.fields.back()->required = field_required::yes;
          s.fields.back()->type = long_type{};
      },
    .validate_err = schema_evolution_errc::new_required_field,
  },
  struct_evolution_test_case{
    .description = "adding required fields is illegal (NOTE: the spec allows "
                   "this but schema registry does not. we may introduce "
                   "support in the future.)",
    .generator = [](unique_id_generator&) { return struct_type{}; },
    .update =
      [](struct_type& s) {
          s.fields.emplace_back(
            nested_field::create(0, "foo", field_required::yes, int_type{}));
      },
    .validate_err = schema_evolution_errc::new_required_field,
  },
  struct_evolution_test_case{
    .description
    = "promoting from date -> timestamp is is illegal in Iceberg v2. it is "
      "allowed in v3, unless the field doesn't appears in the partition  spec",
    .generator =
      [](unique_id_generator& ids) {
          struct_type s{};
          struct_type n{};
          n.fields.emplace_back(
            nested_field::create(
              ids.get_one(), "date", field_required::no, date_type{}));
          s.fields.emplace_back(
            nested_field::create(
              ids.get_one(), "nested", field_required::no, std::move(n)));
          return s;
      },
    .update =
      [](struct_type& s) {
          std::get<struct_type>(s.fields.back()->type).fields.back()->type
            = timestamp_type{};
      },
    .validate_err = schema_evolution_errc::type_mismatch,
    .pspec = "(nested.date)",
  },
  struct_evolution_test_case{
    .description
    = "dropping a field which appears in the partition spec is illegal",
    .generator =
      [](unique_id_generator& ids) {
          struct_type s{};
          s.fields.emplace_back(
            nested_field::create(
              ids.get_one(), "foo", field_required::yes, int_type{}));
          return s;
      },
    .update = [](struct_type& s) { s.fields.pop_back(); },
    .validate_err = schema_evolution_errc::partition_spec_conflict,
    .pspec = "(foo)",
  },
  struct_evolution_test_case{
    .description
    = "dropping the enclosing struct for a partition field also fails",
    .generator =
      [](unique_id_generator& ids) {
          struct_type s{};
          struct_type nested{};
          nested.fields.emplace_back(
            nested_field::create(
              ids.get_one(), "foo", field_required::yes, int_type{}));
          s.fields.emplace_back(
            nested_field::create(
              ids.get_one(), "nested", field_required::yes, std::move(nested)));
          return s;
      },
    .update = [](struct_type& s) { s.fields.pop_back(); },
    .validate_err = schema_evolution_errc::partition_spec_conflict,
    .pspec = "(nested.foo)",
  },
};

static constexpr auto valid_plus_errs = [](auto&& R) {
    std::vector<struct_evolution_test_case> result;
    result.reserve(valid_cases.size() + invalid_cases.size());
    std::ranges::copy(valid_cases, std::back_inserter(result));
    std::ranges::copy(R, std::back_inserter(result));
    return result;
};

class StructCompatibilityTestBase
  : public CompatibilityTest<struct_evolution_test_case> {
protected:
    unique_id_generator ids;

public:
    auto generator() { return GetParam().generator(ids); }
    auto update(const struct_type& s) {
        auto cp = s.copy();
        GetParam().update(cp);
        reset_field_ids(cp);
        return cp;
    }
    auto& err() {
        return GetParam().annotate_err.has_error() ? GetParam().annotate_err
                                                   : GetParam().validate_err;
    }
    auto& annotate_err() { return GetParam().annotate_err; }
    auto& validate_err() { return GetParam().validate_err; }
    auto validator(const struct_type& src, const struct_type& dest) {
        return GetParam().validator(src, dest);
    }
    partition_spec gen_partition_spec(const struct_type& s) const {
        auto raw = GetParam().pspec;
        if (!raw.has_value()) {
            return partition_spec{};
        }
        auto parsed = datalake::parse_partition_spec(raw.value());
        EXPECT_TRUE(parsed.has_value()) << GetParam();
        auto resolved = partition_spec::resolve(parsed.value(), s);
        EXPECT_TRUE(resolved.has_value()) << GetParam();
        return std::move(resolved).value();
    }
    auto& any_change() { return GetParam().any_change; }
};

struct AnnotateStructTest : public StructCompatibilityTestBase {};

INSTANTIATE_TEST_SUITE_P(
  StructEvolutionTest,
  AnnotateStructTest,
  ::testing::ValuesIn(
    valid_plus_errs(invalid_cases | std::views::filter([](const auto& tc) {
                        return tc.annotate_err.has_error();
                    }))));

TEST_P(AnnotateStructTest, AnnotationWorksAndDetectsStructuralErrors) {
    // generate a schema per the test case
    auto original_schema_struct = generator();

    // manually update a copy of the schema in some way, also specified by the
    // test case
    auto type = update(original_schema_struct);

    {
        // transforming self -> self returns no change
        auto c1 = type.copy();
        auto c2 = type.copy();
        auto annotate_res = annotate_schema_transform(c1, c2, partition_spec{});
        if (
          !annotate_err().has_error()
          || annotate_err().error() != schema_evolution_errc::ambiguous) {
            ASSERT_FALSE(annotate_res.has_error());
            EXPECT_EQ(annotate_res.value().total(), 0);
        }
    }

    // check that annotation works or errors as expected
    auto annotate_res = annotate_schema_transform(
      original_schema_struct, type, gen_partition_spec(original_schema_struct));

    ASSERT_EQ(annotate_res.has_error(), annotate_err().has_error())
      << (annotate_res.has_error()
            ? fmt::format("Unexpected error: {}", annotate_res.error())
            : fmt::format("Expected {}", annotate_err().error()));

    if (annotate_res.has_error()) {
        EXPECT_EQ(annotate_res.error(), annotate_err().error());
        return;
    }
    // if no annotation errors, check that every field in the destination
    // type was marked
    auto res = for_each_field(type, [](const nested_field* f) {
        ASSERT_TRUE(f->has_evolution_metadata());
        EXPECT_TRUE(
          std::holds_alternative<nested_field::src_info>(f->meta)
          || std::holds_alternative<nested_field::is_new>(f->meta))
          << fmt::format("Unexpected meta variant index: {}", f->meta.index());
    });
    EXPECT_FALSE(res.has_error());

    // and that every field in the source struct was marked
    // note that source fields are marked removed::yes or removed::no to
    // indicate removal.
    res = for_each_field(original_schema_struct, [](const nested_field* f) {
        ASSERT_TRUE(f->has_evolution_metadata());
        EXPECT_TRUE(std::holds_alternative<nested_field::removed>(f->meta))
          << fmt::format("Unexpected meta variant index: {}", f->meta.index());
    });
    EXPECT_FALSE(res.has_error());
}

struct ValidateAnnotationTest : public StructCompatibilityTestBase {};

INSTANTIATE_TEST_SUITE_P(
  StructEvolutionTest,
  ValidateAnnotationTest,
  ::testing::ValuesIn(
    valid_plus_errs(invalid_cases | std::views::filter([](const auto& tc) {
                        return tc.validate_err.has_error();
                    }))));

TEST_P(ValidateAnnotationTest, ValidateCatchesTypeErrors) {
    // generate a schema per the test case
    auto original_schema_struct = generator();

    // manually update a copy of the schema in some way, also specified by the
    // test case
    auto type = update(original_schema_struct);

    {
        // transforming self -> self returns no change
        auto c1 = original_schema_struct.copy();
        auto c2 = original_schema_struct.copy();
        auto annotate_res = annotate_schema_transform(
          c1, c2, gen_partition_spec(c1));
        ASSERT_FALSE(annotate_res.has_error());
        auto validate_res = validate_schema_transform(
          annotate_res, c2, gen_partition_spec(c1));
        ASSERT_FALSE(validate_res.has_error())
          << fmt::format("Unexpected error: {}", validate_res.error());
        EXPECT_EQ(validate_res.value().total(), 0);
    }

    // For this subset of cases we expect annotate to pass
    auto annotate_res = annotate_schema_transform(
      original_schema_struct, type, gen_partition_spec(original_schema_struct));
    ASSERT_FALSE(annotate_res.has_error());
    if (annotate_res.value().n_removed_partition_fields > 0) {
        ASSERT_TRUE(validate_err().has_error());
        EXPECT_EQ(
          validate_err().error(),
          schema_evolution_errc::partition_spec_conflict);
    }

    // but validate may fail
    auto validate_res = validate_schema_transform(
      annotate_res, type, gen_partition_spec(original_schema_struct));
    ASSERT_EQ(validate_res.has_error(), validate_err().has_error())
      << (validate_res.has_error()
            ? fmt::format("Unexpected error: {}", validate_res.error())
            : fmt::format("Expected {}", validate_err().error()));

    if (validate_res.has_error()) {
        EXPECT_EQ(validate_res.error(), validate_err().error());
        return;
    }

    // If validate passed, every field in the destination struct should either
    // have a nonzero ID assigned OR be marked as new.
    auto res = for_each_field(type, [](const nested_field* f) {
        ASSERT_TRUE(f->has_evolution_metadata());
        EXPECT_TRUE(
          f->id() > 0 || std::holds_alternative<nested_field::is_new>(f->meta));
    });

    EXPECT_FALSE(res.has_error());
}

struct StructEvoCompatibilityTest : public StructCompatibilityTestBase {};

INSTANTIATE_TEST_SUITE_P(
  StructEvolutionTest,
  StructEvoCompatibilityTest,
  ::testing::ValuesIn(valid_plus_errs(invalid_cases)));

TEST_P(StructEvoCompatibilityTest, CanEvolveStructsAndDetectErrors) {
    // generate a schema per the test case
    auto original_schema_struct = generator();

    // manually update a copy of the schema in some way, also specified by the
    // test case
    auto type = update(original_schema_struct);

    // try to evolve the original schema into the new and update the latter
    // accordingly. check against expectations (both success and expected
    // qualities of the result)
    auto evolve_res = evolve_schema(
      original_schema_struct, type, gen_partition_spec(original_schema_struct));

    ASSERT_EQ(evolve_res.has_error(), err().has_error())
      << (evolve_res.has_error()
            ? fmt::format("Unexpected error: {}", evolve_res.error())
            : fmt::format(
                "Expected {} got {}", err().error(), evolve_res.value()));
    if (evolve_res.has_error()) {
        ASSERT_EQ(evolve_res.error(), err().error())
          << fmt::format("{}", evolve_res.error());
        return;
    }

    // check expected value for whether the schema changed
    ASSERT_EQ(evolve_res.value(), any_change());

    // Full validation step for struct evolution result
    ASSERT_TRUE(validator(original_schema_struct, type)) << fmt::format(
      "Original: {}\nEvolved: {}", original_schema_struct, evolve_res.value());
}

TEST_P(StructEvoCompatibilityTest, CanCheckEquivalence) {
    auto original = generator();

    EXPECT_TRUE(schemas_equivalent(original, original));

    auto next = update(original);

    EXPECT_FALSE(schemas_equivalent(original, next));
    EXPECT_FALSE(schemas_equivalent(next, original));
}

TEST(ValuePromotionTest, PrimitiveValuePromotion) {
    // trivial promotions should leave the value intact
    std::vector<std::pair<primitive_value, primitive_type>> trivial;
    trivial.emplace_back(boolean_value{true}, boolean_type{});
    trivial.emplace_back(int_value{42}, int_type{});
    trivial.emplace_back(long_value{42}, long_type{});
    trivial.emplace_back(float_value{1.5}, float_type{});
    trivial.emplace_back(double_value{1.5}, double_type{});
    trivial.emplace_back(
      decimal_value{42}, decimal_type{.precision = 10, .scale = 2});
    trivial.emplace_back(date_value{123}, date_type{});
    trivial.emplace_back(time_value{1234}, time_type{});
    trivial.emplace_back(timestamp_value{31536000000023ul}, timestamp_type{});
    trivial.emplace_back(
      timestamptz_value{31536000000023ul}, timestamptz_type{});
    trivial.emplace_back(
      string_value{bytes_to_iobuf(bytes::from_string("foobar"))},
      string_type{});
    trivial.emplace_back(uuid_value{uuid_t::create()}, uuid_type{});
    trivial.emplace_back(
      fixed_value{bytes_to_iobuf(bytes{1, 2, 3, 4, 5, 6, 7, 8, 255})},
      fixed_type{.length = 9});
    trivial.emplace_back(
      binary_value{bytes_to_iobuf(bytes{1, 2, 3, 4, 5, 6, 7, 8, 255})},
      binary_type{});

    for (const auto& [val, type] : trivial) {
        ASSERT_EQ(promote_primitive_value_type(make_copy(val), type), val);
    }

    // non-trivial promotions
    ASSERT_EQ(
      promote_primitive_value_type(int_value{42}, long_type{}), long_value{42});
    ASSERT_EQ(
      promote_primitive_value_type(float_value{1.5}, double_type{}),
      double_value{1.5});

    // test some forbidden promotions
    std::vector<std::pair<primitive_value, primitive_type>> forbidden;
    forbidden.emplace_back(boolean_value{true}, int_type{});
    forbidden.emplace_back(int_value{42}, double_type{});
    forbidden.emplace_back(long_value{42}, int_type{});
    forbidden.emplace_back(float_value{1.5}, int_type{});
    forbidden.emplace_back(double_value{1.5}, float_type{});
    forbidden.emplace_back(date_value{123}, timestamptz_type{});
    forbidden.emplace_back(time_value{1234}, timestamptz_type{});
    forbidden.emplace_back(decimal_value{42}, int_type{});
    forbidden.emplace_back(
      timestamp_value{31536000000023ul}, timestamptz_type{});
    forbidden.emplace_back(
      timestamptz_value{31536000000023ul}, timestamp_type{});
    forbidden.emplace_back(
      string_value{bytes_to_iobuf(bytes::from_string("foobar"))},
      binary_type{});
    forbidden.emplace_back(uuid_value{uuid_t::create()}, binary_type{});
    forbidden.emplace_back(
      fixed_value{bytes_to_iobuf(bytes{1, 2, 3, 4, 5, 6, 7, 8, 255})},
      binary_type{});
    forbidden.emplace_back(
      binary_value{bytes_to_iobuf(bytes::from_string("foobar"))},
      string_type{});

    for (const auto& [val, type] : forbidden) {
        ASSERT_THROW(
          promote_primitive_value_type(make_copy(val), type), std::logic_error)
          << "value: " << val << ", type: " << type;
    }
}

namespace {

struct fill_ids_test_case {
    std::string_view description{};
    ss::lw_shared_ptr<struct_type> source;
    ss::lw_shared_ptr<struct_type> dest;
    ids_filled expected_result;
};

std::ostream& operator<<(std::ostream& os, const fill_ids_test_case& tc) {
    return os << tc.description;
}

std::vector<fill_ids_test_case> generate_fill_ids_test_cases() {
    std::vector<fill_ids_test_case> test_cases;

    auto struct_template = [] {
        auto s = ss::make_lw_shared<struct_type>();
        s->fields.emplace_back(
          nested_field::create(
            nested_field::id_t{1}, "foo", field_required::yes, int_type{}));
        s->fields.emplace_back(
          nested_field::create(
            nested_field::id_t{2}, "bar", field_required::no, string_type{}));
        return s;
    };

    test_cases.emplace_back(
      fill_ids_test_case{
        .description = "equivalent schemas should succeed",
        .source = struct_template(),
        .dest = struct_template(),
        .expected_result = ids_filled::yes,
      });

    test_cases.emplace_back(
      fill_ids_test_case{
        .description = "equivalent schemas should succeed (nested)",
        .source = [] { return ss::make_lw_shared(nested_test_struct()); }(),
        .dest = [] { return ss::make_lw_shared(nested_test_struct()); }(),
        .expected_result = ids_filled::yes,
      });

    test_cases.emplace_back(
      fill_ids_test_case{
        .description = "destination missing field should succeed",
        .source = struct_template(),
        .dest =
          [&]() {
              auto s = struct_template();
              s->fields.pop_back();
              return s;
          }(),
        .expected_result = ids_filled::yes,
      });

    test_cases.emplace_back(
      fill_ids_test_case{
        .description = "destination with extra field should fail",
        .source = struct_template(),
        .dest =
          [&]() {
              auto s = struct_template();
              s->fields.emplace_back(
                nested_field::create(
                  nested_field::id_t{0},
                  "extra",
                  field_required::no,
                  string_type{}));
              return s;
          }(),
        .expected_result = ids_filled::no,
      });

    test_cases.emplace_back(
      fill_ids_test_case{
        .description
        = "dest long type, source int type should fail (requires promotion)",
        .source =
          []() {
              auto s = ss::make_lw_shared<struct_type>();
              s->fields.emplace_back(
                nested_field::create(
                  nested_field::id_t{1},
                  "foo",
                  field_required::yes,
                  int_type{}));
              return s;
          }(),
        .dest =
          []() {
              auto s = ss::make_lw_shared<struct_type>();
              s->fields.emplace_back(
                nested_field::create(
                  nested_field::id_t{0},
                  "foo",
                  field_required::yes,
                  long_type{}));
              return s;
          }(),
        .expected_result = ids_filled::no,
      });

    test_cases.emplace_back(
      fill_ids_test_case{
        .description = "dest int type, source long type should succeed "
                       "(allowed under promotion rules)",
        .source =
          []() {
              auto s = ss::make_lw_shared<struct_type>();
              s->fields.emplace_back(
                nested_field::create(
                  nested_field::id_t{1},
                  "foo",
                  field_required::yes,
                  long_type{}));
              return s;
          }(),
        .dest =
          []() {
              auto s = ss::make_lw_shared<struct_type>();
              s->fields.emplace_back(
                nested_field::create(
                  nested_field::id_t{0},
                  "foo",
                  field_required::yes,
                  int_type{}));
              return s;
          }(),
        .expected_result = ids_filled::yes,
      });

    for (auto& tc : test_cases) {
        reset_field_ids(*tc.dest);
    }

    return test_cases;
}

} // namespace

template<typename T>
struct FillIdsCompatibilityTest
  : ::testing::Test
  , testing::WithParamInterface<T> {};

using FillIdsTest = FillIdsCompatibilityTest<fill_ids_test_case>;

INSTANTIATE_TEST_SUITE_P(
  FillIdsCompatibilityTest,
  FillIdsTest,
  ::testing::ValuesIn(generate_fill_ids_test_cases()));

TEST_P(FillIdsTest, TryFillFieldIds) {
    const auto& tc = GetParam();

    // Make copies since try_fill_field_ids modifies the dest schema
    auto source = tc.source->copy();
    auto dest = tc.dest->copy();
    auto result = try_fill_field_ids(source, dest);

    ASSERT_EQ(result, tc.expected_result);

    // If successful and ids_filled::yes, verify that all dest fields have
    // non-zero IDs
    if (result == ids_filled::yes) {
        bool all_have_ids = true;
        std::ignore = for_each_field(
          dest, [&all_have_ids](const nested_field* f) {
              if (f->id == nested_field::id_t{0}) {
                  all_have_ids = false;
              }
          });
        ASSERT_TRUE(all_have_ids)
          << "All destination fields should have assigned IDs";
    }
}

namespace {

struct merge_test_case {
    std::string description;

    ss::lw_shared_ptr<struct_type> source;
    ss::lw_shared_ptr<struct_type> dest;

    checked<void, schema_evolution_errc> expected_result = outcome::success();
    ss::lw_shared_ptr<struct_type> expected_dest;
};

std::vector<merge_test_case> generate_merge_test_cases() {
    std::vector<merge_test_case> test_cases;

    test_cases.push_back(
      merge_test_case{
        .description = "merging two empty structs",
        .source = ss::make_lw_shared<struct_type>(),
        .dest = ss::make_lw_shared<struct_type>(),
        .expected_dest = ss::make_lw_shared<struct_type>(),
      });

    {
        auto nested_struct = nested_test_struct();
        test_cases.push_back(
          merge_test_case{
            .description = "merging two complex but equal structs",
            .source = ss::make_lw_shared<struct_type>(nested_struct.copy()),
            .dest = ss::make_lw_shared<struct_type>(nested_struct.copy()),
            .expected_dest = ss::make_lw_shared<struct_type>(
              nested_struct.copy()),
          });
    }

    {
        auto src_struct = ss::make_lw_shared<struct_type>();
        src_struct->fields.emplace_back(
          nested_field::create(0, "foo", field_required::no, int_type{}));
        src_struct->fields.emplace_back(
          nested_field::create(1, "bar", field_required::no, int_type{}));

        auto dst_struct = ss::make_lw_shared<struct_type>();
        dst_struct->fields.emplace_back(
          nested_field::create(0, "bar", field_required::no, int_type{}));
        dst_struct->fields.emplace_back(
          nested_field::create(1, "baz", field_required::no, int_type{}));

        auto result_struct = ss::make_lw_shared<struct_type>();
        result_struct->fields.emplace_back(
          nested_field::create(0, "bar", field_required::no, int_type{}));
        result_struct->fields.emplace_back(
          nested_field::create(1, "baz", field_required::no, int_type{}));
        result_struct->fields.emplace_back(
          nested_field::create(2, "foo", field_required::no, int_type{}));

        test_cases.push_back(
          merge_test_case{
            .description = "merging two structs with overlapping fields",
            .source = src_struct,
            .dest = dst_struct,
            .expected_dest = result_struct,
          });
    }

    {
        auto struct_a = ss::make_lw_shared<struct_type>();
        struct_a->fields.emplace_back(
          nested_field::create(0, "foo", field_required::no, int_type{}));

        auto struct_b = ss::make_lw_shared<struct_type>();
        struct_b->fields.emplace_back(
          nested_field::create(0, "foo", field_required::no, long_type{}));

        test_cases.push_back(
          merge_test_case{
            .description = "wider primitive takes precedence",
            .source = struct_a,
            .dest = struct_b,
            .expected_dest = ss::make_lw_shared<struct_type>(struct_b->copy()),
          });

        test_cases.push_back(
          merge_test_case{
            .description = "wider primitive takes precedence",
            .source = struct_b,
            .dest = struct_a,
            .expected_dest = ss::make_lw_shared<struct_type>(struct_b->copy()),
          });
    }

    {
        auto struct_a = ss::make_lw_shared<struct_type>(
          nested_test_struct().copy());
        auto struct_b = ss::make_lw_shared<struct_type>(struct_a->copy());
        auto& map = get<map_type>(struct_b->fields[0]);
        auto& key = get<struct_type>(map.key_field);
        key.fields.emplace_back(
          nested_field::create(0, "qux", field_required::no, int_type{}));

        test_cases.push_back(
          merge_test_case{
            .description = "change map key",
            .source = struct_a,
            .dest = struct_b,
            .expected_result
            = {schema_evolution_errc::violates_map_key_invariant},
          });

        test_cases.push_back(
          merge_test_case{
            .description = "change map key",
            .source = struct_b,
            .dest = struct_a,
            .expected_result
            = {schema_evolution_errc::violates_map_key_invariant},
          });
    }

    {
        auto struct_a = ss::make_lw_shared<struct_type>(
          nested_test_struct().copy());
        auto struct_b = ss::make_lw_shared<struct_type>(struct_a->copy());
        auto& map = get<map_type>(struct_b->fields[0]);
        auto& value = get<map_type>(map.value_field);
        auto& nested_map_value = get<struct_type>(value.value_field);
        nested_map_value.fields.emplace_back(
          nested_field::create(0, "quux", field_required::no, int_type{}));

        test_cases.push_back(
          merge_test_case{
            .description = "change map value",
            .source = struct_a,
            .dest = struct_b,
            .expected_dest = ss::make_lw_shared<struct_type>(struct_b->copy()),
          });

        test_cases.push_back(
          merge_test_case{
            .description = "change map value",
            .source = struct_b,
            .dest = struct_a,
            .expected_dest = ss::make_lw_shared<struct_type>(struct_b->copy()),
          });
    }

    {
        auto struct_a = ss::make_lw_shared<struct_type>(
          nested_test_struct().copy());
        auto struct_b = ss::make_lw_shared<struct_type>(struct_a->copy());
        struct_a->fields[0]->required = field_required::no;

        test_cases.push_back(
          merge_test_case{
            .description = "change nullability",
            .source = struct_a,
            .dest = struct_b,
            .expected_dest = ss::make_lw_shared<struct_type>(struct_a->copy()),
          });

        test_cases.push_back(
          merge_test_case{
            .description = "change nullability",
            .source = struct_b,
            .dest = struct_a,
            .expected_dest = ss::make_lw_shared<struct_type>(struct_a->copy()),
          });
    }

    return test_cases;
}

struct MergeTest : public CompatibilityTest<merge_test_case> {};

} // namespace

INSTANTIATE_TEST_SUITE_P(
  MergeTest, MergeTest, ::testing::ValuesIn(generate_merge_test_cases()));

TEST_P(MergeTest, CompatibleTypesAreCompatible) {
    const auto& p = GetParam();

    auto dest = p.dest->copy();

    auto res = merge_struct_types(*p.source, dest);
    ASSERT_EQ(res.has_error(), p.expected_result.has_error())
      << (res.has_error()
            ? fmt::format("Unexpected error: {}", res.error())
            : fmt::format("Expected {}", p.expected_result.error()));

    if (res.has_error()) {
        ASSERT_EQ(res.error(), p.expected_result.error());
    }

    ASSERT_FALSE(for_each_field(dest, [i = 0](nested_field* f) mutable {
                     f->id = nested_field::id_t{i++};
                     f->meta = std::nullopt;
                 }).has_error());

    ASSERT_FALSE(
      p.expected_dest != nullptr
      && for_each_field(*p.expected_dest, [i = 0](nested_field* f) mutable {
             f->id = nested_field::id_t{i++};
             f->meta = std::nullopt;
         }).has_error());

    if (p.expected_result.has_value() && res.has_value()) {
        ASSERT_TRUE(structs_equivalent(dest, *p.expected_dest))
          << fmt::format("Expected: {}\nGot: {}", *p.expected_dest, dest);
    }
}
