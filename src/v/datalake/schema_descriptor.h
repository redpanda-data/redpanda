/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "iceberg/datatypes.h"
#include "iceberg/values.h"

#include <seastar/core/sstring.hh>

#include <array>
#include <cstddef>
#include <optional>
#include <string_view>
#include <type_traits>
#include <utility>

namespace datalake {

/// A compile-time string usable as a template parameter (C++20 NTTP).
template<size_t N>
struct ct_string {
    char data[N]{};
    constexpr ct_string(const char (&s)[N]) { std::copy_n(s, N, data); }
    constexpr operator std::string_view() const { return {data, N - 1}; }
    constexpr auto operator<=>(const ct_string&) const = default;
};

/// A field descriptor: compile-time name + iceberg type.
template<ct_string Name, typename T>
struct field_desc {
    static constexpr std::string_view name{Name};
    using type = T;
};

/// Forward declarations.
template<typename... Fields>
struct struct_desc;

template<typename ElementDesc>
struct list_desc;

/// Find the index of a field by name. Fails to compile if not found.
template<ct_string Name, typename... Fields>
consteval size_t index_of_fn() {
    constexpr std::array names = {Fields::name...};
    for (size_t i = 0; i < names.size(); ++i) {
        if (names[i] == std::string_view{Name}) {
            return i;
        }
    }
    // If we reach here, the name was not found. This is consteval,
    // so the compiler will reject it with an error pointing here.
    throw "field name not found in descriptor";
}

/// Check that given names match field names in order.
/// Uses an array of field names passed directly to avoid partial
/// specialization.
template<ct_string... Names>
consteval bool
names_match_fn(std::array<std::string_view, sizeof...(Names)> field_names) {
    constexpr std::array given = {std::string_view{Names}...};
    for (size_t i = 0; i < field_names.size(); ++i) {
        if (field_names[i] != given[i]) {
            return false;
        }
    }
    return true;
}

/// A named value for use with struct_desc::build_value. The name is
/// checked at compile time to match the corresponding field descriptor.
template<ct_string Name>
struct val {
    std::optional<iceberg::value> v;

    template<typename U>
    requires std::constructible_from<std::optional<iceberg::value>, U>
    val(U&& u) // NOLINT(google-explicit-constructor)
      : v(std::forward<U>(u)) {}
};

template<typename... Fields>
struct struct_desc {
    static constexpr size_t count = sizeof...(Fields);

    /// Compile-time index of a field by name.
    template<ct_string Name>
    static constexpr size_t index_of = index_of_fn<Name, Fields...>();

    /// Total number of fields in the tree (used for ID assignment).
    static int total_fields() {
        int next_id = 0;
        build_impl(next_id);
        return next_id;
    }

    /// Build the runtime iceberg::struct_type.
    static iceberg::struct_type build() {
        int next_id = 0;
        return build_impl(next_id);
    }

private:
    template<typename...>
    friend struct struct_desc;
    template<typename>
    friend struct list_desc;

    static iceberg::struct_type build_impl(int& next_id) {
        iceberg::struct_type st;
        auto add = [&]<typename F>() {
            int my_id = next_id++;
            st.fields.push_back(
              iceberg::nested_field::create(
                my_id,
                ss::sstring{F::name},
                iceberg::field_required::no,
                build_iceberg_type<F>(next_id)));
        };
        (add.template operator()<Fields>(), ...);
        return st;
    }

public:
    /// Build a struct_value with compile-time name and arity checking.
    /// Each argument is a val<"field_name"> matching the descriptor.
    ///
    /// Wrong arity, wrong name, or wrong order = compile error.
    template<ct_string... Names>
    static std::unique_ptr<iceberg::struct_value>
    build_value(val<Names>... args) {
        static_assert(sizeof...(Names) == count, "wrong number of fields");
        static_assert(
          names_match_fn<Names...>({Fields::name...}),
          "field names don't match descriptor or are in wrong order");
        auto sv = std::make_unique<iceberg::struct_value>();
        sv->fields.reserve(count);
        (sv->fields.emplace_back(std::move(args.v)), ...);
        return sv;
    }

private:
    template<typename T>
    static constexpr bool is_nested_v = false;
    template<typename... Fs>
    static constexpr bool is_nested_v<struct_desc<Fs...>> = true;
    template<typename E>
    static constexpr bool is_nested_v<list_desc<E>> = true;

    template<typename F>
    static auto build_iceberg_type(int& next_id) {
        if constexpr (is_nested_v<typename F::type>) {
            return F::type::build_impl(next_id);
        } else {
            return typename F::type{};
        }
    }
};

template<typename ElementDesc>
struct list_desc;

template<typename... Fields>
struct list_desc<struct_desc<Fields...>> {
    static iceberg::list_type build_impl(int& next_id) {
        int element_id = next_id++;
        return iceberg::list_type::create(
          element_id,
          iceberg::field_required::no,
          struct_desc<Fields...>::build_impl(next_id));
    }
};

template<typename Desc, ct_string Name>
iceberg::nested_field& type_field(iceberg::struct_type& st) {
    return *st.fields[Desc::template index_of<Name>];
}

template<typename Desc, ct_string Name>
const iceberg::nested_field& type_field(const iceberg::struct_type& st) {
    return *st.fields[Desc::template index_of<Name>];
}

template<typename Desc, ct_string Name>
std::optional<iceberg::value>& value_field(iceberg::struct_value& sv) {
    return sv.fields[Desc::template index_of<Name>];
}

template<typename Desc, ct_string Name>
const std::optional<iceberg::value>&
value_field(const iceberg::struct_value& sv) {
    return sv.fields[Desc::template index_of<Name>];
}

} // namespace datalake
