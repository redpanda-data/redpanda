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
#pragma once

#include "container/chunked_vector.h"
#include "security/acl.h"
#include "security/config.h"
#include "security/jwt.h"
#include "security/oidc_principal_mapping.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/variant_utils.hh>

#include <fmt/format.h>

#include <cstddef>
#include <iterator>
#include <optional>
#include <string_view>
#include <variant>

namespace security {

/// \brief Lazy range for ACL group principals
///
/// This class provides a lazy view over group principals that defers
/// parsing and transformation until iteration. This is critical for
/// authentication performance since groups are often not needed:
/// - Superusers skip group checks entirely
/// - Empty ACLs skip group checks
/// - Principal matches often short-circuit before groups
///
/// The range supports multiple source types:
/// - Comma-separated string (lazy split on iteration)
/// - Pre-split array of string_view (from JWT array claims)
/// - Empty (no groups)
///
/// Example usage:
/// \code
///   auto groups = group_range(jwt, group_claim_policy);
///   for (const auto& g : groups) {  // Only materializes on iteration
///       if (check_acl(g)) break;    // Can short-circuit early
///   }
/// \endcode
class group_range {
private:
    struct jwt_list_state {
        ss::lw_shared_ptr<const oidc::jwt> jwt;
        oidc::group_claim_policy policy;
        chunked_vector<std::string_view> list_claim;
    };

    struct jwt_string_state {
        ss::lw_shared_ptr<const oidc::jwt> jwt;
        oidc::group_claim_policy policy;
        std::string_view string_claim;
    };

    struct materialized_state {
        chunked_vector<security::acl_principal> materialized_groups;
    };

public:
    using value_type = security::acl_principal;

    // Default constructor creates an empty range with no allocation
    group_range() = default;

    group_range(
      ss::lw_shared_ptr<const oidc::jwt> jwt,
      const oidc::group_claim_policy& policy);

    explicit group_range(
      chunked_vector<security::acl_principal> materialized_groups);

    group_range(group_range&&) = default;
    group_range& operator=(group_range&&) = default;
    ~group_range() = default;

    group_range(const group_range&) = delete;
    group_range& operator=(const group_range&) = delete;

    /// \brief Explicitly copy this group_range
    [[nodiscard]] group_range copy() const;

    /// \brief Check if this group_range is empty (has no groups)
    [[nodiscard]] bool empty() const noexcept;

    /// \warning Iterator lifetime is tied to the group_range.
    /// Moving or destroying the group_range invalidates all iterators.
    struct iterator {
        using iterator_category = std::forward_iterator_tag;
        using value_type = security::acl_principal;
        using difference_type = std::ptrdiff_t;
        using reference = security::acl_principal;

        struct jwt_list_it_state {
            const jwt_list_state* jwt_list_state = nullptr;
            std::size_t index = 0;
        };

        struct jwt_string_it_state {
            const jwt_string_state* jwt_string_state = nullptr;
            // Invariant: remaining should not have leading or trailing commas
            std::string_view remaining;
            // Invariant: current should not have leading or trailing
            // whitespaces
            std::string_view current{};

            jwt_string_it_state(
              const group_range::jwt_string_state* state, std::string_view rem);

            explicit jwt_string_it_state(
              const group_range::jwt_string_state* state);

            void advance() noexcept;

            bool at_end() const noexcept;
        };

        struct materialized_it_state {
            const materialized_state* materialized_state = nullptr;
            std::size_t index = 0;
        };

        using type = std::variant<
          std::monostate,
          jwt_list_it_state,
          jwt_string_it_state,
          materialized_it_state>;

        type _it_state;

        reference operator*() const noexcept;
        iterator& operator++() noexcept;
        iterator operator++(int) noexcept;
        friend bool operator==(const iterator& a, const iterator& b) noexcept;
        friend bool operator!=(const iterator& a, const iterator& b) noexcept;
    };

    // Delete rvalue overloads to prevent dangerous patterns
    iterator begin() const&& = delete; // Prevents: group_range(...).begin()
    iterator end() const&& = delete;

    iterator begin() const& noexcept;
    iterator end() const& noexcept;

private:
    std::variant<
      std::monostate,
      jwt_list_state,
      jwt_string_state,
      materialized_state>
      _storage;
};

} // namespace security

// Formatter for group_range to enable logging
template<>
struct fmt::formatter<security::group_range> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

    template<typename FormatContext>
    auto format(const security::group_range& range, FormatContext& ctx) const {
        auto out = ctx.out();
        *out++ = '[';
        bool first = true;
        for (const auto& group : range) {
            if (!first) {
                *out++ = ',';
                *out++ = ' ';
            }
            first = false;
            out = fmt::format_to(out, "{}", group);
        }
        *out++ = ']';
        return out;
    }
};
