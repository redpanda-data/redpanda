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

#include "security/group_range.h"

#include "base/vassert.h"

namespace security {

namespace {
acl_principal apply_nested_group_policy(
  std::string_view user, oidc::nested_group_behavior b) {
    switch (b) {
    case oidc::nested_group_behavior::none:
        return acl_principal{principal_type::group, ss::sstring{user}};
    case oidc::nested_group_behavior::suffix: {
        auto pos = user.find_last_of('/');
        if (pos == std::string_view::npos) {
            return acl_principal{principal_type::group, ss::sstring{user}};
        }
        return acl_principal{
          principal_type::group, ss::sstring{user.substr(pos + 1)}};
    }
    }
}

void skip_leading(std::string_view& str, std::string_view skip_chars) {
    auto first = str.find_first_not_of(skip_chars);
    if (first == std::string_view::npos) {
        // Here, str is either all commas or empty
        str = {};
    } else {
        // first is the index of the first character we want to keep.
        str.remove_prefix(first);
    }
}

void skip_trailing(std::string_view& str, std::string_view skip_chars) {
    auto last = str.find_last_not_of(skip_chars);
    if (last == std::string_view::npos) {
        // Here, str is either all commas or empty
        str = {};
    } else {
        // last is the index of the last character we want to keep.
        str.remove_suffix(str.size() - last - 1);
    }
}
} // namespace

group_range::group_range(
  ss::lw_shared_ptr<const oidc::jwt> jwt,
  const oidc::group_claim_policy& policy) {
    const auto& p = policy.group_pointer();
    auto list_claim = jwt->claim<chunked_vector<std::string_view>>(p);
    if (list_claim) {
        _storage = jwt_list_state{
          .jwt = jwt,
          .policy = policy,
          .list_claim = std::move(list_claim.value())};
        return;
    }

    auto string_claim = jwt->claim<std::string_view>(p);
    if (string_claim) {
        _storage = jwt_string_state{
          .jwt = jwt, .policy = policy, .string_claim = string_claim.value()};
        return;
    }
}

group_range::group_range(
  chunked_vector<security::acl_principal> materialized_groups)
  : _storage{materialized_state{
      .materialized_groups = std::move(materialized_groups)}} {}

group_range group_range::copy() const {
    return ss::visit(
      _storage,
      [](const std::monostate&) -> group_range { return group_range{}; },
      [](const jwt_list_state& state) -> group_range {
          return {state.jwt, state.policy};
      },
      [](const jwt_string_state& state) -> group_range {
          return {state.jwt, state.policy};
      },
      [](const materialized_state& state) -> group_range {
          return group_range(state.materialized_groups.copy());
      });
}

bool group_range::empty() const noexcept {
    return ss::visit(
      _storage,
      [](const std::monostate&) -> bool { return true; },
      [](const jwt_list_state& state) -> bool {
          return state.list_claim.empty();
      },
      [](const jwt_string_state& state) -> bool {
          // TODO: Is this correct? What if the string is just whitespace?
          return state.string_claim.empty();
      },
      [](const materialized_state& state) -> bool {
          return state.materialized_groups.empty();
      });
}

group_range::iterator::jwt_string_it_state::jwt_string_it_state(
  const group_range::jwt_string_state* state, std::string_view rem)
  : jwt_string_state(state)
  , remaining(rem) {
    skip_leading(remaining, ",");
    skip_trailing(remaining, ",");
    // Prime the first token into current
    advance();
}

group_range::iterator::jwt_string_it_state::jwt_string_it_state(
  const group_range::jwt_string_state* state)
  : jwt_string_it_state(state, state->string_claim) {}

void group_range::iterator::jwt_string_it_state::advance() noexcept {
    current = {};

    while (!remaining.empty()) {
        auto next_comma = remaining.find(',');
        if (next_comma == std::string_view::npos) {
            current = remaining;
            remaining = {};
        } else {
            current = remaining.substr(0, next_comma);
            remaining.remove_prefix(next_comma + 1);
        }

        // Invariant: remaining has no leading/trailing commas
        skip_leading(remaining, ",");

        // Invariant: current has no leading/trailing whitespaces
        skip_leading(current, " \t\r\n");
        skip_trailing(current, " \t\r\n");
        // Skip empty/blank fields
        if (current.empty()) {
            // Loop continues, and remaining is smaller than before
            continue;
        } else {
            return; // Found a valid current token
        }
    }
}

bool group_range::iterator::jwt_string_it_state::at_end() const noexcept {
    return current.empty() && remaining.empty();
}

group_range::iterator::reference
group_range::iterator::operator*() const noexcept {
    return ss::visit(
      _it_state,
      [](const std::monostate&) -> security::acl_principal {
          // UB to dereference empty/end iterator
          vassert(false, "Dereferencing empyt/end group_range iterator");
          return {};
      },
      [](const jwt_list_it_state& it_state) -> security::acl_principal {
          const auto& list_state = *it_state.jwt_list_state;
          auto group = list_state.list_claim.at(it_state.index);
          skip_leading(group, " \t\r\n");
          skip_trailing(group, " \t\r\n");
          return apply_nested_group_policy(
            group, list_state.policy.nested_behavior());
      },
      [](const jwt_string_it_state& it_state) -> security::acl_principal {
          // In split mode, current is always the token for this position.
          // (If we're at end, deref is undefined like normal iterators.)
          const auto& string_state = *it_state.jwt_string_state;

          return apply_nested_group_policy(
            it_state.current, string_state.policy.nested_behavior());
      },
      [](const materialized_it_state& it_state) -> security::acl_principal {
          return it_state.materialized_state->materialized_groups.at(
            it_state.index);
      });
}

group_range::iterator& group_range::iterator::operator++() noexcept {
    ss::visit(
      _it_state,
      [](std::monostate&) noexcept {
          // UB to increment empty/end iterator - no-op
      },
      [](jwt_list_it_state& s) noexcept { ++s.index; },
      [](jwt_string_it_state& s) noexcept { s.advance(); },
      [](materialized_it_state& s) noexcept { ++s.index; });
    return *this;
}

group_range::iterator group_range::iterator::operator++(int) noexcept {
    auto tmp = *this;
    ++(*this);
    return tmp;
}

bool operator==(
  const group_range::iterator& a, const group_range::iterator& b) noexcept {
    return std::visit(
      [&](const auto& x, const auto& y) noexcept {
          using T1 = std::decay_t<decltype(x)>;
          using T2 = std::decay_t<decltype(y)>;

          if constexpr (!std::is_same_v<T1, T2>) {
              return false; // Different variant alternatives can't be equal
          } else if constexpr (std::is_same_v<T1, std::monostate>) {
              return true; // All empty iterators are equal
          } else if constexpr (std::is_same_v<
                                 T1,
                                 group_range::iterator::jwt_list_it_state>) {
              return x.jwt_list_state == y.jwt_list_state && x.index == y.index;
          } else if constexpr (std::is_same_v<
                                 T1,
                                 group_range::iterator::jwt_string_it_state>) {
              // If both at end, they're equal
              if (x.at_end() && y.at_end()) {
                  return true;
              }
              if (x.at_end() != y.at_end()) {
                  return false;
              }

              // Both not at end - compare remaining position
              return x.remaining.data() == y.remaining.data()
                     && x.remaining.size() == y.remaining.size()
                     && x.jwt_string_state->policy.nested_behavior()
                          == y.jwt_string_state->policy.nested_behavior();
          } else if constexpr (
            std::is_same_v<T1, group_range::iterator::materialized_it_state>) {
              return x.materialized_state == y.materialized_state
                     && x.index == y.index;
          }
      },
      a._it_state,
      b._it_state);
}

bool operator!=(
  const group_range::iterator& a, const group_range::iterator& b) noexcept {
    return !(a == b);
}

group_range::iterator group_range::begin() const& noexcept {
    return ss::visit(
      _storage,
      [](const std::monostate&) -> iterator { return {std::monostate{}}; },
      [](const jwt_list_state& state) -> iterator {
          return {
            iterator::jwt_list_it_state{.jwt_list_state = &state, .index = 0}};
      },
      [](const jwt_string_state& state) -> iterator {
          return {iterator::jwt_string_it_state{&state}};
      },
      [](const materialized_state& state) -> iterator {
          return {iterator::materialized_it_state{
            .materialized_state = &state, .index = 0}};
      });
}

group_range::iterator group_range::end() const& noexcept {
    return ss::visit(
      _storage,
      [](const std::monostate&) -> iterator { return {std::monostate{}}; },
      [](const jwt_list_state& state) -> iterator {
          return {iterator::jwt_list_it_state{
            .jwt_list_state = &state, .index = state.list_claim.size()}};
      },
      [](const jwt_string_state& state) -> iterator {
          return {iterator::jwt_string_it_state{&state, {}}};
      },
      [](const materialized_state& state) -> iterator {
          return {iterator::materialized_it_state{
            .materialized_state = &state,
            .index = state.materialized_groups.size()}};
      });
}

} // namespace security
