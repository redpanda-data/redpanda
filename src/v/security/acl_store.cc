/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "security/acl_store.h"

namespace security {

std::optional<std::reference_wrapper<const acl_entry>> acl_entry_set::find(
  acl_operation operation,
  const acl_principal& principal,
  const acl_host& host,
  acl_permission perm) const {
    for (const auto& entry : _entries) {
        if (entry.permission() != perm) {
            continue;
        }
        if (entry.principal() != principal && !entry.principal().wildcard()) {
            continue;
        }
        if (
          entry.operation() != operation
          && entry.operation() != acl_operation::all) {
            continue;
        }
        if (entry.host() != host && entry.host() != acl_wildcard_host) {
            continue;
        }
        return entry;
    }
    return std::nullopt;
}

bool acl_matches::empty() const {
    if (wildcards && !wildcards->get().empty()) {
        return false;
    }
    if (literals && !literals->get().empty()) {
        return false;
    }
    return std::all_of(
      prefixes.cbegin(), prefixes.cend(), [](const entry_set_ref& e) {
          return e.get().empty();
      });
}

bool acl_matches::contains(
  acl_operation operation,
  const acl_principal& principal,
  const acl_host& host,
  acl_permission perm) const {
    for (const auto& entries : prefixes) {
        if (entries.get().contains(operation, principal, host, perm)) {
            return true;
        }
    }

    if (wildcards) {
        if (wildcards->get().contains(operation, principal, host, perm)) {
            return true;
        }
    }

    if (literals) {
        if (literals->get().contains(operation, principal, host, perm)) {
            return true;
        }
    }

    return false;
}

acl_matches
acl_store::find(resource_type resource, const ss::sstring& name) const {
    using opt_entry_set = std::optional<acl_matches::entry_set_ref>;

    const resource_pattern wildcard_pattern(
      resource, resource_pattern::wildcard, pattern_type::literal);

    opt_entry_set wildcards;
    if (const auto it = _acls.find(wildcard_pattern); it != _acls.end()) {
        wildcards = it->second;
    }

    const resource_pattern literal_pattern(
      resource, name, pattern_type::literal);

    opt_entry_set literals;
    if (const auto it = _acls.find(literal_pattern); it != _acls.end()) {
        literals = it->second;
    }

    /*
     * TODO: the prefix search operation can be optimized by bounding the search
     * range by upper/lower limits, but we need to be careful about which upper
     * limit is chosen.
     */
    std::vector<acl_matches::entry_set_ref> prefixes;
    for (const auto& it : _acls) {
        if (it.first.pattern() != pattern_type::prefixed) {
            continue;
        }
        if (std::string_view(name).starts_with(it.first.name())) {
            prefixes.emplace_back(it.second);
        }
    }

    return acl_matches(wildcards, literals, std::move(prefixes));
}

void acl_store::remove_bindings(
  const std::vector<acl_binding_filter>& filters) {
    for (auto& [pattern, entries] : _acls) {
        for (const auto& filter : filters) {
            if (filter.pattern().matches(pattern)) {
                for (auto it = entries.begin(); it != entries.end(); ++it) {
                    if (filter.entry().matches(*it)) {
                        entries.erase(it);
                        entries.rehash();
                        break;
                    }
                }
            }
        }
    }
}

std::vector<acl_binding>
acl_store::acls(const acl_binding_filter& filter) const {
    std::vector<acl_binding> result;
    for (const auto& acl : _acls) {
        for (const auto& entry : acl.second) {
            acl_binding binding(acl.first, entry);
            if (filter.matches(binding)) {
                result.push_back(binding);
            }
        }
    }
    return result;
}

} // namespace security
