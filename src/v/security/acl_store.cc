/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "security/acl_store.h"

#include "security/logger.h"

#include <absl/container/flat_hash_map.h>

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
     * the acls are sorted within (resource-type, pattern-type) bounds by name
     * in reverse order so longer names (prefixes) appear first.
     */
    std::vector<acl_matches::entry_set_ref> prefixes;
    {
        auto it = _acls.lower_bound(
          resource_pattern(resource, name, pattern_type::prefixed));

        auto end = _acls.upper_bound(resource_pattern(
          resource, name.substr(0, 1), pattern_type::prefixed));

        for (; it != end; ++it) {
            if (std::string_view(name).starts_with(it->first.name())) {
                prefixes.emplace_back(it->second);
            }
        }
    }

    return acl_matches(wildcards, literals, std::move(prefixes));
}

std::vector<std::vector<acl_binding>> acl_store::remove_bindings(
  const std::vector<acl_binding_filter>& filters, bool dry_run) {
    // the pair<filter, size_t> is used to record the index of the filter in the
    // input so that returned set of matching binding is organized in the same
    // order as the input filters. this is a property needed by the kafka api.
    absl::flat_hash_map<
      resource_pattern,
      std::vector<std::pair<acl_binding_filter, size_t>>>
      resources;

    // collect binding filters that match resources
    for (auto& [pattern, entries] : _acls) {
        for (size_t i = 0U; i < filters.size(); i++) {
            const auto& filter = filters[i];
            if (filter.pattern().matches(pattern)) {
                resources[pattern].emplace_back(filter, i);
            }
        }
    }

    // do the same collection as above, but instead of the source patterns being
    // the set of existing acls the source patterns are the resources from
    // filters with the special property that they are exact matches.
    for (const auto& filter_as_resource : filters) {
        auto patterns = filter_as_resource.pattern().to_resource_patterns();
        for (const auto& pattern : patterns) {
            for (size_t i = 0U; i < filters.size(); i++) {
                const auto& filter = filters[i];
                if (filter.pattern().matches(pattern)) {
                    resources[pattern].emplace_back(filter, i);
                }
            }
        }
    }

    // deleted binding index of deleted filter that matched
    absl::flat_hash_map<acl_binding, size_t> deleted;

    for (const auto& resources_it : resources) {
        // structured binding in for-range prevents capturing reference to
        // filters in erase_if below; a limitation in current standard.
        const auto& resource = resources_it.first;
        const auto& filters = resources_it.second;

        // existing acl binding for this resource
        auto it = _acls.find(resource);
        if (it == _acls.end()) {
            continue;
        }

        // remove matching entries and track the deleted binding along with the
        // index of the filter that matched the entry.
        it->second.erase_if(
          [&filters, &resource, &deleted, dry_run](const acl_entry& entry) {
              for (const auto& filter : filters) {
                  if (filter.first.entry().matches(entry)) {
                      auto binding = acl_binding(resource, entry);
                      deleted.emplace(binding, filter.second);
                      return !dry_run;
                  }
              }
              return false;
          });
    }

    std::vector<std::vector<acl_binding>> res;
    res.assign(filters.size(), {});

    for (const auto& binding : deleted) {
        res[binding.second].push_back(binding.first);
    }

    return res;
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
