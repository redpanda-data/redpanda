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
#include "security/acl.h"

#include "security/acl_store.h"
#include "security/logger.h"
#include "utils/to_string.h"

#include <seastar/coroutine/maybe_yield.hh>

#include <absl/container/flat_hash_map.h>
#include <fmt/format.h>

namespace security {

seastar::logger seclog("security");

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

ss::future<fragmented_vector<acl_binding>> acl_store::all_bindings() const {
    fragmented_vector<acl_binding> result;
    for (const auto& acl : _acls) {
        for (const auto& entry : acl.second) {
            result.push_back(acl_binding{acl.first, entry});
            co_await ss::coroutine::maybe_yield();
        }
    }
    co_return result;
}

ss::future<>
acl_store::reset_bindings(const fragmented_vector<acl_binding>& bindings) {
    // NOTE: not coroutinized because otherwise clang-14 crashes.
    _acls.clear();
    return ss::do_for_each(
             bindings,
             [this](const auto& binding) {
                 _acls[binding.pattern()].insert(binding.entry());
             })
      .then([this] {
          return ss::do_for_each(_acls, [](auto& kv) { kv.second.rehash(); });
      });
}

std::ostream& operator<<(std::ostream& os, acl_operation op) {
    switch (op) {
    case acl_operation::all:
        return os << "all";
    case acl_operation::read:
        return os << "read";
    case acl_operation::write:
        return os << "write";
    case acl_operation::create:
        return os << "create";
    case acl_operation::remove:
        return os << "remove";
    case acl_operation::alter:
        return os << "alter";
    case acl_operation::describe:
        return os << "describe";
    case acl_operation::cluster_action:
        return os << "cluster_action";
    case acl_operation::describe_configs:
        return os << "describe_configs";
    case acl_operation::alter_configs:
        return os << "alter_configs";
    case acl_operation::idempotent_write:
        return os << "idempotent_write";
    }
    __builtin_unreachable();
}

std::ostream& operator<<(std::ostream& os, acl_permission perm) {
    switch (perm) {
    case acl_permission::deny:
        return os << "deny";
    case acl_permission::allow:
        return os << "allow";
    }
    __builtin_unreachable();
}

std::ostream& operator<<(std::ostream& os, resource_type type) {
    switch (type) {
    case resource_type::topic:
        return os << "topic";
    case resource_type::group:
        return os << "group";
    case resource_type::cluster:
        return os << "cluster";
    case resource_type::transactional_id:
        return os << "transactional_id";
    }
    __builtin_unreachable();
}

std::ostream& operator<<(std::ostream& os, pattern_type type) {
    switch (type) {
    case pattern_type::literal:
        return os << "literal";
    case pattern_type::prefixed:
        return os << "prefixed";
    }
    __builtin_unreachable();
}

std::ostream& operator<<(std::ostream& os, principal_type type) {
    switch (type) {
    case principal_type::user:
        return os << "user";
    case principal_type::ephemeral_user:
        return os << "ephemeral user";
    }
    __builtin_unreachable();
}

std::ostream& operator<<(std::ostream& os, const acl_principal& principal) {
    fmt::print(os, "{{type {} name {}}}", principal._type, principal._name);
    return os;
}

std::ostream& operator<<(std::ostream& os, const resource_pattern& r) {
    fmt::print(
      os,
      "type {{{}}} name {{{}}} pattern {{{}}}",
      r._resource,
      r._name,
      r._pattern);
    return os;
}

std::ostream& operator<<(std::ostream& os, const acl_host& host) {
    if (host._addr) {
        fmt::print(os, "{{{}}}", *host._addr);
    } else {
        // we can log whatever representation we want for a wildcard host,
        // but kafka expects "*" as the wildcard representation.
        os << "{{any_host}}";
    }
    return os;
}

std::ostream& operator<<(std::ostream& os, const acl_entry& entry) {
    fmt::print(
      os,
      "{{principal {} host {} op {} perm {}}}",
      entry._principal,
      entry._host,
      entry._operation,
      entry._permission);
    return os;
}

std::ostream& operator<<(std::ostream& os, const acl_binding& binding) {
    fmt::print(os, "{{pattern {} entry {}}}", binding._pattern, binding._entry);
    return os;
}

std::ostream&
operator<<(std::ostream& os, const resource_pattern_filter::pattern_match&) {
    fmt::print(os, "{{}}");
    return os;
}

std::ostream& operator<<(std::ostream& o, const resource_pattern_filter& f) {
    fmt::print(
      o,
      "{{ resource: {} name: {} pattern: {} }}",
      f._resource,
      f._name,
      f._pattern);
    return o;
}

std::ostream& operator<<(
  std::ostream& os, resource_pattern_filter::serialized_pattern_type type) {
    using pattern_type = resource_pattern_filter::serialized_pattern_type;
    switch (type) {
    case pattern_type::literal:
        return os << "literal";
    case pattern_type::match:
        return os << "match";
    case pattern_type::prefixed:
        return os << "prefixed";
    }
    __builtin_unreachable();
}

std::ostream& operator<<(std::ostream& o, const acl_entry_filter& f) {
    fmt::print(
      o,
      "{{ pattern: {} host: {} operation: {}, permission: {} }}",
      f._principal,
      f._host,
      f._operation,
      f._permission);
    return o;
}

std::ostream& operator<<(std::ostream& o, const acl_binding_filter& f) {
    fmt::print(o, "{{ pattern: {} acl: {} }}", f._pattern, f._acl);
    return o;
}

std::vector<acl_operation> acl_implied_ops(acl_operation operation) {
    switch (operation) {
    case acl_operation::describe:
        return {
          acl_operation::describe,
          acl_operation::read,
          acl_operation::write,
          acl_operation::remove,
          acl_operation::alter,
        };
    case acl_operation::describe_configs:
        return {
          acl_operation::describe_configs,
          acl_operation::alter_configs,
        };
    default:
        return {operation};
    }
}

bool acl_entry_filter::matches(const acl_entry& other) const {
    if (_principal && _principal != other.principal()) {
        return false;
    }

    if (_host && _host != other.host()) {
        return false;
    }

    if (_operation && *_operation != other.operation()) {
        return false;
    }

    return !_permission || *_permission == other.permission();
}

std::vector<resource_pattern>
resource_pattern_filter::to_resource_patterns() const {
    if (!_resource || !_name) {
        return {};
    }

    if (
      _pattern
      && std::holds_alternative<resource_pattern_filter::pattern_match>(
        *_pattern)) {
        return {};
    }

    if (_pattern) {
        if (std::holds_alternative<resource_pattern_filter::pattern_match>(
              *_pattern)) {
            return {};
        }
        return {
          resource_pattern(
            *_resource, *_name, std::get<pattern_type>(*_pattern)),
        };
    } else {
        return {
          resource_pattern(*_resource, *_name, pattern_type::literal),
          resource_pattern(*_resource, *_name, pattern_type::prefixed),
        };
    }
}

bool resource_pattern_filter::matches(const resource_pattern& pattern) const {
    if (_resource && *_resource != pattern.resource()) {
        return false;
    }

    if (
      _pattern && std::holds_alternative<pattern_type>(*_pattern)
      && std::get<pattern_type>(*_pattern) != pattern.pattern()) {
        return false;
    }

    if (!_name) {
        return true;
    }

    if (
      !_pattern || (std::holds_alternative<pattern_type>(*_pattern)
      && std::get<pattern_type>(*_pattern) == pattern.pattern())) {
        return _name == pattern.name();
    }

    switch (pattern.pattern()) {
    case pattern_type::literal:
        return _name == pattern.name()
               || pattern.name() == resource_pattern::wildcard;

    case pattern_type::prefixed:
        return std::string_view(*_name).starts_with(pattern.name());
    }

    __builtin_unreachable();
}

void read_nested(
  iobuf_parser& in,
  resource_pattern_filter& filter,
  size_t const bytes_left_limit) {
    using serde::read_nested;

    read_nested(in, filter._resource, bytes_left_limit);
    read_nested(in, filter._name, bytes_left_limit);

    using serialized_pattern_type
      = resource_pattern_filter::serialized_pattern_type;

    auto pattern = read_nested<std::optional<serialized_pattern_type>>(
      in, bytes_left_limit);

    if (!pattern) {
        filter._pattern = std::nullopt;
        return;
    }

    switch (*pattern) {
    case serialized_pattern_type::literal:
        filter._pattern = security::pattern_type::literal;
        break;

    case serialized_pattern_type::prefixed:
        filter._pattern = security::pattern_type::prefixed;
        break;
    case serialized_pattern_type::match:
        filter._pattern = security::resource_pattern_filter::pattern_match{};
        break;
    }
}

void write(iobuf& out, resource_pattern_filter filter) {
    using serde::write;

    using serialized_pattern_type
      = resource_pattern_filter::serialized_pattern_type;

    std::optional<serialized_pattern_type> pattern;
    if (filter.pattern()) {
        if (std::holds_alternative<
              security::resource_pattern_filter::pattern_match>(
              *filter.pattern())) {
            pattern = serialized_pattern_type::match;
        } else {
            auto source_pattern = std::get<security::pattern_type>(
              *filter.pattern());
            pattern = resource_pattern_filter::to_pattern(source_pattern);
        }
    }
    write(out, filter._resource);
    write(out, filter._name);
    write(out, pattern);
}

} // namespace security
