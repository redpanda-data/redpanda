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

#include "absl/container/flat_hash_map.h"
#include "absl/container/node_hash_map.h"
#include "pandaproxy/schema_registry/types.h"
#include "security/acl_store.h"
#include "security/logger.h"
#include "serde/envelope.h"
#include "serde/read_header.h"
#include "serde/rw/rw.h"
#include "utils/to_string.h"

#include <seastar/coroutine/maybe_yield.hh>

#include <container/chunked_vector.h>
#include <fmt/format.h>

namespace security {

namespace {
std::optional<std::tuple<principal_type, std::string_view>>
extract_principal_and_type(std::string_view principal) {
    constexpr std::string_view user_prefix{"User:"};
    constexpr std::string_view role_prefix{"RedpandaRole:"};
    constexpr std::string_view group_prefix{"Group:"};

    if (principal.starts_with(user_prefix)) {
        return std::make_tuple(
          principal_type::user, principal.substr(user_prefix.size()));
    }

    if (principal.starts_with(group_prefix)) {
        return std::make_tuple(
          principal_type::group, principal.substr(group_prefix.size()));
    }

    if (principal.starts_with(role_prefix)) {
        return std::make_tuple(
          principal_type::role, principal.substr(role_prefix.size()));
    }
    return std::nullopt;
}
} // namespace

void acl_entry_set::insert(acl_entry entry) {
    auto [it, ins] = _entries.insert(std::move(entry));
    if (
      const auto& principal = it->principal();
      ins && principal.type() == principal_type::role) {
        _role_cache[principal.name_view()] += 1;
    }
}

void acl_entry_set::remove_if_role(const acl_principal_base& p) {
    if (p.type() != principal_type::role) {
        return;
    }
    if (auto it = _role_cache.find(p.name_view()); it != _role_cache.end()) {
        it->second -= 1;
        vassert(
          it->second >= 0,
          "Role binding count unexpectedly < 0: {}",
          it->second);
        if (it->second == 0) {
            _role_cache.erase(it);
        }
    }
}

std::optional<std::reference_wrapper<const acl_entry>> acl_entry_set::find(
  acl_operation operation,
  const acl_principal_base& principal,
  const acl_host& host,
  acl_permission perm) const {
    // NOTE(oren): We don't allow wildcard roles, so we can short circuit on a
    // straight name lookup here. This is advantageous because the common case
    // for role-based authZ requires several ACL lookups (one for each role to
    // which an authenticated principal belongs), each requiring linear work in
    // the length of the target resource ACL set.
    if (
      principal.type() == principal_type::role
      && !_role_cache.contains(principal.name_view())) {
        return std::nullopt;
    }
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
    if (wildcards && !wildcards->acl_entry_set.get().empty()) {
        return false;
    }
    if (literals && !literals->acl_entry_set.get().empty()) {
        return false;
    }
    return std::all_of(
      prefixes.begin(), prefixes.end(), [](const entry_set_ref& e) {
          return e.acl_entry_set.get().empty();
      });
}

std::optional<security::acl_match> acl_matches::find(
  acl_operation operation,
  const acl_principal_base& principal,
  const acl_host& host,
  acl_permission perm) const {
    for (const auto& entries : prefixes) {
        if (
          auto entry = entries.acl_entry_set.get().find(
            operation, principal, host, perm);
          entry.has_value()) {
            return {{entries.resource, *entry}};
        }
    }

    if (wildcards) {
        if (
          auto entry = wildcards->acl_entry_set.get().find(
            operation, principal, host, perm);
          entry.has_value()) {
            return {{wildcards->resource, *entry}};
        }
    }

    if (literals) {
        if (
          auto entry = literals->acl_entry_set.get().find(
            operation, principal, host, perm);
          entry.has_value()) {
            return {{literals->resource, *entry}};
        }
    }

    return std::nullopt;
}

acl_matches
acl_store::find(resource_type resource, const ss::sstring& name) const {
    using opt_entry_set = std::optional<acl_matches::entry_set_ref>;

    const resource_pattern wildcard_pattern(
      resource, resource_pattern::wildcard, pattern_type::literal);

    opt_entry_set wildcards;
    if (const auto it = _acls.find(wildcard_pattern); it != _acls.end()) {
        wildcards = {it->first, it->second};
    }

    const resource_pattern literal_pattern(
      resource, name, pattern_type::literal);

    opt_entry_set literals;
    if (const auto it = _acls.find(literal_pattern); it != _acls.end()) {
        literals = {it->first, it->second};
    }

    auto prefixes = get_prefix_view<acl_matches::entry_set_ref>(
      _acls, resource, name);

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
    // NOTE: the algorithm below requires pointer stability of the key
    absl::node_hash_map<acl_binding, size_t> deleted;
    // NOTE(oren): deleted bindings are held in this scope, so
    // non-owning references are safe to use as long as they are
    // accessed exclusively inside the loop body.
    chunked_vector<acl_principal_view> maybe_roles;

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
          [&filters, &resource, &deleted, &maybe_roles, dry_run](
            const acl_entry& entry) {
              for (const auto& filter : filters) {
                  if (filter.first.entry().matches(entry)) {
                      auto binding = acl_binding(resource, entry);
                      auto [it, _] = deleted.emplace(binding, filter.second);
                      if (
                        const auto& p = it->first.entry().principal();
                        !dry_run && p.type() == principal_type::role) {
                          maybe_roles.emplace_back(p);
                      }
                      return !dry_run;
                  }
              }
              return false;
          });
        for (const auto& principal : maybe_roles) {
            it->second.remove_if_role(principal);
        }
        // ensure that elements won't outlive the corresponding bindings
        // in enclosing scope.
        maybe_roles.clear();
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

ss::future<chunked_vector<acl_binding>> acl_store::all_bindings() const {
    chunked_vector<acl_binding> result;
    for (const auto& acl : _acls) {
        for (const auto& entry : acl.second) {
            result.push_back(acl_binding{acl.first, entry});
            co_await ss::coroutine::maybe_yield();
        }
    }
    co_return result;
}

ss::future<>
acl_store::reset_bindings(const chunked_vector<acl_binding>& bindings) {
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

acl_principal acl_principal::from_string(std::string_view principal) {
    auto maybe_type_and_name = extract_principal_and_type(principal);
    if (!maybe_type_and_name) {
        throw acl_conversion_error(
          fmt::format("Invalid principal name: {{{}}}", principal));
    }

    auto [type, name] = maybe_type_and_name.value();

    if (unlikely(name.empty())) {
        throw acl_conversion_error(
          fmt::format("Principal name cannot be empty"));
    }
    if (name == "*" && type != principal_type::user) {
        throw acl_conversion_error(
          fmt::format(
            "Illegal wildcard principal: only user principals support "
            "wildcards: {{{}}}",
            principal));
    }
    return {type, ss::sstring{name}};
}

template<>
std::optional<resource_type>
from_string_view<resource_type>(std::string_view str) {
    return string_switch<std::optional<resource_type>>(str)
      .match(to_string_view(resource_type::topic), resource_type::topic)
      .match(to_string_view(resource_type::group), resource_type::group)
      .match(to_string_view(resource_type::cluster), resource_type::cluster)
      .match(
        to_string_view(resource_type::transactional_id),
        resource_type::transactional_id)
      .match(
        to_string_view(resource_type::sr_subject), resource_type::sr_subject)
      .match(
        to_string_view(resource_type::sr_registry), resource_type::sr_registry)
      .default_match(std::nullopt);
}

template<>
std::optional<pattern_type>
from_string_view<pattern_type>(std::string_view str) {
    return string_switch<std::optional<pattern_type>>(str)
      .match(to_string_view(pattern_type::literal), pattern_type::literal)
      .match(to_string_view(pattern_type::prefixed), pattern_type::prefixed)
      .default_match(std::nullopt);
}

template<>
std::optional<acl_operation>
from_string_view<acl_operation>(std::string_view str) {
    return string_switch<std::optional<acl_operation>>(str)
      .match(to_string_view(acl_operation::all), acl_operation::all)
      .match(to_string_view(acl_operation::read), acl_operation::read)
      .match(to_string_view(acl_operation::write), acl_operation::write)
      .match(to_string_view(acl_operation::create), acl_operation::create)
      .match(to_string_view(acl_operation::remove), acl_operation::remove)
      .match(to_string_view(acl_operation::alter), acl_operation::alter)
      .match(to_string_view(acl_operation::describe), acl_operation::describe)
      .match(
        to_string_view(acl_operation::cluster_action),
        acl_operation::cluster_action)
      .match(
        to_string_view(acl_operation::describe_configs),
        acl_operation::describe_configs)
      .match(
        to_string_view(acl_operation::alter_configs),
        acl_operation::alter_configs)
      .match(
        to_string_view(acl_operation::idempotent_write),
        acl_operation::idempotent_write)
      .default_match(std::nullopt);
}

template<>
std::optional<acl_permission>
from_string_view<acl_permission>(std::string_view str) {
    return string_switch<std::optional<acl_permission>>(str)
      .match(to_string_view(acl_permission::deny), acl_permission::deny)
      .match(to_string_view(acl_permission::allow), acl_permission::allow)
      .default_match(std::nullopt);
}

template<>
std::optional<principal_type>
from_string_view<principal_type>(std::string_view str) {
    return string_switch<std::optional<principal_type>>(str)
      .match(to_string_view(principal_type::user), principal_type::user)
      .match(
        to_string_view(principal_type::ephemeral_user),
        principal_type::ephemeral_user)
      .match(to_string_view(principal_type::role), principal_type::role)
      .match(to_string_view(principal_type::group), principal_type::group)
      .default_match(std::nullopt);
}

std::ostream& operator<<(std::ostream& os, acl_operation op) {
    return os << to_string_view(op);
}

std::ostream& operator<<(std::ostream& os, acl_permission perm) {
    return os << to_string_view(perm);
}

std::ostream& operator<<(std::ostream& os, resource_type type) {
    return os << to_string_view(type);
}

std::ostream& operator<<(std::ostream& os, pattern_type type) {
    return os << to_string_view(type);
}

std::ostream& operator<<(std::ostream& os, principal_type type) {
    return os << to_string_view(type);
}

std::ostream&
operator<<(std::ostream& os, const acl_principal_base& principal) {
    fmt::print(os, "{:l}", principal);
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

std::ostream&
operator<<(std::ostream& os, resource_pattern_filter::resource_subsystem s) {
    using resource_subsystem = resource_pattern_filter::resource_subsystem;
    switch (s) {
    case resource_subsystem::kafka:
        return os << "kafka";
    case resource_subsystem::schema_registry:
        return os << "schema_registry";
    }
    __builtin_unreachable();
}

std::ostream& operator<<(std::ostream& o, const resource_pattern_filter& f) {
    fmt::print(
      o,
      "{{ resource: {} name: {} pattern: {} subsystem: {}}}",
      f._resource,
      f._name,
      f._pattern,
      f._subsystem);
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
        if (
          std::holds_alternative<resource_pattern_filter::pattern_match>(
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

    switch (_subsystem) {
    case resource_subsystem::kafka:
        if (pattern.resource() > resource_type::transactional_id) {
            return false;
        }
        break;
    case resource_subsystem::schema_registry:
        if (pattern.resource() < resource_type::sr_subject) {
            return false;
        }
        break;
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

void read_nested_v0(
  iobuf_parser& in,
  resource_pattern_filter& filter,
  const size_t bytes_left_limit) {
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

void write_v0(iobuf& out, resource_pattern_filter filter) {
    using serde::write;

    using serialized_pattern_type
      = resource_pattern_filter::serialized_pattern_type;

    std::optional<serialized_pattern_type> pattern;
    if (filter.pattern()) {
        if (
          std::holds_alternative<
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

namespace {
[[maybe_unused]] void write_v0_dummy_resource_pattern_filter(iobuf& out) {
    using serde::write;

    write<std::optional<resource_type>>(out, std::nullopt);
    write<std::optional<ss::sstring>>(out, std::nullopt);
    write<std::optional<resource_pattern_filter::serialized_pattern_type>>(
      out, std::nullopt);
}
} // namespace

void acl_binding_filter::serde_write(iobuf& out) const {
    using serde::write;

    // Wire format for backwards/forwards compatibility:
    // clang-format off
    // V0: | serde header | raw resource_pattern_filter | enveloped acl_entry_filter |
    // V1: | serde header | raw resource_pattern_filter | enveloped acl_entry_filter | enveloped resource_pattern_filter |
    // V?: | serde header | dummy resource_pattern_filter | enveloped acl_entry_filter | enveloped resource_pattern_filter |
    // clang-format on
    //
    // V1 duplicates resource_pattern_filter (raw + enveloped) to support
    // migration:
    // - V0 readers can read the raw field and ignore the enveloped fields
    // - V1+ readers ignore the raw field and use the enveloped fields
    // Future version will replace raw field with 3-byte dummy once V0
    // compatibility is dropped

    // Write actual V0 data for backwards compatibility with old readers
    write_v0(out, _pattern);
    // TODO: Switch to dummy write once old readers are no longer supported
    // (earliest_logical_version > 25.2.1):
    // write_v0_dummy_resource_pattern_filter(out);

    write(out, _acl);
    write(out, _pattern);
}

void acl_binding_filter::serde_read(iobuf_parser& in, const serde::header& h) {
    using serde::read_nested;

    if (h._version == 0) {
        // V0: read actual data from the V0 field
        read_nested_v0(in, _pattern, h._bytes_left_limit);
    } else {
        // V1+: read and discard V0 field (written for old reader compatibility)
        resource_pattern_filter ignored;
        read_nested_v0(in, ignored, h._bytes_left_limit);
    }

    _acl = read_nested<decltype(_acl)>(in, h._bytes_left_limit);

    if (h._version >= 1) {
        // V1+: read actual data from new enveloped field
        _pattern = read_nested<decltype(_pattern)>(in, h._bytes_left_limit);
    }
}

namespace {
template<typename Other, typename Writer>
void write_other_version(iobuf& out, Writer writer) {
    // This is a test-only, simplified version of:
    // `void tag_invoke(tag_t<write_tag>, iobuf& out, T t)`

    serde::write(out, Other::redpanda_serde_version);
    serde::write(out, Other::redpanda_serde_compat_version);

    auto size_placeholder = out.reserve(sizeof(serde::serde_size_t));

    const auto size_before = out.size_bytes();

    writer();

    const auto written_size = out.size_bytes() - size_before;
    if (
      unlikely(
        written_size > std::numeric_limits<serde::serde_size_t>::max())) {
        throw serde::serde_exception("envelope too big");
    }
    const auto size = ss::cpu_to_le(
      static_cast<serde::serde_size_t>(written_size));
    size_placeholder.write(
      reinterpret_cast<const char*>(&size), sizeof(serde::serde_size_t));
}
} // namespace

void acl_binding_filter::testing_serde_full_write_v0(iobuf& out) const {
    write_other_version<testing::acl_binding_filter_v0>(out, [&]() {
        using serde::write;

        write_v0(out, _pattern);
        write(out, _acl);
    });
}

void acl_binding_filter::testing_serde_full_read_v0(
  iobuf_parser& in, const std::size_t bytes_left_limit) {
    // This follows the pattern of:
    // `void tag_invoke(tag_t<read_tag>, iobuf_parser& in, T& t, const
    // std::size_t bytes_left_limit)`

    auto h = serde::read_header<testing::acl_binding_filter_v0>(
      in, bytes_left_limit);

    read_nested_v0(in, _pattern, h._bytes_left_limit);

    if (unlikely(in.bytes_left() < h._bytes_left_limit)) {
        throw serde::serde_exception(fmt_with_ctx(
          ssx::sformat,
          "field spill over in {}, field type {}: envelope_end={}, "
          "in.bytes_left()={}",
          serde::type_str<testing::acl_binding_filter_v0>(),
          serde::type_str<decltype(_acl)>(),
          h._bytes_left_limit,
          in.bytes_left()));
    }

    if (h._bytes_left_limit != in.bytes_left()) {
        _acl = serde::read_nested<decltype(_acl)>(in, h._bytes_left_limit);
    }

    if (in.bytes_left() > h._bytes_left_limit) {
        in.skip(in.bytes_left() - h._bytes_left_limit);
    }
}

void acl_binding_filter::testing_serde_full_write_v2(iobuf& out) const {
    write_other_version<testing::acl_binding_filter_v2>(out, [&]() {
        using serde::write;

        write_v0_dummy_resource_pattern_filter(out);
        write(out, _acl);
        write(out, _pattern);
    });
}

void acl_binding_filter::testing_serde_full_read_v2(
  iobuf_parser& in, const std::size_t bytes_left_limit) {
    // The V2 read path will be identical to the V0 read path, the only
    // difference being the serde version and compat version
    auto res = serde::read_nested<testing::acl_binding_filter_v2>(
      in, bytes_left_limit);
    *this = acl_binding_filter{res._pattern, res._acl};
}

template<typename T>
const std::vector<acl_operation>& get_allowed_operations() {
    static const std::vector<acl_operation> topic_resource_ops{
      acl_operation::read,
      acl_operation::write,
      acl_operation::create,
      acl_operation::describe,
      acl_operation::remove,
      acl_operation::alter,
      acl_operation::describe_configs,
      acl_operation::alter_configs,
    };

    static const std::vector<acl_operation> group_resource_ops{
      acl_operation::read,
      acl_operation::describe,
      acl_operation::remove,
    };

    static const std::vector<acl_operation> transactional_id_resource_ops{
      acl_operation::write,
      acl_operation::describe,
    };

    static const std::vector<acl_operation> cluster_resource_ops{
      acl_operation::create,
      acl_operation::cluster_action,
      acl_operation::describe_configs,
      acl_operation::alter_configs,
      acl_operation::idempotent_write,
      acl_operation::alter,
      acl_operation::describe,
    };

    static const std::vector<acl_operation> sr_subject_resource_ops{
      acl_operation::read,
      acl_operation::write,
      acl_operation::remove,
      acl_operation::describe,
      acl_operation::alter_configs,
      acl_operation::describe_configs,
    };

    static const std::vector<acl_operation> sr_registry_resource_ops{
      acl_operation::read,
      acl_operation::describe,
      acl_operation::alter_configs,
      acl_operation::describe_configs,
    };

    auto resource_type = get_resource_type<T>();

    switch (resource_type) {
    case resource_type::cluster:
        return cluster_resource_ops;
    case resource_type::group:
        return group_resource_ops;
    case resource_type::topic:
        return topic_resource_ops;
    case resource_type::transactional_id:
        return transactional_id_resource_ops;
    case resource_type::sr_subject:
        return sr_subject_resource_ops;
    case resource_type::sr_registry:
        return sr_registry_resource_ops;
    };

    __builtin_unreachable();
}

template const std::vector<acl_operation>&
get_allowed_operations<model::topic>();
template const std::vector<acl_operation>&
get_allowed_operations<kafka::group_id>();
template const std::vector<acl_operation>&
get_allowed_operations<acl_cluster_name>();
template const std::vector<acl_operation>&
get_allowed_operations<kafka::transactional_id>();
template const std::vector<acl_operation>&
get_allowed_operations<pandaproxy::schema_registry::context_subject>();
template const std::vector<acl_operation>&
get_allowed_operations<pandaproxy::schema_registry::registry_resource>();

chunked_vector<audit::group> acl_principals_to_audit_groups(
  const chunked_vector<acl_principal>& principals) {
    chunked_vector<audit::group> audit_groups;
    audit_groups.reserve(principals.size());
    std::ranges::copy(
      principals | std::views::filter([](const auto& p) {
          return p.type() == principal_type::group;
      }) | std::views::transform([](const auto& p) {
          return audit::group{
            .type = audit::group::type_id::idp_group, .name = p.name()};
      }),
      std::back_inserter(audit_groups));
    return audit_groups;
}

} // namespace security
