// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "client_quota_store.h"

namespace cluster::client_quota {

void store::set_quota(const entity_key& key, const entity_value& value) {
    if (!value.is_empty()) {
        _quotas.insert_or_assign(key, value);
    } else {
        _quotas.erase(key);
    }
}

void store::remove_quota(const entity_key& key) { _quotas.erase(key); }

std::optional<entity_value> store::get_quota(const entity_key& key) const {
    auto it = _quotas.find(key);
    if (it != _quotas.end()) {
        return it->second;
    }
    return std::nullopt;
}

store::range_container_type store::range(
  std::function<bool(const std::pair<entity_key, entity_value>&)>&& pred)
  const {
    range_container_type result;
    std::copy_if(
      _quotas.cbegin(), _quotas.cend(), std::back_inserter(result), pred);
    return result;
}

store::container_type::size_type store::size() const { return _quotas.size(); }

void store::clear() { _quotas.clear(); }

const store::container_type& store::all_quotas() const { return _quotas; }

} // namespace cluster::client_quota
