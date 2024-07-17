/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"
#include "cluster/fwd.h"
#include "config/client_group_byte_rate_quota.h"
#include "config/property.h"
#include "utils/named_type.h"

#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>

#include <utility>

namespace kafka {

using k_client_id = named_type<ss::sstring, struct k_client_id_tag>;
using k_group_name = named_type<ss::sstring, struct k_group_name_tag>;

/// tracker_key is the we use to key into the client quotas map
///
/// Note: while the limits applicable to a single tracker_key are always going
/// to be the same, it is not guaranteed that a client id will have the same
/// tracker_key for different types of kafka requests (produce/fetch/partition
/// mutations). For examples, refer to the unit tests.
///
/// Note: the tracker_key is different from the entity_key used to configure
/// client quotas. The tracker_key defines the granularity at which we track
/// quotas, whereas the quota limits might be defined more broadly.
/// For example, if the default client quota applies to the request, the
/// tracker_key may be client id specific even though the matching entity_key is
/// the default client quota. In this case, we may have multiple independent
/// rate trackers for each unique client with all of these rate tracking having
/// the same shared quota limit
using tracker_key = std::variant<k_client_id, k_group_name>;

std::ostream& operator<<(std::ostream&, const tracker_key&);

/// client_quota_limits describes the limits applicable to a tracker_key
struct client_quota_limits {
    std::optional<uint64_t> produce_limit;
    std::optional<uint64_t> fetch_limit;
    std::optional<uint64_t> partition_mutation_limit;

    friend bool
    operator==(const client_quota_limits&, const client_quota_limits&)
      = default;
    friend std::ostream&
    operator<<(std::ostream& os, const client_quota_limits& l);
};

enum class client_quota_type {
    produce_quota,
    fetch_quota,
    partition_mutation_quota
};

inline constexpr std::array all_client_quota_types = {
  client_quota_type::produce_quota,
  client_quota_type::fetch_quota,
  client_quota_type::partition_mutation_quota};

std::ostream& operator<<(std::ostream&, client_quota_type);

struct client_quota_request_ctx {
    client_quota_type q_type;
    std::optional<std::string_view> client_id;
};

std::ostream& operator<<(std::ostream&, const client_quota_request_ctx&);

/// client_quota_rule is used for reporting metrics to show which type of rule
/// is being used for limiting clients
enum class client_quota_rule {
    not_applicable,
    kafka_client_default,
    cluster_client_default,
    kafka_client_prefix,
    cluster_client_prefix,
    kafka_client_id
};

inline constexpr std::array all_client_quota_rules = {
  client_quota_rule::not_applicable,
  client_quota_rule::kafka_client_default,
  client_quota_rule::cluster_client_default,
  client_quota_rule::kafka_client_prefix,
  client_quota_rule::cluster_client_prefix,
  client_quota_rule::kafka_client_id};

std::ostream& operator<<(std::ostream&, client_quota_rule);

struct client_quota_value {
    std::optional<uint64_t> limit;
    client_quota_rule rule;
};

std::ostream& operator<<(std::ostream&, client_quota_value);

/// client_quota_translator is responsible for providing quota_manager with a
/// simplified interface to the quota configurations
///  * It is responsible for translating the quota-specific request context (for
///  now the client.id header field) into the applicable tracker_key.
///  * It is also responsible for finding the quota limits of a tracker_key
class client_quota_translator {
public:
    using on_change_fn = std::function<void()>;

    explicit client_quota_translator(
      ss::sharded<cluster::client_quota::store>&);

    /// Returns the quota tracker key applicable to the given quota context
    /// Note: because the client quotas configured for produce/fetch/pm might be
    /// different, the tracker_key for produce/fetch/pm might be different
    tracker_key find_quota_key(const client_quota_request_ctx& ctx) const;

    /// Finds the limits applicable to the given quota tracker key
    client_quota_limits find_quota_value(const tracker_key&) const;

    /// Returns the quota tracker key and value. If no quota applies to the
    /// given context, the value may have an empty limit with the rule
    /// not_applicable
    std::pair<tracker_key, client_quota_value>
    find_quota(const client_quota_request_ctx& ctx) const;

    /// Returns the quota rule type that applies to the given tracker key and
    /// quota type
    client_quota_rule
    find_quota_rule(const tracker_key&, client_quota_type) const;

    /// `watch` can be used to register for quota changes
    void watch(on_change_fn&& fn);

    /// Returns true if there are no quotas configured
    bool is_empty() const;

private:
    using quota_config
      = std::unordered_map<ss::sstring, config::client_group_quota>;

    using config_callback = std::function<void(void)>;

    const quota_config& get_quota_config(client_quota_type qt) const;
    std::optional<uint64_t> get_default_config(client_quota_type qt) const;

    client_quota_value get_client_quota_value(
      const tracker_key& quota_id, client_quota_type qt) const;

    void maybe_log_deprecated_configs_nag() const;

    ss::sharded<cluster::client_quota::store>& _quota_store;

    std::vector<config_callback> _config_callbacks;
    config::binding<uint32_t> _default_target_produce_tp_rate;
    config::binding<std::optional<uint32_t>> _default_target_fetch_tp_rate;
    config::binding<std::optional<uint32_t>> _target_partition_mutation_quota;
    config::binding<quota_config> _target_produce_tp_rate_per_client_group;
    config::binding<quota_config> _target_fetch_tp_rate_per_client_group;
};

} // namespace kafka
