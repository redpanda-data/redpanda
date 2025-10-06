/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster_link/model/types.h"

#include "utils/to_string.h"

#include <seastar/util/variant_utils.hh>

#include <fmt/ranges.h>

#include <ostream>

namespace {
template<typename T, typename... Args>
bool is_any_of(T status, Args&&... args) {
    return ((args == status) || ...);
}
} // namespace

namespace cluster_link::model {

bool is_valid_status_transition(
  mirror_topic_status current, mirror_topic_status target) noexcept {
    switch (target) {
    case mirror_topic_status::active:
        // Currently a mirror topic is created with active status
        // and we do not allow transition back to active from a different
        // status. This needs to change when we allow failed_over/promoted
        // topics to be re-activated.
        return false;
    case mirror_topic_status::failed:
        return is_any_of(
          current,
          mirror_topic_status::active,
          mirror_topic_status::failing_over,
          mirror_topic_status::promoting);
    case mirror_topic_status::paused:
    case mirror_topic_status::failing_over:
    case mirror_topic_status::promoting:
        return is_any_of(current, mirror_topic_status::active);
    case mirror_topic_status::failed_over:
        return is_any_of(current, mirror_topic_status::failing_over);
    case mirror_topic_status::promoted:
        return is_any_of(current, mirror_topic_status::promoting);
    }
}

std::ostream& operator<<(std::ostream& os, const link_status& s) {
    return os << fmt::format("{}", s);
}

std::ostream& operator<<(std::ostream& os, mirror_topic_status s) {
    return os << fmt::format("{}", s);
}

std::ostream& operator<<(std::ostream& os, task_state s) {
    return os << fmt::format("{}", s);
}

std::ostream& operator<<(std::ostream& os, const scram_credentials& creds) {
    return os << fmt::format("{}", creds);
}

std::ostream& operator<<(std::ostream& os, const tls_file_path& p) {
    return os << fmt::format("{}", p());
}

std::ostream& operator<<(std::ostream& os, const tls_value& v) {
    return os << fmt::format("{}", v());
}

std::ostream& operator<<(std::ostream& os, const tls_file_or_value& t) {
    return os << fmt::format("{}", t);
}

std::ostream& operator<<(std::ostream& os, const connection_config& cfg) {
    return os << fmt::format("{}", cfg);
}

std::ostream& operator<<(std::ostream& os, const mirror_topic_metadata& md) {
    return os << fmt::format("{}", md);
}

std::ostream& operator<<(std::ostream& os, filter_pattern_type f) {
    return os << fmt::format("{}", f);
}

std::ostream& operator<<(std::ostream& os, filter_type f) {
    return os << fmt::format("{}", f);
}

std::ostream&
operator<<(std::ostream& os, const resource_name_filter_pattern& p) {
    return os << fmt::format("{}", p);
}

std::ostream&
operator<<(std::ostream& os, const topic_metadata_mirroring_config& cfg) {
    return os << fmt::format("{}", cfg);
}

std::ostream&
operator<<(std::ostream& os, const consumer_groups_mirroring_config& cfg) {
    return os << fmt::format("{}", cfg);
}

std::ostream& operator<<(std::ostream& os, const link_configuration& cfg) {
    return os << fmt::format("{}", cfg);
}

std::ostream& operator<<(std::ostream& os, const link_state& ls) {
    return os << fmt::format("{}", ls);
}

std::ostream& operator<<(std::ostream& os, const metadata& md) {
    return os << fmt::format("{}", md);
}

mirror_topic_metadata mirror_topic_metadata::copy() const {
    mirror_topic_metadata copy;
    copy.status = status;
    copy.source_topic_id = source_topic_id;
    copy.source_topic_name = source_topic_name;
    copy.destination_topic_id = destination_topic_id;
    copy.partition_count = partition_count;
    copy.replication_factor = replication_factor;
    copy.topic_configs.reserve(topic_configs.size());
    for (const auto& [key, value] : topic_configs) {
        copy.topic_configs.emplace(key, value);
    }

    return copy;
}

topic_metadata_mirroring_config topic_metadata_mirroring_config::copy() const {
    topic_metadata_mirroring_config copy;

    copy.is_enabled = is_enabled;
    copy.task_interval = task_interval;
    copy.topic_name_filters.reserve(topic_name_filters.size());
    for (const auto& filter : topic_name_filters) {
        copy.topic_name_filters.emplace_back(filter);
    }
    copy.topic_properties_to_mirror = topic_properties_to_mirror;

    return copy;
}

consumer_groups_mirroring_config
consumer_groups_mirroring_config::copy() const {
    consumer_groups_mirroring_config copy;

    copy.is_enabled = is_enabled;
    copy.task_interval = task_interval;
    copy.filters = filters.copy();
    return copy;
}

security_settings_sync_config security_settings_sync_config::copy() const {
    security_settings_sync_config copy;

    copy.is_enabled = is_enabled;
    copy.task_interval = task_interval;
    copy.acl_filters.reserve(acl_filters.size());
    for (const auto& filter : acl_filters) {
        copy.acl_filters.emplace_back(filter);
    }
    return copy;
}

link_configuration link_configuration::copy() const {
    link_configuration copy;
    copy.topic_metadata_mirroring_cfg = topic_metadata_mirroring_cfg.copy();
    copy.consumer_groups_mirroring_cfg = consumer_groups_mirroring_cfg.copy();
    copy.security_settings_sync_cfg = security_settings_sync_cfg.copy();
    return copy;
}

void link_state::set_mirror_topics(const mirror_topics_t& topics) {
    mirror_topics.reserve(topics.size());
    for (const auto& [topic, state] : topics) {
        mirror_topics.emplace(topic, state.copy());
    }
}

void link_state::set_mirror_topics(mirror_topics_t&& topics) {
    mirror_topics = std::move(topics);
}

link_state link_state::copy() const {
    link_state copy;
    copy.status = status;
    copy.mirror_topics.reserve(mirror_topics.size());
    for (const auto& [topic, state] : mirror_topics) {
        copy.mirror_topics.emplace(topic, state.copy());
    }
    return copy;
}

metadata metadata::copy() const {
    metadata copy;
    copy.name = name;
    copy.uuid = uuid;
    copy.connection = connection;
    copy.state = state.copy();
    copy.configuration = configuration.copy();
    return copy;
}

add_mirror_topic_cmd add_mirror_topic_cmd::copy() const {
    add_mirror_topic_cmd copy;
    copy.topic = topic;
    copy.metadata = metadata.copy();
    return copy;
}

update_mirror_topic_properties_cmd
update_mirror_topic_properties_cmd::copy() const {
    update_mirror_topic_properties_cmd copy;
    copy.topic = topic;
    copy.partition_count = partition_count;
    copy.replication_factor = replication_factor;
    copy.topic_configs.reserve(topic_configs.size());
    for (const auto& [key, value] : topic_configs) {
        copy.topic_configs.emplace(key, value);
    }
    return copy;
}

update_cluster_link_configuration_cmd
update_cluster_link_configuration_cmd::copy() const {
    update_cluster_link_configuration_cmd copy;
    copy.connection = connection;
    copy.link_config = link_config.copy();

    return copy;
}

auto format_as(acl_resource r) { return to_string_view(r); }
auto format_as(acl_pattern p) { return to_string_view(p); }
auto format_as(acl_operation o) { return to_string_view(o); }
auto format_as(acl_permission_type p) { return to_string_view(p); }
} // namespace cluster_link::model

namespace cluster_link::rpc {

fmt::iterator shadow_topic_report_request::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{ link: {}, topic: {} }}", link_id, topic_name);
}

fmt::iterator
shadow_topic_partition_leader_report::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{ partition: {} }}", partition);
}

fmt::iterator shadow_topic_report_response::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ link_update_revision: {}, leaders: [{}], err_code: {} }}",
      link_update_revision,
      fmt::join(leaders.begin(), leaders.end(), ","),
      err_code);
}

} // namespace cluster_link::rpc

auto fmt::formatter<cluster_link::model::task_state>::format(
  cluster_link::model::task_state st, format_context& ctx) const
  -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "{}", to_string_view(st));
}

auto fmt::formatter<cluster_link::model::scram_credentials>::format(
  const cluster_link::model::scram_credentials& c, format_context& ctx)
  -> decltype(ctx.out()) {
    return fmt::format_to(
      ctx.out(),
      "{{username: {}, password: ****, mechanism: {}}}",
      c.username,
      c.mechanism);
}

auto fmt::formatter<
  std::optional<cluster_link::model::connection_config::authn_variant>>::
  format(
    const std::optional<cluster_link::model::connection_config::authn_variant>&
      m,
    format_context& ctx) -> decltype(ctx.out()) {
    if (!m) {
        return fmt::format_to(ctx.out(), "none");
    }
    return ss::visit(*m, [&ctx](const auto& authn) {
        return fmt::format_to(ctx.out(), "{}", authn);
    });
}

auto fmt::formatter<cluster_link::model::tls_file_or_value>::format(
  const cluster_link::model::tls_file_or_value& t, format_context& ctx)
  -> decltype(ctx.out()) {
    return ss::visit(
      t,
      [&ctx](const cluster_link::model::tls_file_path& p) {
          return fmt::format_to(ctx.out(), "{{file: {}}}", p());
      },
      [this, &ctx](const cluster_link::model::tls_value& v) {
          if (_is_sensitive) {
              return fmt::format_to(ctx.out(), "{{value: ****}}");
          }
          // If not sensitive, we can show the value
          // This is useful for debugging purposes.
          return fmt::format_to(ctx.out(), "{{value: {}}}", v());
      });
}

auto fmt::formatter<std::optional<cluster_link::model::tls_file_or_value>>::
  format(
    const std::optional<cluster_link::model::tls_file_or_value>& m,
    format_context& ctx) -> decltype(ctx.out()) {
    if (!m) {
        return fmt::format_to(ctx.out(), "not-set");
    }
    if (_is_sensitive) {
        return fmt::format_to(ctx.out(), "{:s}", *m);
    }
    return fmt::format_to(ctx.out(), "{}", *m);
}

auto fmt::formatter<cluster_link::model::connection_config>::format(
  const cluster_link::model::connection_config& c, format_context& ctx)
  -> decltype(ctx.out()) {
    return fmt::format_to(
      ctx.out(),
      "{{bootstrap_servers: {}, authn_config: {}, tls_enabled: {}, cert: {}, "
      "key: {:s}, ca: {}, client_id: {}, metadata_max_age_ms: {}, "
      "connection_timeout_ms: {}, retry_backoff_ms: {}, fetch_wait_max_ms: {}, "
      "fetch_min_bytes: {}, fetch_max_bytes: {}}}",
      c.bootstrap_servers,
      c.authn_config,
      c.tls_enabled,
      c.cert,
      c.key,
      c.ca,
      c.client_id,
      c.metadata_max_age_ms,
      c.connection_timeout_ms,
      c.retry_backoff_ms,
      c.fetch_wait_max_ms,
      c.fetch_min_bytes,
      c.fetch_max_bytes);
}

auto fmt::formatter<std::optional<model::topic_id>>::format(
  const std::optional<model::topic_id>& m, format_context& ctx)
  -> decltype(ctx.out()) {
    if (!m) {
        return fmt::format_to(ctx.out(), "none");
    }
    return fmt::format_to(ctx.out(), "{}", *m);
}

auto fmt::formatter<cluster_link::model::mirror_topic_metadata>::format(
  const cluster_link::model::mirror_topic_metadata& m,
  format_context& ctx) const -> decltype(ctx.out()) {
    return fmt::format_to(
      ctx.out(),
      "{{state: {}, source_topic_id: {}, source_topic_name: {}, "
      "destination_topic_id: {}, partition_count: {}, replication_factor: {}, "
      "topic_configs: {}}}",
      m.status,
      m.source_topic_id,
      m.source_topic_name,
      m.destination_topic_id,
      m.partition_count,
      m.replication_factor,
      m.topic_configs);
}

auto fmt::formatter<
  decltype(cluster_link::model::link_state::mirror_topics)::value_type>::
  format(
    const decltype(cluster_link::model::link_state::mirror_topics)::value_type&
      m,
    format_context& ctx) const -> decltype(ctx.out()) {
    return fmt::format_to(
      ctx.out(), "{{topic: {}, metadata: {}}}", m.first, m.second);
}

auto fmt::formatter<cluster_link::model::filter_pattern_type>::format(
  cluster_link::model::filter_pattern_type s, format_context& ctx) const
  -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "{}", to_string_view(s));
}

auto fmt::formatter<cluster_link::model::filter_type>::format(
  cluster_link::model::filter_type s, format_context& ctx) const
  -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "{}", to_string_view(s));
}

auto fmt::formatter<cluster_link::model::resource_name_filter_pattern>::format(
  const cluster_link::model::resource_name_filter_pattern& m,
  format_context& ctx) const -> decltype(ctx.out()) {
    return fmt::format_to(
      ctx.out(),
      "{{pattern_type: {}, filter: {}, pattern: {}}}",
      m.pattern_type,
      m.filter,
      m.pattern);
}

auto fmt::formatter<cluster_link::model::topic_metadata_mirroring_config>::
  format(
    const cluster_link::model::topic_metadata_mirroring_config& m,
    format_context& ctx) const -> decltype(ctx.out()) {
    return fmt::format_to(
      ctx.out(),
      "{{is_enabled: {}, task_interval: {}, filters: {}, "
      "topic_properties_to_mirror: {}}}",
      m.is_enabled,
      m.task_interval,
      m.topic_name_filters,
      m.topic_properties_to_mirror);
}

auto fmt::formatter<cluster_link::model::consumer_groups_mirroring_config>::
  format(
    const cluster_link::model::consumer_groups_mirroring_config& m,
    format_context& ctx) const -> decltype(ctx.out()) {
    return fmt::format_to(
      ctx.out(),
      "{{is_enabled: {}, task_interval: {}, filters: {}}}",
      m.is_enabled,
      m.task_interval,
      m.filters);
}

auto fmt::formatter<cluster_link::model::link_state>::format(
  const cluster_link::model::link_state& s, format_context& ctx) const
  -> decltype(ctx.out()) {
    return fmt::format_to(
      ctx.out(),
      "{{status: {}, mirror_topics: {}}}",
      s.status,
      fmt::join(s.mirror_topics.begin(), s.mirror_topics.end(), ","));
}

auto fmt::formatter<cluster_link::model::metadata>::format(
  const cluster_link::model::metadata& m, format_context& ctx)
  -> decltype(ctx.out()) {
    return fmt::format_to(
      ctx.out(),
      "{{name: {}, uuid: {}, connection: {}, state: {}}}",
      m.name,
      m.uuid,
      m.connection,
      m.state);
}

auto fmt::formatter<cluster_link::model::add_mirror_topic_cmd>::format(
  const cluster_link::model::add_mirror_topic_cmd& m, format_context& ctx)
  -> decltype(ctx.out()) {
    return fmt::format_to(
      ctx.out(), "{{topic: {}, metadata: {}}}", m.topic, m.metadata);
}

auto fmt::formatter<cluster_link::model::update_mirror_topic_status_cmd>::
  format(
    const cluster_link::model::update_mirror_topic_status_cmd& m,
    format_context& ctx) -> decltype(ctx.out()) {
    return fmt::format_to(
      ctx.out(), "{{topic: {}, state: {}}}", m.topic, m.status);
}

auto fmt::formatter<cluster_link::model::update_mirror_topic_properties_cmd>::
  format(
    const cluster_link::model::update_mirror_topic_properties_cmd& m,
    format_context& ctx) -> decltype(ctx.out()) {
    return fmt::format_to(
      ctx.out(),
      "{{topic={}, partition_count={}, replication_factor={}, "
      "topic_configs={}}}",
      m.topic,
      m.partition_count,
      m.replication_factor,
      m.topic_configs);
}

auto fmt::formatter<cluster_link::model::task_status_report>::format(
  const cluster_link::model::task_status_report& r, format_context& ctx) const
  -> decltype(ctx.out()) {
    return fmt::format_to(
      ctx.out(),
      "{{task_name: {}, task_state: {}, task_state_reason: {}}}",
      r.task_name,
      r.task_state,
      r.task_state_reason);
}

auto fmt::formatter<decltype(cluster_link::model::link_task_status_report::
                               task_status_reports)::value_type>::
  format(
    const decltype(cluster_link::model::link_task_status_report::
                     task_status_reports)::value_type& m,
    format_context& ctx) const -> decltype(ctx.out()) {
    return fmt::format_to(
      ctx.out(),
      "{{task_name: {}, task_status_report: {}}}",
      m.first,
      m.second);
}

auto fmt::formatter<cluster_link::model::link_task_status_report>::format(
  const cluster_link::model::link_task_status_report& r,
  format_context& ctx) const -> decltype(ctx.out()) {
    return fmt::format_to(
      ctx.out(),
      "{{link_name: {}, task_status_reports: {}}}",
      r.link_name,
      fmt::join(
        r.task_status_reports.begin(), r.task_status_reports.end(), ","));
}

auto fmt::formatter<
  decltype(cluster_link::model::cluster_link_task_status_report::link_reports)::
    value_type>::
  format(
    const decltype(cluster_link::model::cluster_link_task_status_report::
                     link_reports)::value_type& m,
    format_context& ctx) const -> decltype(ctx.out()) {
    return fmt::format_to(
      ctx.out(), "{{topic: {}, metadata: {}}}", m.first, m.second);
}

auto fmt::formatter<cluster_link::model::cluster_link_task_status_report>::
  format(
    const cluster_link::model::cluster_link_task_status_report& r,
    format_context& ctx) const -> decltype(ctx.out()) {
    return fmt::format_to(
      ctx.out(),
      "{{link_reports: {}}}",
      fmt::join(r.link_reports.begin(), r.link_reports.end(), ","));
}

auto fmt::formatter<cluster_link::model::acl_resource_filter>::format(
  const cluster_link::model::acl_resource_filter& f, format_context& ctx) const
  -> decltype(ctx.out()) {
    return fmt::format_to(
      ctx.out(),
      "{{resource_type: {}, pattern_type: {}, name: {}}}",
      f.resource_type,
      f.pattern_type,
      f.name);
}

auto fmt::formatter<cluster_link::model::acl_access_filter>::format(
  const cluster_link::model::acl_access_filter& f, format_context& ctx) const
  -> decltype(ctx.out()) {
    return fmt::format_to(
      ctx.out(),
      "{{principal: {}, operation: {}, permission_type: {}, host: {}}}",
      f.principal,
      f.operation,
      f.permission_type,
      f.host);
}

auto fmt::formatter<cluster_link::model::acl_filter>::format(
  const cluster_link::model::acl_filter& f, format_context& ctx) const
  -> decltype(ctx.out()) {
    return fmt::format_to(
      ctx.out(),
      "{{resource_filter: {}, access_filter: {}}}",
      f.resource_filter,
      f.access_filter);
}

auto fmt::formatter<cluster_link::model::security_settings_sync_config>::format(
  const cluster_link::model::security_settings_sync_config& cfg,
  format_context& ctx) const -> decltype(ctx.out()) {
    return fmt::format_to(
      ctx.out(),
      "{{enabled: {}, task_interval: {}, acl_filters: {}}}",
      cfg.is_enabled,
      cfg.task_interval,
      cfg.acl_filters);
}

auto fmt::formatter<cluster_link::model::link_configuration>::format(
  const cluster_link::model::link_configuration& cfg, format_context& ctx) const
  -> decltype(ctx.out()) {
    return fmt::format_to(
      ctx.out(),
      "{{topic_metadata_mirroring_cfg: {}, consumer_groups_mirroring_cfg: {}, "
      "security_settings_sync_cfg: {}}}",
      cfg.topic_metadata_mirroring_cfg,
      cfg.consumer_groups_mirroring_cfg,
      cfg.security_settings_sync_cfg);
}

auto fmt::
  formatter<cluster_link::model::update_cluster_link_configuration_cmd>::format(
    const cluster_link::model::update_cluster_link_configuration_cmd& cfg,
    format_context& ctx) const -> decltype(ctx.out()) {
    return fmt::format_to(
      ctx.out(),
      "{{connection: {}, link_config: {}}}",
      cfg.connection,
      cfg.link_config);
}
