// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "migrations/cloud_storage_config.h"

#include "cluster/controller.h"
#include "cluster/feature_manager.h"
#include "cluster/topics_frontend.h"
#include "features/feature_table.h"
#include "features/logger.h"
#include "migrations/feature_migrator.h"
#include "ssx/future-util.h"
#include "vlog.h"

#include <seastar/core/sleep.hh>

#include <type_traits>

using namespace std::chrono_literals;

namespace features::migrators {

template<typename T>
cluster::property_update<T> make_property_set(const T& v) {
    return cluster::property_update<T>(
      v, cluster::incremental_update_operation::set);
}

ss::future<> cloud_storage_config::do_mutate() {
    cluster::topic_table& topic_table = _controller.get_topics_state().local();

    vlog(featureslog.info, "Checking for topic configs to update...");
    size_t update_count = 0;
    for (const auto& i : topic_table.topics_map()) {
        auto& topic_conf = i.second.metadata.get_configuration();

        auto& props = topic_conf.properties;

        auto cloud_storage_enabled
          = config::shard_local_cfg().cloud_storage_enabled();

        auto remote_write_enabled
          = model::is_archival_enabled(props.shadow_indexing.value_or(
              model::shadow_indexing_mode::disabled))
            || config::shard_local_cfg().cloud_storage_enable_remote_write();

        if (
          props.retention_local_target_bytes.has_optional_value()
          || props.retention_local_target_ms.has_optional_value()) {
            // This may be a retry, if we got partway through an upgrade and
            // were interrupted: do not keep emitting configuration update
            // events for all topics
            vlog(
              featureslog.info,
              "Tiered storage topic {} has already been updated",
              i.first.tp);
            continue;
        }

        if (cloud_storage_enabled && remote_write_enabled) {
            // This topic must have been created prior to the cloud_retention
            // feature, because we block topic create/alter while this feature
            // is not yet active.

            cluster::topic_properties_update update;
            update.tp_ns = i.first;

            // In Redpanda 22.3, the meaning of the retention.[ms|bytes]
            // properties changes: they used to describe local retention for
            // a tiered storage topic, in >=22.3 they describe total retention.
            update.properties.retention_duration = make_property_set(
              tristate<std::chrono::milliseconds>{});
            update.properties.retention_bytes = make_property_set(
              tristate<size_t>{});
            update.properties.retention_local_target_bytes = make_property_set(
              props.retention_bytes);
            update.properties.retention_local_target_ms = make_property_set(
              props.retention_duration);

            // Prior to Redpanda 22.3, data in S3 was not deleted on topic
            // deletion: preserve this behavior for legacy topics.
            update.properties.remote_delete = make_property_set(false);

            // Prior to Redpanda 22.3, cluster default properties were
            // applied at runtime and not applied persistently to newly
            // created topics.
            auto topic_remote_write = false;
            auto topic_remote_read = false;

            if (props.shadow_indexing) {
                switch (props.shadow_indexing.value()) {
                case model::shadow_indexing_mode::disabled:
                    break;
                case model::shadow_indexing_mode::archival:
                    topic_remote_write = true;
                    break;
                case model::shadow_indexing_mode::fetch:
                    topic_remote_read = true;
                    break;
                case model::shadow_indexing_mode::full:
                    topic_remote_write = true;
                    topic_remote_read = true;
                    break;
                default:
                    break;
                }
            }

            auto remote_write
              = config::shard_local_cfg().cloud_storage_enable_remote_write()
                || topic_remote_write;
            auto remote_read
              = config::shard_local_cfg().cloud_storage_enable_remote_read()
                || topic_remote_read;

            model::shadow_indexing_mode mode
              = model::shadow_indexing_mode::disabled;

            if (remote_write) {
                mode = model::shadow_indexing_mode::archival;
            }

            if (remote_read) {
                mode = mode == model::shadow_indexing_mode::archival
                         ? model::shadow_indexing_mode::full
                         : model::shadow_indexing_mode::fetch;
            }

            update.properties.shadow_indexing = make_property_set(
              std::make_optional(mode));

            vlog(
              featureslog.info, "Updating tiered storage topic {}", i.first.tp);

            auto result = co_await _controller.get_topics_frontend()
                            .local()
                            .do_update_topic_properties(
                              std::move(update),
                              model::timeout_clock::now() + 5s);
            if (result.ec != cluster::errc::success) {
                vlog(featureslog.warn, "Topic update failed: {}", result);
                throw std::runtime_error(
                  fmt::format("Failed to update topic {}", i.first.tp));
            }
            vlog(featureslog.debug, "Topic update result: {}", result);

            update_count += 1;
        }
    }
    vlog(
      featureslog.info,
      "Updated {} topics' properties during upgrade",
      update_count);
}
} // namespace features::migrators
