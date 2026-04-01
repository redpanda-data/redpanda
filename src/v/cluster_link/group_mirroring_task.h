#pragma once

#include "base/format_to.h"
#include "cluster_link/deps.h"
#include "cluster_link/task.h"
#include "constants/common.h"
#include "kafka/protocol/types.h"

#include <fmt/format.h>

#include <expected>
namespace cluster_link {

/**
 * @brief Task responsible for mirroring consumer group offsets across cluster
 * links
 *
 * This task manages the synchronization of consumer group committed offsets
 * from a source cluster to a target cluster. It performs the following
 * operations:
 *
 * 1. Periodically discovers consumer groups in the source cluster by querying
 * all brokers
 * 2. Identifies which groups should be mirrored based on configuration filters
 * 3. Tracks group coordinators to optimize offset fetch operations
 * 4. Fetches committed offsets for each consumer group from the source cluster
 * 5. Commits those offsets to the corresponding consumer groups in the target
 * cluster
 *
 * The task runs on shards that own partitions of the __consumer_offsets topic
 * and are leaders for those partitions. It supports concurrent requests with a
 * configurable limit to optimize throughput while avoiding overwhelming the
 * clusters.
 *
 * Configuration is provided through consumer_groups_mirroring_config which
 * includes:
 * - Group sync interval: How often to synchronize offsets for known groups
 * - Consumer group filters: Which groups to include/exclude from mirroring
 *
 * Task handles terminal error by marking the state of link as unavailable. The
 * task is made unavailable only if the operations for each
 * broker/partition/group failed. Otherwise it is assumed that the task can
 * still make progress.
 */
class group_mirroring_task : public task {
public:
    struct group_metadata {
        std::optional<::model::node_id> coordinator_id;
    };

    struct partition_committed_offset {
        ::model::partition_id partition;
        ::kafka::offset committed_offset;

        fmt::iterator format_to(fmt::iterator) const;
    };

    struct topic_offsets {
        ::model::topic topic;
        ::chunked_vector<partition_committed_offset> partition_offsets;

        fmt::iterator format_to(fmt::iterator) const;
    };

    struct group_offsets {
        using topics_t = chunked_vector<topic_offsets>;
        kafka::group_id group_id;
        topics_t topic_offsets;

        fmt::iterator format_to(fmt::iterator) const;
    };

    static constexpr auto task_name = "Consumer Group Shadowing";
    static constexpr auto concurrent_requests_limit
      = constants::common::default_concurrency;

    group_mirroring_task(link* link, const model::metadata& link_metadata);
    group_mirroring_task(const group_mirroring_task&) = delete;
    group_mirroring_task(group_mirroring_task&&) = delete;
    group_mirroring_task& operator=(const group_mirroring_task&) = delete;
    group_mirroring_task& operator=(group_mirroring_task&&) = delete;
    ~group_mirroring_task() override = default;

    void update_config(const model::metadata& link_metadata) override;

    model::enabled_t is_enabled() const final;

protected:
    ss::future<state_transition> run_impl(ss::abort_source&) override;

    bool should_start_impl(ss::shard_id, ::model::node_id) const final;

    bool should_stop_impl(ss::shard_id, ::model::node_id) const final;

private:
    struct error {
        error(ss::sstring msg, kafka::error_code errc)
          : message(std::move(msg))
          , errc(errc) {}

        explicit error(ss::sstring msg)
          : message(std::move(msg))
          , errc(kafka::error_code::none) {}

        ss::sstring message;
        kafka::error_code errc;
        fmt::iterator format_to(fmt::iterator it) const;
    };
    using result_t = std::expected<void, error>;

    ss::future<result_t> list_consumer_groups();

    ss::future<std::expected<chunked_vector<kafka::group_id>, error>>
    list_groups_from_broker(::model::node_id broker_id);

    ss::future<std::expected<::model::node_id, error>> do_find_coordinator(
      const kafka::group_id& group_id, kafka::api_version max_version);

    ss::future<std::expected<chunked_vector<kafka::coordinator>, error>>
    do_find_coordinator_batched(
      chunked_vector<ss::sstring> group_ids, kafka::api_version max_version);

    inline bool should_group_be_mirrored(
      const kafka::group_id& group_id,
      const chunked_hash_set<::model::partition_id>&
        current_shard_coordinators);

    ss::future<result_t> update_group_coordinators();

    ss::future<result_t> synchronize_consumer_groups_offsets();

    ss::future<
      std::expected<chunked_vector<group_mirroring_task::group_offsets>, error>>
    fetch_offsets(
      chunked_vector<kafka::group_id> groups, ::model::node_id coordinator_id);

    ss::future<std::expected<chunked_vector<topic_offsets>, error>>
    process_single_group_fetch_offsets_response(
      const kafka::group_id& group_id, kafka::offset_fetch_response resp);

    ss::future<std::expected<void, group_mirroring_task::error>>
    fetch_and_commit_offsets(
      chunked_vector<kafka::group_id> groups, ::model::node_id coordinator_id);

    ss::future<>
    commit_group_offsets(chunked_vector<group_offsets> group_offsets);

    ss::future<chunked_vector<group_offsets>>
      trim_to_partition_highwatermark(chunked_vector<group_offsets>);

    kafka::client::cluster& get_cluster_connection();

    void handle_offset_commit_response(
      kafka::group_id, kafka::offset_commit_response);

    [[nodiscard]] state_transition make_unavailable(const error& err);
    [[nodiscard]] state_transition make_active();

    ss::future<std::optional<kafka::offset>> get_partition_high_watermark(
      const ::model::topic& topic, ::model::partition_id p_id);

    model::consumer_groups_mirroring_config _config;
    chunked_hash_map<kafka::group_id, group_metadata> _groups_to_mirror;

    bool _needs_coordinator_update = false;
};

/**
 * @brief Factory used to create the consumer offsets mirroring task
 */
class group_mirroring_task_factory : public task_factory {
public:
    /// Returns the name of the task that this factory creates
    std::string_view created_task_name() const noexcept override;

    /// Creates a new task through the factory.  Provides the owning link
    std::unique_ptr<task> create_task(link* link) override;
};
} // namespace cluster_link
