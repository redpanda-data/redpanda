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
#pragma once
#include "base/format_to.h"
#include "container/chunked_vector.h"
#include "container/intrusive_list_helpers.h"
#include "kafka/client/direct_consumer/api_types.h"
#include "kafka/client/direct_consumer/data_queue.h"
#include "kafka/protocol/fetch.h"
#include "kafka/protocol/list_offset.h"
#include "kafka/protocol/types.h"
#include "model/fundamental.h"
#include "utils/mutex.h"
#include "utils/prefix_logger.h"

#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/rwlock.hh>

#include <fmt/format.h>

#include <optional>

namespace kafka::client {
class direct_consumer;

struct fetch_session_state {
    enum class state {
        none,
        need_full_fetch,
        incremental_fetch,
        needs_close,
    };

    fetch_session_state(
      model::node_id id,
      prefix_logger& logger,
      fetch_sessions_enabled sessions_enabled = fetch_sessions_enabled::yes);

    /**
     *         reset()            any ID
     *    sessions OFF           +------+
     * (epoch = final)|          |      |
     *                |          |      |
     *                |     +----v------+---+
     * +--------------+---->|     none      |
     * |                    +-------+-------+
     * |                            | enable
     * |                            | sessions
     * |                            v (epoch = init)
     * |        reset()     +---------------+
     * |    sessions ON     |               |<---+
     * |     -------------->|   full_fetch  |    | invalid ID
     * | (epoch = init) |   |               +----+
     * |                |   +-------+-------+
     * |                |           |
     * |                |           | valid ID (epoch++)
     * |                |           v
     * |                |   +---------------+
     * |                +---+               |<--+
     * |                |   |  incremental  |   | same ID (epoch++)
     * |     invalid ID |   |               +---+
     * |    sessions ON |   +-------+-------+
     * | (epoch = init) |           |
     * |                |           | different ID (epoch = final)
     * |                |           v OR disable sessions
     * |                |   +---------------+
     * |                |   |               |<--+
     * |                +---+  needs_close  |   | valid ID
     * |                    |               +---+
     * +------------------------------------+
     *       invalid ID
     *     sessions OFF
     *  (epoch = final)
     *
     * Basic lifecycle of a fetch session, driven primarily by session IDs
     * returned by the broker in fetch_response.
     */
    void update_fetch_session(kafka::fetch_session_id id);
    void reset();
    void toggle(fetch_sessions_enabled enable);

    bool incremental() const {
        return session_state == state::incremental_fetch
               || session_state == state::needs_close;
    }

    /**
     * Paraphrased from KIP-227
     *
     * FetchRequest metadata meaning
     * | ID  | Epoch  | Meaning                                             |
     * |-----|--------|-----------------------------------------------------|
     * | 0   | -1     | Make a full fetch that doesn't use sessions         |
     * | 0   | 0      | Make a full fetch that tries to create a session    |
     * | $ID | 0      | Close the session w/ $ID and create a new one       |
     * | $ID | $EPOCH | Make an incremental fetch request                   |
     * | $ID | -1     | Close the session w/ $ID and don't create a new one |
     *
     * FetchResponse metadata meaning
     * | ID  | Meaning                                                     |
     * |-----|-------------------------------------------------------------|
     * | 0   | No fetch session was created                                |
     * | $ID | The next request can be an incremental fetch with given $ID |
     */
    kafka::fetch_session_id session_id;
    kafka::fetch_session_epoch session_epoch;
    state session_state;

private:
    void advance_epoch() {
        if (session_epoch == kafka::fetch_session_epoch::max()) {
            session_epoch = kafka::initial_fetch_session_epoch + 1;
        } else {
            ++session_epoch;
        }
    }
    model::node_id _id;
    prefix_logger* _logger;
    fetch_sessions_enabled _fetch_sessions_enabled;
    fmt::iterator format_to(fmt::iterator it) const;
};

/**
 * Class responsible for fetching data from a single broker. It is maintaining a
 * list of partitions and corresponding fetch offsets. The fetcher loop
 * constantly querying the broker for data and updates the fetch offsets based
 * on the received data. Fetcher is also responsible for fetching offsets to
 * apply the reset policy.
 *
 * Fetcher put the fetched data into the parent consumer's data queue.
 *
 * Fetcher locks the assignment state while changing assignments, preparing the
 * fetch request and processing the response. Every time the partition
 * assignment is updated its corresponding assignment epoch is incremented.
 * Partition fetch responses with stale assignment epoch are ignored. This
 * concurrency control mechanism guarantees that the fetcher will not update its
 * subscriptions with the stale information from the request when it was changed
 * more than once while it was waiting for the response.
 *
 *
 *
 * TODO:
 * - support incremental fetches
 * - support leader epochs
 * - support server side throttling and quotas
 * - support topic identifiers
 */
class fetcher {
public:
    fetcher(direct_consumer* parent, model::node_id id, fetch_sessions_enabled);
    void start();

    ss::future<> stop();
    /**
     * Assign fetcher partitions, it can be used to assign new partitions or
     * update the fetch offsets for the already assigned partitions.
     */
    ss::future<> assign_partition(
      model::topic_partition_view, std::optional<kafka::offset>);
    /**
     * Unassign partition from the fetcher, it will stop fetching data for the
     * partition and remove it from the fetcher state. After the unassignment
     * the fetcher will not return any data for the partition.
     *
     * Returned offset can be used to reassign the partition later to different
     * fetcher to continue reading from the same offset.
     */

    ss::future<std::optional<kafka::offset>>
      unassign_partition(model::topic_partition_view);

    bool is_idle() const {
        return _partitions.empty() && _partitions_to_forget.empty();
    }

    void toggle_sessions(fetch_sessions_enabled);

private:
    using assignment_epoch = named_type<uint64_t, struct fetcher_epoch_tag>;
    struct partition_fetch_state {
        model::partition_id partition_id;
        std::optional<kafka::offset> fetch_offset;
        std::optional<kafka::offset> high_watermark;
        leader_epoch current_leader_epoch{kafka::invalid_leader_epoch};
        assignment_epoch assignment_epoch{0};
        bool incremental_include{false};

        bool include_in_fetch_request() const {
            return fetch_offset.has_value();
        }
    };
    using state_list = chunked_vector<partition_fetch_state>;
    struct partitions_to_process {
        model::topic topic;
        state_list to_include_in_fetch;
        state_list to_list_offsets;
        chunked_vector<model::partition_id> to_forget;

        bool empty() const {
            return to_include_in_fetch.empty() && to_list_offsets.empty()
                   && to_forget.empty();
        }
    };
    struct partitions_with_epoch {
        topic_partition_map<assignment_epoch> assignment_epochs;
        chunked_vector<partitions_to_process> partitions;
    };
    struct fetch_response_content {
        chunked_vector<fetched_topic_data> topics;
        size_t total_bytes{0};
        bool needs_metadata_update{false};
        kafka::fetch_session_id session_id{0};
    };

    static std::optional<assignment_epoch> find_assignment_epoch(
      const model::topic& topic,
      model::partition_id partition,
      const topic_partition_map<assignment_epoch>& epochs);

    ss::future<api_version> get_fetch_request_version() const;
    ss::future<api_version> get_list_offsets_request_version() const;
    ss::future<> do_fetch();
    ss::future<partitions_with_epoch> collect_partitions();
    ss::future<kafka::error_code> maybe_initialise_fetch_offsets(
      const chunked_vector<partitions_to_process>&,
      const topic_partition_map<assignment_epoch>& epochs);

    ss::future<fetch_request>
    make_fetch_request(const chunked_vector<partitions_to_process>&);

    ss::future<kafka_result<fetch_response_content>> process_fetch_response(
      fetch_response resp,
      const topic_partition_map<assignment_epoch>& epochs,
      const chunked_vector<partitions_to_process>& partitions);
    /**
     * Returns false if the partition was not found or the fetch offset was
     * not updated.
     * This indicates that the fetch response should be ignored.
     */
    bool maybe_update_fetch_offset(
      const model::topic&,
      model::partition_id,
      kafka::offset,
      kafka::offset,
      std::optional<assignment_epoch>);

    ss::future<kafka_result<chunked_vector<topic_partition_offsets>>>
      do_list_offsets(list_offsets_request);

    data_queue& queue();
    prefix_logger& logger();
    assignment_epoch next_epoch() { return ++_epoch; }

    void reset_partition_offset(model::topic_partition_view);

    direct_consumer* _parent;
    model::node_id _id;
    fetch_session_state _session_state;
    topic_partition_map<partition_fetch_state> _partitions;
    topic_partition_map<model::partition_id> _partitions_to_forget;
    ss::condition_variable _partitions_updated;
    ss::gate _gate;
    mutex _state_lock;
    assignment_epoch _epoch{0};
    ss::abort_source _as;
};
} // namespace kafka::client

namespace fmt {
template<>
struct fmt::formatter<kafka::client::fetch_session_state::state>
  : formatter<std::string_view> {
    auto
    format(kafka::client::fetch_session_state::state, format_context&) const
      -> iterator;
};

} // namespace fmt
