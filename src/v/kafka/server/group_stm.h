#pragma once
#include "cluster/fwd.h"
#include "cluster/partition.h"
#include "kafka/protocol/fwd.h"
#include "kafka/server/group.h"
#include "kafka/server/logger.h"
#include "kafka/server/member.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/log.hh>

#include <absl/container/node_hash_map.h>
#include <absl/container/node_hash_set.h>

#include <iosfwd>
#include <optional>
#include <vector>

namespace kafka {

/**
 * the value type of a group metadata log record.
 */
struct group_log_group_metadata {
    kafka::protocol_type protocol_type;
    kafka::generation_id generation;
    std::optional<kafka::protocol_name> protocol;
    std::optional<kafka::member_id> leader;
    int32_t state_timestamp;
    std::vector<member_state> members;
};

/**
 * the key type for offset commit records.
 */
struct group_log_offset_key {
    kafka::group_id group;
    model::topic topic;
    model::partition_id partition;

    bool operator==(const group_log_offset_key& other) const = default;

    friend std::ostream& operator<<(std::ostream&, const group_log_offset_key&);
};

/**
 * the value type for offset commit records.
 */
struct group_log_offset_metadata {
    model::offset offset;
    int32_t leader_epoch;
    std::optional<ss::sstring> metadata;

    friend std::ostream&
    operator<<(std::ostream&, const group_log_offset_metadata&);
};

struct group_log_prepared_tx_offset {
    model::topic_partition tp;
    model::offset offset;
    int32_t leader_epoch;
    std::optional<ss::sstring> metadata;
};

struct group_log_prepared_tx {
    kafka::group_id group_id;
    // TODO: get rid of pid, we have it in the headers
    model::producer_identity pid;
    std::vector<group_log_prepared_tx_offset> offsets;
};

struct group_log_commit_tx {
    kafka::group_id group_id;
};

} // namespace kafka

namespace std {
template<>
struct hash<kafka::group_log_offset_key> {
    size_t operator()(const kafka::group_log_offset_key& key) const {
        size_t h = 0;
        boost::hash_combine(h, hash<ss::sstring>()(key.group));
        boost::hash_combine(h, hash<ss::sstring>()(key.topic));
        boost::hash_combine(h, hash<model::partition_id>()(key.partition));
        return h;
    }
};
} // namespace std

namespace kafka {

class group_stm {
public:
    struct logged_metadata {
        model::offset log_offset;
        group_log_offset_metadata metadata;
    };

    void overwrite_metadata(group_log_group_metadata&&);
    void remove() {
        _offsets.clear();
        _is_loaded = false;
        _is_removed = true;
    }

    void update_offset(
      model::topic_partition, model::offset, group_log_offset_metadata&&);
    void remove_offset(model::topic_partition);
    void update_prepared(model::offset, group_log_prepared_tx);
    void commit(model::producer_identity);
    void abort(model::producer_identity, model::tx_seq);
    bool has_data() const {
        return !_is_removed && (_is_loaded || _offsets.size() > 0);
    }
    bool is_removed() const { return _is_removed; }

    const absl::node_hash_map<model::producer_id, group::group_prepared_tx>&
    prepared_txs() const {
        return _prepared_txs;
    }

    const absl::node_hash_map<model::topic_partition, logged_metadata>&
    offsets() const {
        return _offsets;
    }

private:
    absl::node_hash_map<model::topic_partition, logged_metadata> _offsets;
    absl::node_hash_map<model::producer_id, group::group_prepared_tx>
      _prepared_txs;
    group_log_group_metadata _metadata;
    bool _is_loaded{false};
    bool _is_removed{false};
};

} // namespace kafka
