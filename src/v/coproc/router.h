/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once
#include "coproc/errc.h"
#include "coproc/logger.h"
#include "coproc/supervisor.h"
#include "coproc/types.h"
#include "model/fundamental.h"
#include "model/limits.h"
#include "model/metadata.h"
#include "rpc/reconnect_transport.h"
#include "rpc/types.h"
#include "storage/api.h"
#include "storage/ntp_config.h"
#include "storage/types.h"
#include "utils/mutex.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/socket_defs.hh>

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

namespace coproc {
/// Reads data from registered input topics and routes them to the coprocessor
/// engine connected locally. This is done by polling the registered ntps in a
/// loop. Offsets are managed for each coprocessor/input topic so materialized
/// topics can resume upon last processed record in the case of a failure.
class router {
public:
    router(ss::socket_address, ss::sharded<storage::api>&);

    /// Begin the loop on the current shard
    ss::future<> start() {
        _loop_timer.set_callback([this] { (void)route(); });
        _loop_timer.arm(_jitter());
        return ss::now();
    }

    /// Shut down the loop on the current shard
    ss::future<> stop() {
        _loop_timer.cancel();
        _abort_source.request_abort();
        return _gate.close().then([this] { return _transport.stop(); });
    }

    errc add_source(
      const script_id, const model::topic_namespace&, topic_ingestion_policy);
    bool remove_source(const script_id sid);
    bool script_id_exists(const script_id sid) const;
    bool ntp_exists(const model::ntp& ntp) const {
        return _sources.find(ntp) != _sources.cend();
    }

private:
    using offset_rbr_pair
      = std::pair<model::offset, model::record_batch_reader>;
    using opt_req_data = std::optional<process_batch_request::data>;
    using opt_cfg = std::optional<storage::log_reader_config>;

    struct topic_offsets {
        model::offset committed{model::model_limits<model::offset>::min()};
        model::offset dirty{model::model_limits<model::offset>::min()};
        mutex mtx;
    };

    struct topic_state {
        /// For now the only possible topic_ingestion_policy is latest
        storage::log log;
        topic_offsets head;
        absl::flat_hash_set<script_id> scripts;
    };

    ss::future<result<supervisor_client_protocol>> get_client();
    ss::future<storage::log> get_log(const model::ntp& ntp);

    ss::future<> process_reply(process_batch_reply);
    ss::future<> process_reply_one(process_batch_reply::data);

    ss::future<> route();
    ss::future<> do_route();
    ss::future<opt_req_data> route_ntp(const model::ntp&, topic_state&);
    ss::future<> process_batch(std::vector<process_batch_request::data>);
    ss::future<> send_batch(supervisor_client_protocol, process_batch_request);

    ss::future<std::optional<offset_rbr_pair>>
      extract_offset(model::record_batch_reader);
    void bump_offset(const model::ntp&, const script_id);

    ss::future<opt_cfg> make_reader_cfg(storage::log, topic_offsets&);
    storage::log_reader_config reader_cfg(model::offset, model::offset);

private:
    /// Handle to the storage layer. Used to grab the storage::log for the
    /// desired ntp to be tracked
    ss::sharded<storage::api>& _api;

    /// Primitives used to manage the poll loop and close gracefully
    ss::gate _gate;
    ss::abort_source _abort_source;
    uint8_t _connection_attempts{0};
    simple_time_jitter<ss::lowres_clock> _jitter;
    ss::timer<ss::lowres_clock> _loop_timer;

    /// Core in-memory data structure that manages the relationships between
    /// topics and coprocessor scripts
    absl::flat_hash_map<model::ntp, topic_state> _sources;

    /// Connection to the coprocessor engine
    rpc::reconnect_transport _transport;
};

} // namespace coproc
