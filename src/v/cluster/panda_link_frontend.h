/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"
#include "cluster/commands.h"
#include "cluster/panda_link_table.h"
#include "rpc/fwd.h"
#include "utils/mutex.h"

#include <seastar/core/sharded.hh>

namespace cluster {
class panda_link_frontend
  : public ss::peering_sharded_service<panda_link_frontend> {
    using panda_link_cmd
      = std::variant<panda_link_upsert_cmd, panda_link_remove_cmd>;

public:
    panda_link_frontend(
      model::node_id,
      partition_leaders_table*,
      panda_link_table*,
      controller_stm*,
      rpc::connection_cache*,
      ss::abort_source*);

    using notification_id = panda_link_table::notification_id;
    using notification_callback = panda_link_table::notification_callback;

    struct mutation_result {
        errc ec;
    };

    ss::future<mutation_result> upsert_panda_link(
      model::panda_link_metadata, model::timeout_clock::time_point);

    ss::future<mutation_result> remove_panda_link(
      model::panda_link_name, model::timeout_clock::time_point);

    notification_id register_for_updates(notification_callback);
    void unregister_for_updates(notification_id);

    std::optional<model::panda_link_metadata>
    lookup_panda_link(const model::panda_link_name&) const;
    std::optional<model::panda_link_metadata>
      lookup_panda_link(model::panda_link_id) const;

private:
    ss::future<mutation_result>
      do_mutation(panda_link_cmd, model::timeout_clock::time_point);
    ss::future<mutation_result> dispatch_mutation_to_remote(
      model::node_id, panda_link_cmd, model::timeout_clock::duration);
    ss::future<mutation_result>
      do_local_mutation(panda_link_cmd, model::timeout_clock::time_point);

    errc validate_mutation(const panda_link_cmd&);

public:
    class validator {
    public:
        explicit validator(panda_link_table*);

        errc validate_mutation(const panda_link_cmd&);

    private:
        panda_link_table* _table;
    };

private:
    model::node_id _self;
    partition_leaders_table* _leaders;
    rpc::connection_cache* _connections;
    panda_link_table* _table;
    ss::abort_source* _as;

    controller_stm* _controller;

    mutex _mu{"panda_link_frontend::mu"};
};
} // namespace cluster
