/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "datalake/coordinator/rpc_service.h"
#include "datalake/fwd.h"

namespace datalake::coordinator::rpc {
class service final : public impl::datalake_coordinator_rpc_service {
public:
    service(
      ss::scheduling_group, ss::smp_service_group, ss::sharded<frontend>*);

    ss::future<ensure_table_exists_reply> ensure_table_exists(
      ensure_table_exists_request, ::rpc::streaming_context&) override;

    ss::future<add_translated_data_files_reply> add_translated_data_files(
      add_translated_data_files_request, ::rpc::streaming_context&) override;

    ss::future<fetch_latest_translated_offset_reply>
    fetch_latest_translated_offset(
      fetch_latest_translated_offset_request,
      ::rpc::streaming_context&) override;

private:
    ss::sharded<frontend>* _frontend;
};
} // namespace datalake::coordinator::rpc
