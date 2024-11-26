/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/coordinator/service.h"

#include "datalake/coordinator/frontend.h"

namespace datalake::coordinator::rpc {

service::service(
  ss::scheduling_group sg,
  ss::smp_service_group smp_sg,
  ss::sharded<frontend>* frontend)
  : impl::datalake_coordinator_rpc_service(sg, smp_sg)
  , _frontend(frontend) {}

ss::future<ensure_table_exists_reply> service::ensure_table_exists(
  ensure_table_exists_request request, ::rpc::streaming_context&) {
    return _frontend->local().ensure_table_exists(
      std::move(request), frontend::local_only::yes);
}

ss::future<add_translated_data_files_reply> service::add_translated_data_files(
  add_translated_data_files_request request, ::rpc::streaming_context&) {
    return _frontend->local().add_translated_data_files(
      std::move(request), frontend::local_only::yes);
}

ss::future<fetch_latest_translated_offset_reply>
service::fetch_latest_translated_offset(
  fetch_latest_translated_offset_request request, ::rpc::streaming_context&) {
    return _frontend->local().fetch_latest_translated_offset(
      std::move(request), frontend::local_only::yes);
}

}; // namespace datalake::coordinator::rpc
