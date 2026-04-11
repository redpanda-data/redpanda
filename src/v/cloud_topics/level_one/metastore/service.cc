/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/metastore/service.h"

#include "cloud_topics/level_one/metastore/leader_router.h"

namespace cloud_topics::l1::rpc {

service::service(
  ss::scheduling_group sg,
  ss::smp_service_group smp_sg,
  ss::sharded<leader_router>* leader_router)
  : impl::l1_rpc_service(sg, smp_sg)
  , _leader_router(leader_router) {}

ss::future<add_objects_reply>
service::add_objects(add_objects_request request, ::rpc::streaming_context&) {
    return _leader_router->local().add_objects(
      std::move(request), leader_router::local_only::yes);
}

ss::future<replace_objects_reply> service::replace_objects(
  replace_objects_request request, ::rpc::streaming_context&) {
    return _leader_router->local().replace_objects(
      std::move(request), leader_router::local_only::yes);
}

ss::future<set_start_offset_reply> service::set_start_offset(
  set_start_offset_request request, ::rpc::streaming_context&) {
    return _leader_router->local().set_start_offset(
      std::move(request), leader_router::local_only::yes);
}

ss::future<remove_topics_reply> service::remove_topics(
  remove_topics_request request, ::rpc::streaming_context&) {
    return _leader_router->local().remove_topics(
      std::move(request), leader_router::local_only::yes);
}

ss::future<get_first_offset_ge_reply> service::get_first_offset_ge(
  get_first_offset_ge_request request, ::rpc::streaming_context&) {
    return _leader_router->local().get_first_offset_ge(
      std::move(request), leader_router::local_only::yes);
}

ss::future<get_first_timestamp_ge_reply> service::get_first_timestamp_ge(
  get_first_timestamp_ge_request request, ::rpc::streaming_context&) {
    return _leader_router->local().get_first_timestamp_ge(
      std::move(request), leader_router::local_only::yes);
}

ss::future<get_first_offset_for_bytes_reply>
service::get_first_offset_for_bytes(
  get_first_offset_for_bytes_request request, ::rpc::streaming_context&) {
    return _leader_router->local().get_first_offset_for_bytes(
      std::move(request), leader_router::local_only::yes);
}

ss::future<get_offsets_reply>
service::get_offsets(get_offsets_request request, ::rpc::streaming_context&) {
    return _leader_router->local().get_offsets(
      std::move(request), leader_router::local_only::yes);
}

ss::future<get_size_reply>
service::get_size(get_size_request request, ::rpc::streaming_context&) {
    return _leader_router->local().get_size(
      std::move(request), leader_router::local_only::yes);
}

ss::future<get_end_offset_for_term_reply> service::get_end_offset_for_term(
  get_end_offset_for_term_request request, ::rpc::streaming_context&) {
    return _leader_router->local().get_end_offset_for_term(
      std::move(request), leader_router::local_only::yes);
}

ss::future<get_term_for_offset_reply> service::get_term_for_offset(
  get_term_for_offset_request request, ::rpc::streaming_context&) {
    return _leader_router->local().get_term_for_offset(
      std::move(request), leader_router::local_only::yes);
}

ss::future<get_compaction_info_reply> service::get_compaction_info(
  get_compaction_info_request request, ::rpc::streaming_context&) {
    return _leader_router->local().get_compaction_info(
      std::move(request), leader_router::local_only::yes);
}

ss::future<get_compaction_infos_reply> service::get_compaction_infos(
  get_compaction_infos_request request, ::rpc::streaming_context&) {
    return _leader_router->local().get_compaction_infos(
      std::move(request), leader_router::local_only::yes);
}

ss::future<get_extent_metadata_reply> service::get_extent_metadata(
  get_extent_metadata_request request, ::rpc::streaming_context&) {
    return _leader_router->local().get_extent_metadata(
      std::move(request), leader_router::local_only::yes);
}

ss::future<flush_domain_reply>
service::flush_domain(flush_domain_request request, ::rpc::streaming_context&) {
    return _leader_router->local().flush_domain(
      std::move(request), leader_router::local_only::yes);
}

ss::future<restore_domain_reply> service::restore_domain(
  restore_domain_request request, ::rpc::streaming_context&) {
    return _leader_router->local().restore_domain(
      std::move(request), leader_router::local_only::yes);
}

ss::future<preregister_objects_reply> service::preregister_objects(
  preregister_objects_request request, ::rpc::streaming_context&) {
    return _leader_router->local().preregister_objects(
      std::move(request), leader_router::local_only::yes);
}

} // namespace cloud_topics::l1::rpc
