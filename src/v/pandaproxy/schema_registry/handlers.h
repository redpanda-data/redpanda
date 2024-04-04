/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"
#include "pandaproxy/schema_registry/service.h"
#include "pandaproxy/server.h"

#include <seastar/core/future.hh>

namespace pandaproxy::schema_registry {

ss::future<ctx_server<service>::reply_t>
get_config(ctx_server<service>::request_t rq, ctx_server<service>::reply_t rp);

ss::future<ctx_server<service>::reply_t>
put_config(ctx_server<service>::request_t rq, ctx_server<service>::reply_t rp);

ss::future<ctx_server<service>::reply_t> get_config_subject(
  ctx_server<service>::request_t rq, ctx_server<service>::reply_t rp);

ss::future<ctx_server<service>::reply_t> put_config_subject(
  ctx_server<service>::request_t rq, ctx_server<service>::reply_t rp);

ss::future<ctx_server<service>::reply_t> delete_config_subject(
  ctx_server<service>::request_t rq, ctx_server<service>::reply_t rp);

ss::future<ctx_server<service>::reply_t>
get_mode(ctx_server<service>::request_t rq, ctx_server<service>::reply_t rp);

ss::future<ctx_server<service>::reply_t>
put_mode(ctx_server<service>::request_t rq, ctx_server<service>::reply_t rp);

ss::future<ctx_server<service>::reply_t> get_mode_subject(
  ctx_server<service>::request_t rq, ctx_server<service>::reply_t rp);

ss::future<ctx_server<service>::reply_t> put_mode_subject(
  ctx_server<service>::request_t rq, ctx_server<service>::reply_t rp);

ss::future<ctx_server<service>::reply_t> delete_mode_subject(
  ctx_server<service>::request_t rq, ctx_server<service>::reply_t rp);

ss::future<ctx_server<service>::reply_t> get_schemas_types(
  ctx_server<service>::request_t rq, ctx_server<service>::reply_t rp);

ss::future<ctx_server<service>::reply_t> get_schemas_ids_id(
  ctx_server<service>::request_t rq, ctx_server<service>::reply_t rp);

ss::future<ctx_server<service>::reply_t> get_schemas_ids_id_versions(
  ctx_server<service>::request_t rq, ctx_server<service>::reply_t rp);

ss::future<ctx_server<service>::reply_t> get_schemas_ids_id_subjects(
  ctx_server<service>::request_t rq, ctx_server<service>::reply_t rp);

ss::future<ctx_server<service>::reply_t> get_subjects(
  ctx_server<service>::request_t rq, ctx_server<service>::reply_t rp);

ss::future<ctx_server<service>::reply_t> get_subject_versions(
  ctx_server<service>::request_t rq, ctx_server<service>::reply_t rp);

ss::future<ctx_server<service>::reply_t> post_subject(
  ctx_server<service>::request_t rq, ctx_server<service>::reply_t rp);

ss::future<ctx_server<service>::reply_t> post_subject_versions(
  ctx_server<service>::request_t rq, ctx_server<service>::reply_t rp);

ss::future<ctx_server<service>::reply_t> get_subject_versions_version(
  ctx_server<service>::request_t rq, ctx_server<service>::reply_t rp);

ss::future<ctx_server<service>::reply_t> get_subject_versions_version_schema(
  ctx_server<service>::request_t rq, ctx_server<service>::reply_t rp);

ss::future<ctx_server<service>::reply_t>
get_subject_versions_version_referenced_by(
  ctx_server<service>::request_t rq, ctx_server<service>::reply_t rp);

ss::future<ctx_server<service>::reply_t> delete_subject(
  ctx_server<service>::request_t rq, ctx_server<service>::reply_t rp);

ss::future<ctx_server<service>::reply_t> delete_subject_version(
  ctx_server<service>::request_t rq, ctx_server<service>::reply_t rp);

ss::future<ctx_server<service>::reply_t> compatibility_subject_version(
  ctx_server<service>::request_t rq, ctx_server<service>::reply_t rp);

ss::future<ctx_server<service>::reply_t> status_ready(
  ctx_server<service>::request_t rq, ctx_server<service>::reply_t rp);

} // namespace pandaproxy::schema_registry
