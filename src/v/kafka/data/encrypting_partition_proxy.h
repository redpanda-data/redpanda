/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "kafka/data/forwarding_partition_proxy.h"

#include <memory>

namespace encryption {
class schema_resolver;
class dek_manager;
class field_transformer;
} // namespace encryption

namespace kafka {

/// Decorator that intercepts replicate() to encrypt tagged fields before
/// delegating to the inner partition proxy.
class encrypting_partition_proxy final
  : public forwarding_partition_proxy_impl {
public:
    encrypting_partition_proxy(
      std::unique_ptr<partition_proxy::impl> inner,
      encryption::schema_resolver& resolver,
      encryption::dek_manager& dek_mgr,
      encryption::field_transformer& transformer);

    ss::future<result<model::offset>> replicate(
      chunked_vector<model::record_batch> batches,
      raft::replicate_options opts) final;

    raft::replicate_stages replicate(
      model::batch_identity bid,
      model::record_batch batch,
      raft::replicate_options opts) final;

    ss::future<storage::translating_reader>
    make_reader(kafka::log_reader_config cfg) final;

    std::unique_ptr<exact_offset_replicator> make_exact_offset_replicator()
      && final;

private:
    ss::future<model::record_batch> encrypt_batch(model::record_batch batch);

    encryption::schema_resolver& _resolver;
    encryption::dek_manager& _dek_mgr;
    encryption::field_transformer& _transformer;
};

} // namespace kafka
