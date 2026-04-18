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

#include "encryption/schema_resolver.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>

#include <cstdint>
#include <memory>

namespace encryption {

class dek_manager;
class field_transformer;
class kms_provider;
struct encryption_services;

/// Per-shard service that owns all encryption components and exposes them
/// as a bundle via get_encryption_services(). Designed to be held in
/// ss::sharded<encryption_service> and started via invoke_on_all.
class encryption_service
  : public ss::peering_sharded_service<encryption_service> {
public:
    encryption_service();
    ~encryption_service() noexcept;

    /// Start the service. If kms_type is empty encryption is disabled.
    /// \param fetcher callback that returns schema text for a subject,
    ///                nullptr if schema registry is unavailable
    /// \param kms_type the KMS provider type ("mock", etc.)
    /// \param default_kms_key_id default key ID passed to the resolver
    /// \param dek_algorithm algorithm name (reserved for future use)
    /// \param dek_expiry_seconds DEK expiry (reserved for future use)
    ss::future<> start(
      schema_fetcher fetcher,
      ss::sstring kms_type,
      ss::sstring default_kms_key_id,
      ss::sstring dek_algorithm,
      int32_t dek_expiry_seconds);

    ss::future<> stop();

    /// Returns null if encryption is disabled.
    encryption_services* get_encryption_services();

    bool is_enabled() const;

private:
    std::unique_ptr<kms_provider> _kms;
    std::unique_ptr<dek_manager> _dek_mgr;
    std::unique_ptr<schema_resolver> _resolver;
    std::unique_ptr<field_transformer> _transformer;
    std::unique_ptr<encryption_services> _services;
};

} // namespace encryption
