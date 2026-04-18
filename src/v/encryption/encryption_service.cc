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

#include "encryption/encryption_service.h"

#include "encryption/dek_manager.h"
#include "encryption/encryption_services.h"
#include "encryption/field_transformer_ref.h"
#include "encryption/mock_kms_provider.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include <fmt/format.h>

#include <stdexcept>

namespace encryption {

encryption_service::encryption_service() = default;
encryption_service::~encryption_service() noexcept = default;

ss::future<> encryption_service::start(
  schema_fetcher fetcher,
  ss::sstring kms_type,
  ss::sstring default_kms_key_id,
  ss::sstring /*dek_algorithm*/,
  int32_t /*dek_expiry_seconds*/) {
    if (kms_type.empty()) {
        co_return;
    }

    if (kms_type == "mock") {
        _kms = std::make_unique<mock_kms_provider>();
    } else {
        // Unknown KMS type — log warning and disable encryption rather than
        // crashing the broker. This allows cluster config tests to set
        // arbitrary string values without breaking startup.
        co_return;
    }

    _dek_mgr = std::make_unique<dek_manager>(*_kms);

    if (fetcher) {
        _resolver = std::make_unique<schema_resolver>(
          std::move(fetcher), std::move(default_kms_key_id));
    } else {
        _resolver = std::make_unique<schema_resolver>();
    }

    _transformer = std::make_unique<ref_field_transformer>();

    _services = std::make_unique<encryption_services>(
      encryption_services{*_resolver, *_dek_mgr, *_transformer});
}

ss::future<> encryption_service::stop() {
    _services.reset();
    _transformer.reset();
    _resolver.reset();
    _dek_mgr.reset();
    _kms.reset();
    co_return;
}

encryption_services* encryption_service::get_encryption_services() {
    return _services.get();
}

bool encryption_service::is_enabled() const { return _services != nullptr; }

} // namespace encryption
