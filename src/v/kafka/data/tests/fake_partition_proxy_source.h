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

#include "container/chunked_vector.h"
#include "kafka/data/partition_proxy_source.h"
#include "kafka/data/tests/fake_partition_proxy_impl.h"

#include <utility>
#include <vector>

namespace tests {

/// Test double for kafka::partition_proxy_source.
///
/// Backed by a list of (ktp, fake-data) pairs added via add(); produces a
/// fresh fake_partition_proxy_impl on each get() call so callers that
/// take ownership of the proxy don't deplete the source. Iteration order
/// of all_ktps() matches insertion order.
class fake_partition_proxy_source : public kafka::partition_proxy_source {
public:
    /// Canned per-partition values consumed by fake_partition_proxy_impl.
    struct entry {
        size_t local_size = 0;
        model::offset offset_lag{0};
        std::optional<size_t> cloud_size;
    };

    fake_partition_proxy_source& add(model::ktp ktp, entry e) {
        _entries.emplace_back(std::move(ktp), e);
        return *this;
    }

    chunked_vector<model::ktp> all_ktps() const override {
        chunked_vector<model::ktp> out;
        out.reserve(_entries.size());
        for (const auto& [k, _] : _entries) {
            out.emplace_back(k.get_topic(), k.get_partition());
        }
        return out;
    }

    std::optional<kafka::partition_proxy> get(const model::ktp& ktp) override {
        for (const auto& [k, e] : _entries) {
            if (
              k.get_topic() == ktp.get_topic()
              && k.get_partition() == ktp.get_partition()) {
                return kafka::partition_proxy(
                  std::make_unique<fake_partition_proxy_impl>(
                    ktp.to_ntp(), e.local_size, e.offset_lag, e.cloud_size));
            }
        }
        return std::nullopt;
    }

private:
    std::vector<std::pair<model::ktp, entry>> _entries;
};

} // namespace tests
