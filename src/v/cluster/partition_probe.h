/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "model/fundamental.h"

#include <seastar/core/metrics_registration.hh>
#include <seastar/core/shared_ptr.hh>

#include <cstdint>

namespace cluster {

class partition;

class partition_probe {
public:
    struct impl {
        virtual void add_records_produced(uint64_t) = 0;
        virtual void add_records_fetched(uint64_t) = 0;
        virtual void add_bytes_produced(uint64_t) = 0;
        virtual void add_bytes_fetched(uint64_t) = 0;
        virtual void setup_metrics(const model::ntp&) = 0;
        virtual ~impl() noexcept = default;
    };

    explicit partition_probe(std::unique_ptr<impl> impl)
      : _impl(std::move(impl)) {}

    void setup_metrics(const model::ntp& ntp) {
        return _impl->setup_metrics(ntp);
    };

    void add_records_produced(uint64_t num_records) {
        return _impl->add_records_produced(num_records);
    }

    void add_records_fetched(uint64_t num_records) {
        return _impl->add_records_fetched(num_records);
    }
    void add_bytes_produced(uint64_t bytes) {
        return _impl->add_bytes_produced(bytes);
    }

    void add_bytes_fetched(uint64_t bytes) {
        return _impl->add_bytes_fetched(bytes);
    }

private:
    std::unique_ptr<impl> _impl;
};
class replicated_partition_probe : public partition_probe::impl {
public:
    explicit replicated_partition_probe(const partition&) noexcept;

    void setup_metrics(const model::ntp&) final;

    void add_records_fetched(uint64_t cnt) final { _records_fetched += cnt; }
    void add_records_produced(uint64_t cnt) final { _records_produced += cnt; }
    void add_bytes_fetched(uint64_t cnt) final { _bytes_fetched += cnt; }
    void add_bytes_produced(uint64_t cnt) final { _bytes_produced += cnt; }

private:
    void setup_public_metrics(const model::ntp&);
    void setup_internal_metrics(const model::ntp&);

private:
    const partition& _partition;
    uint64_t _records_produced{0};
    uint64_t _records_fetched{0};
    uint64_t _bytes_produced{0};
    uint64_t _bytes_fetched{0};
    ss::metrics::metric_groups _metrics;
    ss::metrics::metric_groups _public_metrics;
};

partition_probe make_materialized_partition_probe();
} // namespace cluster
