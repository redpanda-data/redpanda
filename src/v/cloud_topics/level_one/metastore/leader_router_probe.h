/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "metrics/metrics.h"
#include "utils/log_hist.h"

#include <seastar/core/metrics_registration.hh>

#include <memory>

namespace cloud_topics::l1 {

class leader_router_probe {
public:
    using hist_t = log_hist_internal;

    leader_router_probe() = default;
    leader_router_probe(const leader_router_probe&) = delete;
    leader_router_probe& operator=(const leader_router_probe&) = delete;
    leader_router_probe(leader_router_probe&&) = delete;
    leader_router_probe& operator=(leader_router_probe&&) = delete;
    ~leader_router_probe() = default;

    void setup_metrics();

    std::unique_ptr<hist_t::measurement> auto_measure_add_objects() {
        return _add_objects.auto_measure();
    }

    std::unique_ptr<hist_t::measurement> auto_measure_replace_objects() {
        return _replace_objects.auto_measure();
    }

    std::unique_ptr<hist_t::measurement> auto_measure_get_first_offset_ge() {
        return _get_first_offset_ge.auto_measure();
    }

    std::unique_ptr<hist_t::measurement> auto_measure_get_first_timestamp_ge() {
        return _get_first_timestamp_ge.auto_measure();
    }

    std::unique_ptr<hist_t::measurement>
    auto_measure_get_first_offset_for_bytes() {
        return _get_first_offset_for_bytes.auto_measure();
    }

    std::unique_ptr<hist_t::measurement> auto_measure_get_offsets() {
        return _get_offsets.auto_measure();
    }

    std::unique_ptr<hist_t::measurement> auto_measure_get_size() {
        return _get_size.auto_measure();
    }

    std::unique_ptr<hist_t::measurement> auto_measure_get_compaction_info() {
        return _get_compaction_info.auto_measure();
    }

    std::unique_ptr<hist_t::measurement> auto_measure_get_term_for_offset() {
        return _get_term_for_offset.auto_measure();
    }

    std::unique_ptr<hist_t::measurement>
    auto_measure_get_end_offset_for_term() {
        return _get_end_offset_for_term.auto_measure();
    }

    std::unique_ptr<hist_t::measurement> auto_measure_set_start_offset() {
        return _set_start_offset.auto_measure();
    }

    std::unique_ptr<hist_t::measurement> auto_measure_remove_topics() {
        return _remove_topics.auto_measure();
    }

    std::unique_ptr<hist_t::measurement> auto_measure_get_compaction_infos() {
        return _get_compaction_infos.auto_measure();
    }

    std::unique_ptr<hist_t::measurement> auto_measure_get_extent_metadata() {
        return _get_extent_metadata.auto_measure();
    }

    std::unique_ptr<hist_t::measurement> auto_measure_flush_domain() {
        return _flush_domain.auto_measure();
    }

    std::unique_ptr<hist_t::measurement> auto_measure_restore_domain() {
        return _restore_domain.auto_measure();
    }

    std::unique_ptr<hist_t::measurement> auto_measure_preregister_objects() {
        return _preregister_objects.auto_measure();
    }

private:
    hist_t _add_objects;
    hist_t _replace_objects;
    hist_t _get_first_offset_ge;
    hist_t _get_first_timestamp_ge;
    hist_t _get_first_offset_for_bytes;
    hist_t _get_offsets;
    hist_t _get_size;
    hist_t _get_compaction_info;
    hist_t _get_term_for_offset;
    hist_t _get_end_offset_for_term;
    hist_t _set_start_offset;
    hist_t _remove_topics;
    hist_t _get_compaction_infos;
    hist_t _get_extent_metadata;
    hist_t _flush_domain;
    hist_t _restore_domain;
    hist_t _preregister_objects;

    metrics::internal_metric_groups _metrics;
};

} // namespace cloud_topics::l1
