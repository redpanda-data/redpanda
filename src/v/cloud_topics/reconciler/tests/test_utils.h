/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/level_one/common/fake_io.h"
#include "cloud_topics/level_one/metastore/simple_metastore.h"
#include "cloud_topics/reconciler/reconciliation_source.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"

#include <seastar/core/future.hh>

#include <expected>
#include <optional>
#include <stdexcept>

namespace cloud_topics::reconciler::test {

class fake_source : public source {
public:
    fake_source(model::ntp ntp, model::topic_id_partition tidp)
      : source(std::move(ntp), tidp) {}

    void add_batch(
      model::test::record_batch_spec spec,
      std::optional<model::term_id> term = std::nullopt) {
        if (!_source_log.empty()) {
            spec.offset = _source_log.back().last_offset() + model::offset{1};
        }
        auto batch = model::test::make_random_batch(spec);
        if (term.has_value()) {
            batch.set_term(term.value());
        }
        _source_log.push_back(std::move(batch));
    }

    uint64_t get_backlog_size_estimate() const override {
        uint64_t size = 0;
        for (const auto& batch : _source_log) {
            if (model::offset_cast(batch.base_offset()) < _lro) {
                continue;
            }
            size += batch.size_bytes();
        }
        return size;
    }

    kafka::offset last_reconciled_offset() override { return _lro; }

    ss::future<std::expected<void, errc>>
    set_last_reconciled_offset(kafka::offset o, ss::abort_source&) override {
        if (_fail_set_lro) {
            co_return std::unexpected(errc::failure);
        }
        _lro = o;
        co_return std::expected<void, errc>{};
    }

    ss::future<model::record_batch_reader>
    make_reader(source::reader_config cfg) override {
        if (_fail_make_reader) {
            throw std::runtime_error("Failed to make reader");
        }
        chunked_vector<model::record_batch> log;
        size_t size = 0;
        for (const auto& batch : _source_log) {
            if (
              model::offset_cast(batch.base_offset())
              < last_reconciled_offset()) {
                continue;
            }
            size += batch.size_bytes();
            log.push_back(batch.copy());
            if (size > cfg.max_bytes) {
                break;
            }
        }
        co_return model::make_chunked_memory_record_batch_reader(
          std::move(log));
    }

    void fail_set_lro(bool fail) { _fail_set_lro = fail; }
    void fail_make_reader(bool fail) { _fail_make_reader = fail; }

private:
    kafka::offset _lro;
    chunked_vector<model::record_batch> _source_log;
    bool _fail_set_lro = false;
    bool _fail_make_reader = false;
};

class unreliable_metastore : public l1::simple_metastore {
public:
    ss::future<std::expected<l1::metastore::add_response, l1::metastore::errc>>
    add_objects(
      const l1::metastore::object_metadata_builder& builder,
      const l1::metastore::term_offset_map_t& terms) override {
        if (_fail_add_objects) {
            co_return std::unexpected(l1::metastore::errc::invalid_request);
        }
        if (_fail_add_objects_transiently_count > 0) {
            _fail_add_objects_transiently_count--;
            co_return std::unexpected(l1::metastore::errc::transport_error);
        }
        co_return co_await l1::simple_metastore::add_objects(builder, terms);
    }

    void fail_add_objects(bool fail) { _fail_add_objects = fail; }
    void fail_add_objects_transiently(int times) {
        _fail_add_objects_transiently_count = times;
    }

private:
    bool _fail_add_objects = false;
    int _fail_add_objects_transiently_count = 0;
};

class unreliable_io : public l1::fake_io {
public:
    ss::future<std::expected<std::unique_ptr<l1::staging_file>, l1::io::errc>>
    create_tmp_file() override {
        if (_fail_create_tmp_file) {
            co_return std::unexpected(l1::io::errc::file_io_error);
        }
        co_return co_await l1::fake_io::create_tmp_file();
    }

    ss::future<std::expected<void, l1::io::errc>> put_object(
      l1::object_id oid,
      l1::staging_file* file,
      ss::abort_source* as) override {
        if (_fail_put_object) {
            co_return std::unexpected(l1::io::errc::cloud_op_error);
        }
        co_return co_await l1::fake_io::put_object(oid, file, as);
    }

    void fail_create_tmp_file(bool fail) { _fail_create_tmp_file = fail; }
    void fail_put_object(bool fail) { _fail_put_object = fail; }

private:
    bool _fail_create_tmp_file = false;
    bool _fail_put_object = false;
};

} // namespace cloud_topics::reconciler::test
