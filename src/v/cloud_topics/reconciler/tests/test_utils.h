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

#include "cloud_storage_clients/multipart_upload.h"
#include "cloud_topics/level_one/common/fake_io.h"
#include "cloud_topics/level_one/metastore/simple_metastore.h"
#include "cloud_topics/reconciler/reconciliation_source.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"

#include <seastar/core/future.hh>
#include <seastar/util/log.hh>

#include <expected>
#include <functional>
#include <optional>
#include <stdexcept>

namespace cloud_topics::reconciler::test {

static ss::logger test_log("reconciler_test");

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

    bool has_pending_data() override { return true; }

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
        if (_on_make_reader) {
            _on_make_reader();
        }
        if (_fail_make_reader) {
            throw std::runtime_error("Failed to make reader");
        }
        chunked_vector<model::record_batch> log;
        size_t size = 0;
        for (const auto& batch : _source_log) {
            if (model::offset_cast(batch.base_offset()) < cfg.start_offset) {
                continue;
            }
            if (batch.header().attrs.is_control()) {
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
    void set_on_make_reader(std::function<void()> cb) {
        _on_make_reader = std::move(cb);
    }

private:
    kafka::offset _lro;
    chunked_vector<model::record_batch> _source_log;
    bool _fail_set_lro = false;
    bool _fail_make_reader = false;
    std::function<void()> _on_make_reader;
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

// A multipart upload state that delegates to fake_io's storage
// but can inject failures at each phase of the multipart protocol.
class unreliable_multipart_state final
  : public cloud_storage_clients::multipart_upload_state {
public:
    unreliable_multipart_state(
      l1::object_id oid,
      l1::fake_io& storage,
      bool& fail_upload_part,
      bool& fail_complete,
      bool& fail_abort)
      : _oid(oid)
      , _storage(storage)
      , _fail_upload_part(fail_upload_part)
      , _fail_complete(fail_complete)
      , _fail_abort(fail_abort) {}

    ss::future<> initialize_multipart() override { co_return; }

    ss::future<> upload_part(size_t, iobuf data) override {
        if (_fail_upload_part) {
            throw std::runtime_error("Injected upload_part failure");
        }
        _buffer.append(std::move(data));
        co_return;
    }

    ss::future<> complete_multipart_upload() override {
        if (_fail_complete) {
            throw std::runtime_error("Injected complete_multipart failure");
        }
        _storage.put_object(_oid, std::move(_buffer));
        co_return;
    }

    ss::future<> abort_multipart_upload() override {
        if (_fail_abort) {
            throw std::runtime_error("Injected abort_multipart failure");
        }
        _buffer.clear();
        co_return;
    }

    ss::future<> upload_as_single_object(iobuf data) override {
        if (_fail_complete) {
            throw std::runtime_error(
              "Injected upload_as_single_object failure");
        }
        _storage.put_object(_oid, std::move(data));
        co_return;
    }

    bool is_multipart_initialized() const override { return true; }
    ss::sstring upload_id() const override { return "unreliable"; }

private:
    l1::object_id _oid;
    l1::fake_io& _storage;
    bool& _fail_upload_part;
    bool& _fail_complete;
    bool& _fail_abort;
    iobuf _buffer;
};

class unreliable_io : public l1::fake_io {
public:
    ss::future<
      std::expected<cloud_storage_clients::multipart_upload_ref, l1::io::errc>>
    create_multipart_upload(
      l1::object_id oid, size_t part_size, ss::abort_source*) override {
        if (_fail_create_multipart) {
            co_return std::unexpected(l1::io::errc::cloud_op_error);
        }
        auto state = ss::make_shared<unreliable_multipart_state>(
          oid, *this, _fail_upload_part, _fail_complete, _fail_abort);
        auto upload = ss::make_shared<cloud_storage_clients::multipart_upload>(
          std::move(state), part_size, test_log);
        co_return upload;
    }

    void fail_create_multipart(bool fail) { _fail_create_multipart = fail; }
    void fail_upload_part(bool fail) { _fail_upload_part = fail; }
    void fail_complete(bool fail) { _fail_complete = fail; }
    void fail_abort(bool fail) { _fail_abort = fail; }

private:
    bool _fail_create_multipart = false;
    bool _fail_upload_part = false;
    bool _fail_complete = false;
    bool _fail_abort = false;
};

} // namespace cloud_topics::reconciler::test
