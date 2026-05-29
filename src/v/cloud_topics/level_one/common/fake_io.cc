/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/common/fake_io.h"

#include "bytes/iostream.h"
#include "cloud_storage_clients/multipart_upload.h"
#include "cloud_topics/level_one/common/object_id.h"

namespace cloud_topics::l1 {

static ss::logger fake_io_log("fake_io");

// In-memory multipart upload state for testing.
class fake_multipart_state final
  : public cloud_storage_clients::multipart_upload_state {
public:
    fake_multipart_state(
      object_id oid, absl::btree_map<object_id, iobuf>& storage)
      : _oid(oid)
      , _storage(storage) {}

    ss::future<> initialize_multipart() override { co_return; }

    ss::future<> upload_part(size_t, iobuf data) override {
        _buffer.append(std::move(data));
        co_return;
    }

    ss::future<> complete_multipart_upload() override {
        _storage.insert_or_assign(_oid, std::move(_buffer));
        co_return;
    }

    ss::future<> abort_multipart_upload() override {
        _buffer.clear();
        co_return;
    }

    ss::future<> upload_as_single_object(iobuf data) override {
        _storage.insert_or_assign(_oid, std::move(data));
        co_return;
    }

    bool is_multipart_initialized() const override { return true; }
    ss::sstring upload_id() const override { return "fake"; }

private:
    object_id _oid;
    absl::btree_map<object_id, iobuf>& _storage;
    iobuf _buffer;
};

fake_io::fake_io() = default;

class fake_file : public staging_file {
public:
    fake_file() = default;
    fake_file(const fake_file&) = delete;
    fake_file(fake_file&&) = delete;
    fake_file& operator=(const fake_file&) = delete;
    fake_file& operator=(fake_file&&) = delete;
    ~fake_file() override {
        vassert(_removed, "staging_file must be removed before destruction");
    }

    ss::future<size_t> size() override {
        vassert(!_removed, "cannot get size of a removed file");
        co_return _data.size_bytes();
    }
    ss::future<ss::output_stream<char>> output_stream() override {
        vassert(!_removed, "cannot get output stream of a removed file");
        co_return make_iobuf_ref_output_stream(_data);
    }

    ss::future<> remove() override {
        _removed = true;
        co_return;
    }

    ss::future<ss::input_stream<char>> input_stream() override {
        vassert(!_removed, "cannot get input stream of a removed file");
        co_return make_iobuf_input_stream(_data.share(0, _data.size_bytes()));
    }

private:
    bool _removed = false;
    iobuf _data;
};

ss::future<std::expected<std::unique_ptr<staging_file>, io::errc>>
fake_io::create_tmp_file() {
    std::unique_ptr<staging_file> file = std::make_unique<fake_file>();
    co_return file;
}

ss::future<std::expected<void, io::errc>>
fake_io::put_object(object_id oid, staging_file* file, ss::abort_source*) {
    auto stream = co_await io::read_file(file);
    auto size = co_await file->size();
    auto data = co_await read_iobuf_exactly(stream, size);
    put_object(oid, std::move(data));
    co_return std::expected<void, io::errc>();
}

ss::future<std::expected<ss::input_stream<char>, io::errc>>
fake_io::read_object(
  object_extent extent,
  ss::abort_source*,
  [[maybe_unused]] cloud_io::group_id gid) {
    co_return get_object(extent.id)
      .transform(
        [&extent](
          iobuf data) -> std::expected<ss::input_stream<char>, io::errc> {
            return make_iobuf_input_stream(
              data.share(extent.position, extent.size));
        })
      .value_or(std::unexpected(io::errc::cloud_missing_object));
}

ss::future<std::expected<void, io::errc>>
fake_io::delete_objects(chunked_vector<object_id> oids, ss::abort_source*) {
    for (const auto& oid : oids) {
        remove_object(oid);
    }
    co_return std::expected<void, io::errc>{};
}

std::optional<iobuf> fake_io::get_object(object_id id) {
    auto it = _storage.find(id);
    if (it == _storage.end()) {
        return std::nullopt;
    }
    return it->second.share(0, it->second.size_bytes());
}

void fake_io::put_object(object_id id, iobuf data) {
    _storage.insert_or_assign(id, std::move(data));
}

void fake_io::remove_object(object_id id) { _storage.erase(id); }

chunked_vector<object_id> fake_io::list_objects() const {
    chunked_vector<object_id> oids;
    for (const auto& [oid, _] : _storage) {
        oids.emplace_back(oid);
    }
    return oids;
}

ss::future<std::expected<cloud_storage_clients::multipart_upload_ref, io::errc>>
fake_io::create_multipart_upload(
  object_id oid, size_t part_size, ss::abort_source*) {
    auto state = ss::make_shared<fake_multipart_state>(oid, _storage);
    auto upload = ss::make_shared<cloud_storage_clients::multipart_upload>(
      std::move(state), part_size, fake_io_log);
    co_return upload;
}

} // namespace cloud_topics::l1
