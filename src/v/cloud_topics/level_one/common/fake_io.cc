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
#include "cloud_topics/level_one/common/object_id.h"

namespace cloud_topics::l1 {

fake_io::fake_io() = default;

// In-memory staging implementation for testing.
class fake_staging : public staging {
public:
    fake_staging() = default;
    fake_staging(const fake_staging&) = delete;
    fake_staging(fake_staging&&) = delete;
    fake_staging& operator=(const fake_staging&) = delete;
    fake_staging& operator=(fake_staging&&) = delete;
    ~fake_staging() override {
        vassert(_removed, "staging must be removed before destruction");
    }

    ss::future<size_t> size() override {
        vassert(!_removed, "cannot get size of removed staging");
        co_return _data.size_bytes();
    }
    ss::future<ss::output_stream<char>> output_stream() override {
        vassert(!_removed, "cannot get output stream of removed staging");
        co_return make_iobuf_ref_output_stream(_data);
    }

    ss::future<> remove() override {
        _removed = true;
        co_return;
    }

    ss::future<ss::input_stream<char>> input_stream() override {
        vassert(!_removed, "cannot get input stream of removed staging");
        co_return make_iobuf_input_stream(_data.share(0, _data.size_bytes()));
    }

private:
    bool _removed = false;
    iobuf _data;
};

ss::future<std::expected<std::unique_ptr<staging>, io::errc>>
fake_io::create_tmp_file() {
    std::unique_ptr<staging> stg = std::make_unique<fake_staging>();
    co_return stg;
}

ss::future<std::expected<void, io::errc>>
fake_io::put_object(object_id oid, staging* stg, ss::abort_source*) {
    auto stream = co_await io::read_staging(stg);
    auto size = co_await stg->size();
    auto data = co_await read_iobuf_exactly(stream, size);
    put_object(oid, std::move(data));
    co_return std::expected<void, io::errc>();
}

ss::future<std::expected<ss::input_stream<char>, io::errc>>
fake_io::read_object(object_extent extent, ss::abort_source*) {
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

} // namespace cloud_topics::l1
