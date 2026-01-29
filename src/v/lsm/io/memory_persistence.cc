/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "lsm/io/memory_persistence.h"

#include "base/format_to.h"
#include "lsm/core/exceptions.h"
#include "lsm/core/internal/files.h"

#include <seastar/core/coroutine.hh>

#include <map>
#include <memory>

namespace lsm::io {

namespace {

struct memory_file_state {
    internal::file_handle file_handle;
    iobuf data;
    int32_t open_read_handles = 0;
    int32_t open_write_handles = 0;

    int32_t open_handles() const {
        return open_read_handles + open_write_handles;
    }
};

class memory_random_access_file_reader : public random_access_file_reader {
public:
    explicit memory_random_access_file_reader(
      ss::lw_shared_ptr<memory_file_state> state)
      : _state(std::move(state)) {
        if (_state->open_write_handles > 0) {
            throw io_error_exception(
              "unable to open new readable file with open write handles: {}",
              _state->open_handles());
        }
        ++_state->open_read_handles;
    }

    ~memory_random_access_file_reader() override {
        vassert(_closed, "files must be closed before destructing");
    }

    ss::future<ioarray> read(size_t offset, size_t n) override {
        if ((offset + n) > _state->data.size_bytes()) {
            throw io_error_exception(
              "tried to read out of bounds of the file: "
              "{{offset:{},length:{},file_size:{}}}",
              offset,
              n,
              _state->data.size_bytes());
        }
        co_return ioarray::copy_from(_state->data.share(offset, n));
    }

    ss::future<> close() override {
        if (_closed) {
            throw io_error_exception("double close of file");
        }
        _closed = true;
        --_state->open_read_handles;
        co_return;
    }

    fmt::iterator format_to(fmt::iterator it) const override {
        return fmt::format_to(
          it,
          "{{file={}, size={}}}",
          _state->file_handle,
          _state->data.size_bytes());
    }

private:
    bool _closed = false;
    ss::lw_shared_ptr<memory_file_state> _state;
};

class memory_sequential_file_writer : public sequential_file_writer {
public:
    explicit memory_sequential_file_writer(
      ss::lw_shared_ptr<memory_file_state> state)
      : _state(std::move(state)) {
        if (_state->open_handles() > 0) {
            throw io_error_exception(
              "unable to open new writable file with open handles: {}",
              _state->open_handles());
        }
        ++_state->open_write_handles;
        _state->data.clear();
    }
    ~memory_sequential_file_writer() override {
        vassert(_closed, "files must be closed before destructing");
    }
    ss::future<> append(iobuf buf) override {
        _state->data.append(std::move(buf));
        co_return;
    }
    ss::future<> close() override {
        if (_closed) {
            throw io_error_exception("double close of file");
        }
        _closed = true;
        --_state->open_write_handles;
        co_return;
    }

    fmt::iterator format_to(fmt::iterator it) const override {
        return fmt::format_to(it, "{{file={}}}", _state->file_handle);
    }

private:
    bool _closed = false;
    ss::lw_shared_ptr<memory_file_state> _state;
};

class data_impl : public data_persistence {
public:
    explicit data_impl(memory_persistence_controller* controller)
      : _controller(controller) {}

    ss::future<optional_pointer<random_access_file_reader>>
    open_random_access_reader(internal::file_handle h) override {
        if (_controller && _controller->should_fail) {
            throw io_error_exception("injected error");
        }
        auto it = _data.find(h);
        std::unique_ptr<random_access_file_reader> ptr;
        if (it != _data.end()) {
            ptr = std::make_unique<memory_random_access_file_reader>(
              it->second);
        }
        co_return ptr;
    }

    ss::future<std::unique_ptr<sequential_file_writer>>
    open_sequential_writer(internal::file_handle h) override {
        if (_controller && _controller->should_fail) {
            throw io_error_exception("injected error");
        }
        auto it = _data.try_emplace(
          h, ss::make_lw_shared<memory_file_state>(h));
        co_return std::make_unique<memory_sequential_file_writer>(
          it.first->second);
    }

    ss::future<> remove_file(internal::file_handle h) override {
        if (_controller && _controller->should_fail) {
            throw io_error_exception("injected error");
        }
        auto it = _data.find(h);
        if (it == _data.end()) {
            co_return;
        }
        if (it->second->open_handles() != 0) {
            throw io_error_exception(
              "unable to remove file {}, there are still open handles", h);
        }
        _data.erase(it);
    }

    ss::coroutine::experimental::generator<internal::file_handle>
    list_files() override {
        if (_controller && _controller->should_fail) {
            throw io_error_exception("injected error");
        }
        auto it = _data.begin();
        while (it != _data.end()) {
            auto key = it->first;
            co_yield key;
            it = _data.upper_bound(key);
        }
    }

    ss::future<> close() override {
        _closed = true;
        for (const auto& [file, state] : _data) {
            vassert(
              state->open_handles() == 0,
              "tried to close with open handles on file {}",
              file);
        }
        co_return;
    }

    ~data_impl() override {
        vassert(_closed, "data persistence not properly closed");
    }

private:
    memory_persistence_controller* _controller;
    bool _closed = false;
    std::map<internal::file_handle, ss::lw_shared_ptr<memory_file_state>> _data;
};

class metadata_impl : public metadata_persistence {
public:
    explicit metadata_impl(memory_persistence_controller* controller)
      : _controller(controller) {}
    metadata_impl(const metadata_impl&) = delete;
    metadata_impl(metadata_impl&&) = delete;
    metadata_impl& operator=(const metadata_impl&) = delete;
    metadata_impl& operator=(metadata_impl&&) = delete;
    ~metadata_impl() override {
        vassert(_closed, "metadata persistence not properly closed");
    }
    ss::future<std::optional<iobuf>>
    read_manifest(internal::database_epoch e) override {
        if (_controller && _controller->should_fail) {
            throw io_error_exception("injected error");
        }
        if (e > _epoch) {
            co_return std::nullopt;
        }
        co_return _latest.transform([](iobuf& b) { return b.share(); });
    }
    ss::future<>
    write_manifest(internal::database_epoch epoch, iobuf b) override {
        if (_controller && _controller->should_fail) {
            throw io_error_exception("injected error");
        }
        _epoch = epoch;
        _latest = std::move(b);
        co_return;
    }

    ss::future<> close() override {
        _closed = true;
        co_return;
    }

private:
    memory_persistence_controller* _controller;
    bool _closed = false;
    internal::database_epoch _epoch;
    std::optional<iobuf> _latest;
};

} // namespace

std::unique_ptr<data_persistence>
make_memory_data_persistence(memory_persistence_controller* controller) {
    return std::make_unique<data_impl>(controller);
}

std::unique_ptr<metadata_persistence>
make_memory_metadata_persistence(memory_persistence_controller* controller) {
    return std::make_unique<metadata_impl>(controller);
}

} // namespace lsm::io
