/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "random/generators.h"
#include "seastarx.h"
#include "units.h"
#include "utils/memory_data_source.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>

#include <cstring>
#include <memory>

namespace tests {

/// Generates inputs with varying buffer sizes for testing parsers and making
/// sure that they can deal with inputs that are arbitrarily split between
/// buffers.
///
/// Consider varying buffer sizes based on a fuzzer state.
class varying_buffer_input_stream {
private:
    struct ds_wrapper : ss::data_source_impl {
        explicit ds_wrapper(ss::data_source ds, size_t min, size_t max)
          : _fd(std::move(ds))
          , _min(min)
          , _max(max) {}
        ~ds_wrapper() override = default;

        ss::future<ss::temporary_buffer<char>> get() override {
            auto requested = random_generators::get_int(_min, _max);
            if (_buf.size() >= requested) {
                // We have enough data buffered internally. Share it with the
                // caller.
                auto ret = _buf.share(0, requested);
                _buf.trim_front(requested);
                co_return ret;
            } else {
                // We don't have enough data. Allocate a new buffer of the
                // requested size and try to fill it. If last read doesn't fit,
                // we'll store the remaining bytes for the next call.
                auto ret = ss::temporary_buffer<char>(requested);
                size_t completed = 0;

                if (!_buf.empty()) {
                    strncpy(ret.get_write(), _buf.get(), _buf.size());
                    completed += _buf.size();
                    _buf = {};
                }

                while (completed < requested) {
                    auto data = co_await _fd.get();
                    if (data.empty()) {
                        // The underlying data source is empty.
                        // Return what we have.
                        ret.trim(completed);
                        co_return ret;
                    }

                    const auto required = requested - completed;
                    if (data.size() <= required) {
                        std::copy(
                          data.begin(),
                          data.end(),
                          ret.get_write() + completed);
                        completed += data.size();
                    } else {
                        std::copy_n(
                          data.begin(), required, ret.get_write() + completed);
                        completed += required;
                        assert(completed == requested);
                        data.trim_front(required);
                        _buf = std::move(data);
                        co_return ret;
                    }
                }

                co_return ret;
            }
        }

        ss::future<> close() override {
            _buf = {};
            return _fd.close();
        }

    private:
        ss::temporary_buffer<char> _buf;
        ss::data_source _fd;
        size_t _min;
        size_t _max;
    };

public:
    varying_buffer_input_stream() = delete;

    static ss::input_stream<char>
    create(ss::data_source ds, size_t min, size_t max) {
        vassert(
          min > 0,
          "minimum buffer size should be at least 1 byte to be able to "
          "differentiate from EOF");
        vassert(min <= max, "min lte max");
        vassert(max <= 1_MiB, "at most 1MiB can be buffered in tests");
        return ss::input_stream<char>(ss::data_source(
          std::make_unique<ds_wrapper>(std::move(ds), min, max)));
    }

    static ss::input_stream<char>
    create(ss::input_stream<char> in, size_t min, size_t max) {
        return varying_buffer_input_stream::create(
          std::move(in).detach(), min, max);
    }

    static ss::input_stream<char>
    create(std::string_view str, size_t min, size_t max) {
        auto buffers = std::vector<ss::temporary_buffer<char>>{};
        buffers.emplace_back(str.data(), str.size());
        return varying_buffer_input_stream::create(
          ss::data_source(
            std::make_unique<memory_data_source>(std::move(buffers))),
          min,
          max);
    }
};

/// A class for generating fuzzed (garbled) inputs based on good inputs to
/// improve the effectiveness of parser testing.
/// The goal is to increase the likelihood of the parser exploring a variety of
/// states, including correct parsing, by introducing somewhat controlled
/// variations rather than completely random data.
///
/// \note This is not a replacement for a coverage guided fuzzer. However, we
/// should plug in a fuzzer state.
class garbling_input_stream {
    struct ds_wrapper : ss::data_source_impl {
        explicit ds_wrapper(ss::data_source ds)
          : _fd(std::move(ds)) {}
        ~ds_wrapper() override = default;
        ss::future<ss::temporary_buffer<char>> get() override {
            while (!_eof) {
                auto data = co_await _fd.get();
                if (data.empty()) {
                    _eof = true;
                    break;
                }
                _bufs.emplace_back(std::move(data));
                if (random_generators::get_int(0, 9) < 1) {
                    break;
                }
            }

            if (_bufs.empty()) {
                co_return ss::temporary_buffer<char>();
            }

            auto buf_ix = random_generators::get_int<size_t>(
              0, _bufs.size() - 1);
            auto ret = std::exchange(_bufs[buf_ix], std::move(_bufs.back()));
            _bufs.pop_back();

            co_return ret;
        }

        ss::future<> close() override {
            _bufs.clear();
            return _fd.close();
        }

    private:
        std::vector<ss::temporary_buffer<char>> _bufs;
        ss::data_source _fd;
        bool _eof = false;
    };

public:
    garbling_input_stream() = delete;

    static ss::input_stream<char> create(ss::data_source ds) {
        // For better effectiveness we will wrap the data source with
        // varying_buffer_input_stream too and relatively small buffer sizes.
        return ss::input_stream<char>(
          ss::data_source(std::make_unique<ds_wrapper>(
            varying_buffer_input_stream::create(
              ss::data_source(std::make_unique<ds_wrapper>(std::move(ds))),
              1,
              128)
              .detach())));
    }

    static ss::input_stream<char> create(ss::input_stream<char> in) {
        return garbling_input_stream::create(std::move(in).detach());
    }

    static ss::input_stream<char> create(std::string_view str) {
        auto buffers = std::vector<ss::temporary_buffer<char>>{};
        buffers.emplace_back(str.data(), str.size());
        return garbling_input_stream::create(ss::data_source(
          std::make_unique<memory_data_source>(std::move(buffers))));
    }
};

} // namespace tests
