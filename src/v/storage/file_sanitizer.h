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

#include "base/vassert.h"
#include "config/node_config.h"
#include "ssx/sformat.h"
#include "storage/file_sanitizer_types.h"
#include "storage/logger.h"
#include "storage/segment_appender.h"

#include <seastar/core/file.hh>
#include <seastar/util/backtrace.hh>
#include <seastar/util/exceptions.hh>
#include <seastar/util/log.hh>

#include <boost/intrusive/list.hpp>

#include <optional>
#include <ostream>
#include <random>
#include <unordered_map>
#include <utility>

namespace bi = boost::intrusive;

#include "base/seastarx.h"

namespace storage {

struct sanitizer_op
  : public bi::list_base_hook<bi::link_mode<bi::auto_unlink>> {
    ss::sstring name_op;
    ss::saved_backtrace bt;

    explicit sanitizer_op(ss::sstring operation)
      : name_op(std::move(operation))
      , bt(ss::current_backtrace()) {}

    friend std::ostream& operator<<(std::ostream& o, const sanitizer_op& s) {
        return o << "{sanitizer_op: " << s.name_op << ", backtrace:\n"
                 << s.bt << "\n}";
    }
};

/// Classed used to debug bad file accesses, by wrapping the file handle:
/// auto fd = file(make_shared(file_io_sanitizer(original_fd)));
class file_io_sanitizer final : public ss::file_impl {
public:
    explicit file_io_sanitizer(
      ss::file f,
      std::filesystem::path path,
      ntp_sanitizer_config sanitizer_config)
      : _file(std::move(f))
      , _path(std::move(path))
      , _config(std::move(sanitizer_config)) {
        if (!_config.sanitize_only && _config.finjection_cfg) {
            // Combine the global failure injection seed with the file path
            // hash to get a unique sequence of failures for every file handle.
            auto file_seed = _config.finjection_cfg->seed;
            boost::hash_combine(file_seed, std::hash<std::string>()(_path));

            _random_gen = std::mt19937(file_seed);
        }
    }
    ~file_io_sanitizer() override {
        if (!_closed && _file) {
            vlog(
              finjectlog.error,
              "File {} destroyed without being closed",
              _path);
            output_pending_ops();
        }
    }

    file_io_sanitizer(const file_io_sanitizer&) = delete;
    file_io_sanitizer& operator=(const file_io_sanitizer&) = delete;
    file_io_sanitizer(file_io_sanitizer&& o) noexcept
      : _pending_ops(std::move(o._pending_ops))
      , _file(std::move(o._file))
      , _path(std::move(o._path))
      , _config(std::move(o._config))
      , _closed(std::move(o._closed))
      , _random_gen(std::move(o._random_gen)) {}
    file_io_sanitizer& operator=(file_io_sanitizer&& o) noexcept {
        if (this != &o) {
            this->~file_io_sanitizer();
            new (this) file_io_sanitizer(std::move(o));
        }
        return *this;
    }

    void set_pointer_to_appender(segment_appender* ptr) { _appender_ptr = ptr; }

    ss::future<size_t> write_dma(
      uint64_t pos,
      const void* buffer,
      size_t len,
      const ss::io_priority_class& pc) final {
        assert_file_not_closed();
        return with_op(
          ssx::sformat(
            "ss::future<size_t>::write_dma(pos:{}, *void, len:{})", pos, len),
          maybe_inject_failure(failable_op_type::write)
            .then([this, pos, buffer, len, &pc]() {
                return get_file_impl(_file)
                  ->write_dma(pos, buffer, len, pc)
                  .finally([this]() {
                      if (_appender_ptr) {
                          _appender_ptr->reset_batch_types_to_write();
                      }
                  });
            }));
    }

    ss::future<size_t> write_dma(
      uint64_t pos,
      std::vector<iovec> iov,
      const ss::io_priority_class& pc) final {
        assert_file_not_closed();
        auto iov_size = iov.size();
        return with_op(
          ssx::sformat(
            "ss::future<size_t>::write_dma(pos:{}, vector<iovec>:{})",
            pos,
            iov_size),
          maybe_inject_failure(failable_op_type::write)
            .then([this, pos, iov = std::move(iov), &pc]() {
                return get_file_impl(_file)
                  ->write_dma(pos, iov, pc)
                  .finally([this]() {
                      if (_appender_ptr) {
                          _appender_ptr->reset_batch_types_to_write();
                      }
                  });
            }));
    }

    ss::future<size_t> read_dma(
      uint64_t pos,
      void* buffer,
      size_t len,
      const ss::io_priority_class& pc) final {
        assert_file_not_closed();
        return with_op(
          ssx::sformat(
            "ss::future<size_t> read_dma(pos:{}, void* buffer, len:{})",
            pos,
            len),
          get_file_impl(_file)->read_dma(pos, buffer, len, pc));
    }

    ss::future<size_t> read_dma(
      uint64_t pos,
      std::vector<iovec> iov,
      const ss::io_priority_class& pc) final {
        assert_file_not_closed();
        return with_op(
          ssx::sformat(
            "ss::future<size_t> read_dma(pos{}, std::vector<iovec>:{} , "
            "const ss::io_priority_class& pc)",
            pos,
            iov.size()),
          get_file_impl(_file)->read_dma(pos, iov, pc));
    }

    ss::future<> flush() final {
        assert_file_not_closed();
        return with_op(
          ssx::sformat("ss::future<> flush(void)"),
          maybe_inject_failure(failable_op_type::flush).then([this]() {
              return get_file_impl(_file)->flush();
          }));
    }

    ss::future<struct stat> stat() final {
        assert_file_not_closed();
        return with_op(
          ssx::sformat("ss::future<> stat(void)"),
          get_file_impl(_file)->stat());
    }

    ss::future<> truncate(uint64_t length) final {
        assert_file_not_closed();
        return with_op(
          ssx::sformat("ss::future<> truncate({})", length),
          maybe_inject_failure(failable_op_type::truncate).then([this, length] {
              return get_file_impl(_file)->truncate(length);
          }));
    }

    ss::future<> discard(uint64_t offset, uint64_t length) final {
        assert_file_not_closed();
        return with_op(
          ssx::sformat(
            "ss::future<> discard(offset:{}, length:{})", offset, length),
          get_file_impl(_file)->discard(offset, length));
    }

    ss::future<> allocate(uint64_t position, uint64_t length) final {
        assert_file_not_closed();
        return with_op(
          ssx::sformat(
            "ss::future<> allocate(position:{}, length:{})", position, length),
          maybe_inject_failure(failable_op_type::falloc)
            .then([this, position, length] {
                return get_file_impl(_file)->allocate(position, length);
            }));
    }

    ss::future<uint64_t> size() final {
        assert_file_not_closed();
        return with_op(
          ssx::sformat("ss::future<uint64_t> size(void)"),
          get_file_impl(_file)->size());
    }

    ss::future<> close() final {
        if (_closed) {
            vlog(finjectlog.error, "close called twice on file {}", _path);
            output_pending_ops();
        }

        if (!_pending_ops.empty()) {
            vlog(
              finjectlog.error,
              "close called concurrently with other operations on file "
              "{}. inflight_writes={}, pending_flushes={}",
              _path,
              maybe_dump_appender_inflight_writes(),
              maybe_dump_appender_pending_flushes());

            output_pending_ops();
        }
        _closed = {ss::current_backtrace()};
        return with_op(
          ssx::sformat("ss::future<> close()"),
          maybe_inject_failure(failable_op_type::close).then([this]() {
              return get_file_impl(_file)->close();
          }));
    }

    std::unique_ptr<ss::file_handle_impl> dup() final {
        assert_file_not_closed();
        return get_file_impl(_file)->dup();
    }

    ss::subscription<ss::directory_entry> list_directory(
      std::function<ss::future<>(ss::directory_entry de)> next) final {
        assert_file_not_closed();
        return get_file_impl(_file)->list_directory(next);
    }

    ss::future<ss::temporary_buffer<uint8_t>> dma_read_bulk(
      uint64_t offset,
      size_t range_size,
      const ss::io_priority_class& pc) final {
        assert_file_not_closed();
        return with_op(
          ssx::sformat(
            "ss::future<ss::temporary_buffer<uint8_t>> "
            "dma_read_bulk(offset:{}, range_size:{}",
            offset,
            range_size),
          get_file_impl(_file)->dma_read_bulk(offset, range_size, pc));
    }

private:
    template<typename Future>
    Future with_op(ss::sstring name, Future&& f) {
        auto pending = std::make_unique<sanitizer_op>(std::move(name));
        _pending_ops.push_back(*pending);
        return std::forward<Future>(f).finally(
          [pending = std::move(pending)] {});
    }

    // This function is not a coroutine in order to avoid introducing a
    // scheduling point between the two places that generate random numbers
    // within it. This keeps failure injection deterministic.
    ss::future<> maybe_inject_failure(failable_op_type op_type) {
        if (
          !config::node().storage_failure_injection_enabled()
          || _config.sanitize_only || !_config.finjection_cfg || !_random_gen) {
            return ss::make_ready_future<>();
        }

        const auto& fail_cfgs = _config.finjection_cfg.value();
        const auto fail_cfg = find_failure_config(op_type, fail_cfgs);
        if (!fail_cfg.has_value()) {
            return ss::make_ready_future<>();
        }

        std::uniform_real_distribution<double> distrib(0, 100);

        auto f = ss::now();

        if (
          fail_cfg->delay_probability && fail_cfg->max_delay_ms
          && fail_cfg->min_delay_ms) {
            const auto roll = distrib(_random_gen.value());
            if (roll <= fail_cfg->delay_probability) {
                std::uniform_int_distribution<> delay_distrib(
                  fail_cfg->min_delay_ms.value(),
                  fail_cfg->max_delay_ms.value());
                auto delay = delay_distrib(_random_gen.value());

                vlog(
                  finjectlog.trace,
                  "Injecting delay of {}ms for {} operation on file {} of ntp "
                  "{}",
                  delay,
                  op_type,
                  _path,
                  fail_cfgs.ntp);

                f = ss::sleep(std::chrono::milliseconds{delay});
            }
        }

        if (fail_cfg->failure_probability) {
            const auto roll = distrib(_random_gen.value());
            if (roll <= fail_cfg->failure_probability) {
                vlog(
                  finjectlog.trace,
                  "Injecting EIO for {} operation on file {} of ntp {}",
                  op_type,
                  _path,
                  fail_cfgs.ntp);

                f = f.then([path = _path]() {
                    return ss::make_exception_future<>(
                      ss::make_filesystem_error(
                        "Injected Failure",
                        path,
                        static_cast<int>(std::errc::io_error)));
                });
            }
        }

        return f;
    }

    std::optional<failable_op_config> find_failure_config(
      failable_op_type op_type,
      const ntp_failure_injection_config& ntp_config) const {
        if (op_type != failable_op_type::write || _appender_ptr == nullptr) {
            auto fail_cfg = std::find_if(
              ntp_config.op_configs.begin(),
              ntp_config.op_configs.end(),
              [&op_type](auto& cfg) { return op_type == cfg.op_type; });

            if (fail_cfg == ntp_config.op_configs.end()) {
                return std::nullopt;
            } else {
                return *fail_cfg;
            }
        } else {
            // This branch handles the case where op_type==write.
            // Failure insertion for write operations supports batch
            // granularity, so we iterate through all available failure
            // configs and pick the highest chances of failure from
            // all batch types queued for the write.

            std::optional<failable_op_config> final_config;

            for (const auto& fail_cfg : ntp_config.op_configs) {
                if (fail_cfg.op_type != failable_op_type::write) {
                    continue;
                }

                if (
                  fail_cfg.batch_type
                  && !is_batch_type_queued_in_appender(
                    fail_cfg.batch_type.value())) {
                    continue;
                }

                if (!final_config.has_value()) {
                    final_config = fail_cfg;
                } else {
                    final_config->failure_probability = std::max(
                      final_config->failure_probability,
                      fail_cfg.failure_probability);

                    if (
                      final_config->delay_probability
                      < fail_cfg.delay_probability) {
                        final_config->delay_probability
                          = fail_cfg.delay_probability;
                        final_config->min_delay_ms = fail_cfg.min_delay_ms;
                        final_config->max_delay_ms = fail_cfg.max_delay_ms;
                    }
                }
            }

            return final_config;
        }
    }

    bool is_batch_type_queued_in_appender(
      model::record_batch_type batch_type) const {
        auto batch_types_for_next_write = _appender_ptr->batch_types_to_write();
        return batch_types_for_next_write
               & (1LU << static_cast<uint8_t>(batch_type));
    }

    void output_pending_ops() {
        std::cout << "Called from " << ss::current_backtrace();
        for (sanitizer_op& op : _pending_ops) {
            std::cout << "....pending op: " << op;
        }
    }

    void assert_file_not_closed() {
        vassert(
          !_closed,
          "Op performed on closed file. StackTrace:\n{}",
          ss::current_backtrace());
    }

    ss::sstring maybe_dump_appender_inflight_writes() const {
        if (!_appender_ptr) {
            return "unknown";
        }

        return fmt::format("{}", _appender_ptr->_inflight);
    }

    ss::sstring maybe_dump_appender_pending_flushes() const {
        if (!_appender_ptr) {
            return "unknown";
        }

        return fmt::format("{}", _appender_ptr->_flush_ops);
    }

private:
    boost::intrusive::list<sanitizer_op, bi::constant_time_size<false>>
      _pending_ops;
    ss::file _file;
    std::filesystem::path _path;
    ntp_sanitizer_config _config;
    std::optional<ss::saved_backtrace> _closed;
    std::optional<std::mt19937> _random_gen;

    segment_appender* _appender_ptr{nullptr};
};

} // namespace storage
