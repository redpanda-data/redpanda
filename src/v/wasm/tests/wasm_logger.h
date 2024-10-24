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

#pragma once

#include "wasm/wasi_logger.h"

#include <seastar/util/log.hh>
#include <seastar/util/noncopyable_function.hh>

class wasm_logger final : public wasm::logger {
public:
    wasm_logger() = delete;
    explicit wasm_logger(ss::sstring name, ss::logger* log)
      : _name(std::move(name))
      , _log(log) {}
    ~wasm_logger() override = default;
    wasm_logger(const wasm_logger&) = delete;
    wasm_logger& operator=(const wasm_logger&) = delete;
    wasm_logger(wasm_logger&&) = delete;
    wasm_logger& operator=(wasm_logger&&) = delete;

    void log(ss::log_level lvl, std::string_view message) noexcept override {
        _log->log(lvl, "{} - {}", _name, message);
    }

private:
    ss::sstring _name;
    ss::logger* _log;
};

class capturing_logger final : public wasm::logger {
public:
    capturing_logger() = delete;
    explicit capturing_logger(
      ss::noncopyable_function<void(ss::log_level, std::string_view)> callback)
      : _callback(std::move(callback)) {}
    ~capturing_logger() override = default;
    capturing_logger(const capturing_logger&) = delete;
    capturing_logger& operator=(const capturing_logger&) = delete;
    capturing_logger(capturing_logger&&) = delete;
    capturing_logger& operator=(capturing_logger&&) = delete;

    void log(ss::log_level lvl, std::string_view message) noexcept override {
        _callback(lvl, message);
    }

private:
    ss::noncopyable_function<void(ss::log_level, std::string_view)> _callback;
};
