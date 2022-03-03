/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "seastarx.h"
#include "storage/kvstore.h"
#include "storage/log_manager.h"

namespace storage {

class api {
public:
    explicit api(
      std::function<kvstore_config()> kv_conf_cb,
      std::function<log_config()> log_conf_cb) noexcept
      : _kv_conf_cb(std::move(kv_conf_cb))
      , _log_conf_cb(std::move(log_conf_cb)) {}

    ss::future<> start() {
        _kvstore = std::make_unique<kvstore>(_kv_conf_cb());
        return _kvstore->start().then([this] {
            _log_mgr = std::make_unique<log_manager>(_log_conf_cb(), kvs());
        });
    }

    ss::future<> stop() {
        auto f = ss::now();
        if (_log_mgr) {
            f = _log_mgr->stop();
        }
        if (_kvstore) {
            return f.then([this] { return _kvstore->stop(); });
        }
        return f;
    }

    kvstore& kvs() { return *_kvstore; }
    log_manager& log_mgr() { return *_log_mgr; }

private:
    std::function<kvstore_config()> _kv_conf_cb;
    std::function<log_config()> _log_conf_cb;

    std::unique_ptr<kvstore> _kvstore;
    std::unique_ptr<log_manager> _log_mgr;
};

} // namespace storage
