#pragma once

#include "seastarx.h"
#include "storage/kvstore.h"
#include "storage/log_manager.h"

namespace storage {

class api {
public:
    explicit api(kvstore_config kv_conf, log_config log_conf) noexcept
      : _kv_conf(std::move(kv_conf))
      , _log_conf(std::move(log_conf)) {}

    ss::future<> start() {
        _kvstore = std::make_unique<kvstore>(_kv_conf);
        return _kvstore->start().then([this] {
            _log_mgr = std::make_unique<log_manager>(_log_conf, kvs());
        });
    }

    ss::future<> stop() {
        return _log_mgr->stop().then([this] { return _kvstore->stop(); });
    }

    kvstore& kvs() { return *_kvstore; }
    log_manager& log_mgr() { return *_log_mgr; }

private:
    kvstore_config _kv_conf;
    log_config _log_conf;

    std::unique_ptr<kvstore> _kvstore;
    std::unique_ptr<log_manager> _log_mgr;
};

} // namespace storage
