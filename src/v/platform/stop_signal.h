#pragma once

#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>

class stop_signal {
public:
    stop_signal() {
        ss::engine().handle_signal(SIGINT, [this] { signaled(); });
        ss::engine().handle_signal(SIGTERM, [this] { signaled(); });
    }
    ~stop_signal() {
        ss::engine().handle_signal(SIGINT, [] {});
        ss::engine().handle_signal(SIGTERM, [] {});
    }
    ss::future<> wait() {
        return _cond.wait([this] { return _caught; });
    }

    bool stopping() const { return _caught; }

private:
    void signaled() {
        _caught = true;
        _cond.broadcast();
    }

    bool _caught = false;
    ss::condition_variable _cond;
};
