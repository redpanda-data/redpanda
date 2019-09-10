#pragma once

#include "seastarx.h"

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>

#include <unordered_map>

namespace finjector {

struct probe {
    probe() = default;
    virtual ~probe() = default;
    virtual uint64_t id() const = 0;
    virtual std::vector<sstring> points() = 0;
    virtual int8_t method_for_point(std::string_view point) const = 0;

    [[gnu::always_inline]] bool operator()() const {
#ifndef NDEBUG
        // debug
        return true;
#else
        // production
        return false;
#endif
    }

    bool is_enabled() const {
        return operator()();
    }
    void set_exception(std::string_view point) {
        _exception_methods |= method_for_point(point);
    }
    void set_delay(std::string_view point) {
        _delay_methods |= method_for_point(point);
    }
    void set_termination(std::string_view point) {
        _termination_methods |= method_for_point(point);
    }
    void unset(std::string_view point) {
        const int8_t m = method_for_point(point);
        _exception_methods &= ~m;
        _delay_methods &= ~m;
        _termination_methods &= ~m;
    }

protected:
    int8_t _exception_methods = 0;
    int8_t _delay_methods = 0;
    int8_t _termination_methods = 0;
};

class honey_badger {
public:
    honey_badger() = default;
    void register_probe(const sstring& module, shared_ptr<probe> p);
    void deregister_probe(const sstring& module, uint64_t id);

    void set_exception(const sstring& module, const sstring& point);
    void set_delay(const sstring& module, const sstring& point);
    void set_termination(const sstring& module, const sstring& point);
    void unset(const sstring& module, const sstring& point);
    std::unordered_map<sstring, std::vector<sstring>> points() const;

private:
    std::unordered_map<sstring, shared_ptr<probe>> _probes;
};

honey_badger& shard_local_badger();

} // namespace finjector
