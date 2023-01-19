/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include <krb5/krb5.h>

#include <string_view>

namespace security::krb5 {

class krb5_context_view {
public:
    explicit krb5_context_view(const ::krb5_context* ctx)
      : _ctx(ctx) {}

    ::krb5_context operator*() const { return *_ctx; }
    explicit operator const ::krb5_context*() { return _ctx; }

private:
    const ::krb5_context* _ctx;
};

class krb5_context {
public:
    krb5_context() = default;
    krb5_context(krb5_context&&) = delete;
    krb5_context(const krb5_context&) = delete;
    krb5_context operator=(krb5_context&&) = delete;
    krb5_context operator=(const krb5_context&) = delete;

    ~krb5_context() noexcept { reset(); }

    void reset() noexcept {
        if (_ctx != nullptr) {
            ::krb5_free_context(_ctx);
            _ctx = nullptr;
        }
    }

    explicit operator krb5_context_view() const {
        return krb5_context_view{&_ctx};
    }
    explicit operator ::krb5_context() const { return _ctx; }
    ::krb5_context* operator&() { return &_ctx; }

private:
    ::krb5_context _ctx{nullptr};
};

class krb5_default_realm {
public:
    explicit krb5_default_realm(krb5_context_view ctx)
      : _ctx(std::move(ctx)) {}

    krb5_default_realm(krb5_default_realm&&) = delete;
    krb5_default_realm(const krb5_default_realm&) = delete;
    krb5_default_realm& operator=(krb5_default_realm&&) = delete;
    krb5_default_realm& operator=(const krb5_default_realm&) = delete;

    ~krb5_default_realm() { reset(); }

    void reset() {
        if (_realm) {
            ::krb5_free_default_realm(*_ctx, _realm);
        }
    }

    explicit operator std::string_view() {
        if (_realm) {
            return {_realm, ::strlen(_realm)};
        } else {
            return {};
        }
    }

    char** operator&() { return &_realm; }

private:
    krb5_context_view _ctx;
    char* _realm{nullptr};
};

} // namespace security::krb5
