/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "security/krb5.h"

namespace security::krb5 {

namespace impl {
class error_message {
    struct deleter {
        ::krb5_context ctx;
        void operator()(const char* realm) const {
            ::krb5_free_error_message(ctx, realm);
        }
    };

public:
    explicit error_message(const char* msg, ::krb5_context ctx)
      : _msg(msg, deleter{ctx}) {}

    explicit operator std::string_view() { return _msg.get(); }

private:
    std::unique_ptr<const char, deleter> _msg;
};
} // namespace impl

struct realm_deleter {
    ::krb5_context ctx;
    void operator()(char* realm) const {
        ::krb5_free_default_realm(ctx, realm);
    }
};

result<context> context::create() noexcept {
    ::krb5_context krb5_ctx{nullptr};
    int const krb5_rv = ::krb5_init_context(&krb5_ctx);
    if (krb5_rv != 0) {
        impl::error_message msg{
          ::krb5_get_error_message(nullptr, krb5_rv), nullptr};
        auto err = impl::error_impl{
          krb5_rv, ss::sstring{std::string_view{msg}}};
        return err;
    }

    return context{krb5_ctx};
}

result<ss::sstring> context::get_default_realm() const noexcept {
    char* default_realm_ptr = nullptr;
    int const krb5_rv = ::krb5_get_default_realm(
      _ctx.get(), &default_realm_ptr);
    if (krb5_rv != 0) {
        impl::error_message msg{
          ::krb5_get_error_message(_ctx.get(), krb5_rv), _ctx.get()};
        auto err = impl::error_impl{
          krb5_rv, ss::sstring{std::string_view{msg}}};
        return err;
    }

    std::unique_ptr<char, realm_deleter> const default_realm(
      default_realm_ptr, realm_deleter{_ctx.get()});
    return ss::sstring{std::string_view{default_realm.get()}};
}
} // namespace security::krb5
