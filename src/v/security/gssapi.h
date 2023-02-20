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

#include "bytes/bytes.h"

#include <gssapi/gssapi.h>
#include <gssapi/gssapi_ext.h>

#include <string_view>

namespace security::gss {

class buffer_view {
public:
    explicit buffer_view(::gss_buffer_desc bd)
      : _impl{bd} {}
    explicit buffer_view(std::string_view sv)
      : _impl{sv.size(), const_cast<char*>(sv.data())} {}
    explicit buffer_view(bytes_view bv)
      : _impl{bv.size(), const_cast<unsigned char*>(bv.data())} {}

    explicit operator std::string_view() const {
        return {static_cast<char*>(_impl.value), _impl.length};
    };
    explicit operator bytes_view() const {
        return {static_cast<unsigned char*>(_impl.value), _impl.length};
    };

    const void* value() const { return _impl.value; }
    size_t size() const { return _impl.length; }
    const gss_buffer_desc* operator&() const { return &_impl; };
    // TODO BP: Should I wrap every call that only uses this as an input param?
    gss_buffer_desc* operator&() { return &_impl; };

private:
    ::gss_buffer_desc _impl;
};

class buffer : public buffer_view {
public:
    using buffer_view::buffer_view;
    explicit buffer()
      : buffer_view{::gss_buffer_desc{0, nullptr}} {}
    buffer(buffer&& other) noexcept
      : buffer_view{other} {
        buffer_view& other_view = other;
        other_view = buffer_view{::gss_buffer_desc{0, nullptr}};
    }
    buffer(const buffer&) = delete;
    buffer operator=(buffer&&) = delete;
    buffer operator=(buffer const&) = delete;

    ~buffer() {
        if (value() != nullptr) {
            OM_uint32 minor{};
            gss_release_buffer(&minor, &(*this));
        }
    }
};

class buffer_set {
public:
    buffer_set() = default;
    buffer_set(buffer_set&&) = delete;
    buffer_set(const buffer_set&) = delete;
    buffer_set operator=(buffer_set&&) = delete;
    buffer_set operator=(buffer_set const&) = delete;

    ~buffer_set() {
        if (_impl != nullptr) {
            OM_uint32 minor{};
            ::gss_release_buffer_set(&minor, &_impl);
        }
    }

    auto size() const { return _impl->count; }
    buffer_view operator[](int i) const {
        return buffer_view{_impl->elements[i]};
    }

    operator gss_buffer_set_t() { return _impl; };
    gss_buffer_set_t* operator&() { return &_impl; };

private:
    gss_buffer_set_t _impl{nullptr};
};

class name {
public:
    name() = default;
    name(name&&) = delete;
    name(const name&) = delete;
    name operator=(name&&) = delete;
    name operator=(name const&) = delete;

    ~name() {
        if (_impl != nullptr) {
            OM_uint32 minor{};
            gss_release_name(&minor, &_impl);
        }
    }

    operator gss_name_t() { return _impl; };
    gss_name_t* operator&() { return &_impl; };

    gss::buffer display_name_buffer() const {
        OM_uint32 minor_status{};
        gss_OID* oid{};
        gss::buffer display_name;
        auto major_status = ::gss_display_name(
          &minor_status, _impl, &display_name, oid);
        if (major_status != GSS_S_COMPLETE) {
            return gss::buffer{};
        }
        return display_name;
    }

    friend std::ostream& operator<<(std::ostream& os, name const& name) {
        fmt::print(os, "{}", std::string_view(name.display_name_buffer()));
        return os;
    }

private:
    gss_name_t _impl{nullptr};
};

class cred_id {
public:
    cred_id() = default;
    cred_id(cred_id&&) = delete;
    cred_id(const cred_id&) = delete;
    cred_id operator=(cred_id&&) = delete;
    cred_id operator=(cred_id const&) = delete;
    ~cred_id() { reset(); }

    void reset() {
        if (_impl != nullptr) {
            OM_uint32 minor{};
            gss_release_cred(&minor, &_impl);
        }
    }

    operator gss_cred_id_t() { return _impl; };
    gss_cred_id_t* operator&() { return &_impl; };

private:
    gss_cred_id_t _impl{nullptr};
};

class ctx_id {
public:
    ctx_id() = default;
    ctx_id(ctx_id&&) = delete;
    ctx_id(const ctx_id&) = delete;
    ctx_id operator=(ctx_id&&) = delete;
    ctx_id operator=(ctx_id const&) = delete;
    ~ctx_id() { reset(); }

    void reset() {
        if (_impl != nullptr) {
            OM_uint32 minor{};
            gss_delete_sec_context(&minor, &_impl, GSS_C_NO_BUFFER);
        }
    }

    operator gss_ctx_id_t() { return _impl; };
    gss_ctx_id_t* operator&() { return &_impl; };

private:
    gss_ctx_id_t _impl{nullptr};
};

} // namespace security::gss

namespace fmt {
template<>
struct fmt::formatter<::gss_OID_desc_struct> final
  : fmt::formatter<string_view> {
    template<typename FormatContext>
    auto format(::gss_OID_desc_struct& v, FormatContext& ctx) const {
        OM_uint32 minor_status{};
        security::gss::buffer oid_name;
        auto major_status = ::gss_oid_to_str(&minor_status, &v, &oid_name);
        if (major_status != GSS_S_COMPLETE) {
            return format_to(
              ctx.out(), "error {}:{}", major_status, minor_status);
        } else {
            return format_to(ctx.out(), "{}", std::string_view(oid_name));
        }
    }
};

} // namespace fmt
