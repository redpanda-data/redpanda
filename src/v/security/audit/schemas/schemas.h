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

#include "config/node_config.h"
#include "reflection/type_traits.h"
#include "security/audit/schemas/types.h"
#include "utils/functional.h"

namespace security::audit {

template<typename T>
ss::sstring rjson_serialize(const T& v) {
    ::json::StringBuffer str_buf;
    ::json::Writer<::json::StringBuffer> wrt(str_buf);

    using ::json::rjson_serialize;
    using ::security::audit::rjson_serialize;

    rjson_serialize(wrt, v);

    return ss::sstring{str_buf.GetString(), str_buf.GetSize()};
}

template<typename T>
type_uid get_ocsf_type(class_uid class_uid, T activity_id) {
    using t = type_uid::type;
    constexpr t factor = 100;

    // This is defined by OCSF
    return type_uid{t(class_uid) * factor + t(activity_id)};
}

class ocsf_base_impl {
public:
    /// Instances of this class are moveable but not copyable
    ocsf_base_impl() = default;
    ocsf_base_impl(ocsf_base_impl&&) = default;
    ocsf_base_impl& operator=(ocsf_base_impl&&) = default;
    ocsf_base_impl(const ocsf_base_impl&) = delete;
    ocsf_base_impl& operator=(const ocsf_base_impl&) = delete;
    virtual ~ocsf_base_impl() = default;

    virtual ss::sstring api_info() const = 0;
    virtual ss::sstring to_json() const = 0;
    virtual size_t estimated_size() const noexcept = 0;
    virtual void increment(timestamp_t) const = 0;
    virtual category_uid get_category_uid() const = 0;
    virtual class_uid get_class_uid() const = 0;
    virtual type_uid get_type_uid() const = 0;

private:
    friend std::ostream& operator<<(std::ostream&, const ocsf_base_impl&);
};

template<typename Derived>
class ocsf_base_event : public ocsf_base_impl {
public:
    void increment(timestamp_t event_time) const final {
        this->_count++;
        this->_end_time = event_time;
    }

    size_t estimated_size() const noexcept final {
        return sizeof(*this)
               + estimated_ocsf_size(*static_cast<const Derived*>(this));
    }

    ss::sstring api_info() const override { return ""; }

    ss::sstring to_json() const final {
        return ::security::audit::rjson_serialize(
          *(static_cast<const Derived*>(this)));
    }

    category_uid get_category_uid() const final { return _category_uid; }

    class_uid get_class_uid() const final { return _class_uid; }

    type_uid get_type_uid() const final { return _type_uid; }

    timestamp_t get_time() const { return _time; }

    ocsf_base_event(const ocsf_base_event&) = delete;
    ocsf_base_event& operator=(const ocsf_base_event&) = delete;
    ocsf_base_event(ocsf_base_event&&) noexcept = default;
    ocsf_base_event& operator=(ocsf_base_event&&) noexcept = default;
    ~ocsf_base_event() override = default;

protected:
    template<typename T>
    ocsf_base_event(
      category_uid category_uid,
      class_uid class_uid,
      severity_id severity_id,
      timestamp_t time,
      T activity_id)
      : ocsf_base_event(
          category_uid,
          class_uid,
          ocsf_redpanda_metadata(),
          severity_id,
          time,
          activity_id) {}

    template<typename T>
    ocsf_base_event(
      category_uid category_uid,
      class_uid class_uid,
      metadata metadata,
      severity_id severity_id,
      timestamp_t time,
      T activity_id)
      : _category_uid(category_uid)
      , _class_uid(class_uid)
      , _metadata(std::move(metadata))
      , _severity_id(severity_id)
      , _start_time(time)
      , _time(time)
      , _type_uid(get_ocsf_type(this->_class_uid, activity_id)) {
        _metadata.product.uid = ss::to_sstring(
          config::node().node_id().value_or(model::node_id{0}));
    }

    void rjson_serialize(::json::Writer<::json::StringBuffer>& w) const {
        w.Key("category_uid");
        ::json::rjson_serialize(w, _category_uid);
        w.Key("class_uid");
        ::json::rjson_serialize(w, _class_uid);
        if (_count > 1) {
            w.Key("count");
            ::json::rjson_serialize(w, _count);
        }
        if (_count > 1) {
            w.Key("end_time");
            ::json::rjson_serialize(w, _end_time);
        }
        w.Key("metadata");
        ::json::rjson_serialize(w, _metadata);
        w.Key("severity_id");
        ::json::rjson_serialize(w, _severity_id);
        if (_count > 1) {
            w.Key("start_time");
            ::json::rjson_serialize(w, _start_time);
        }
        w.Key("time");
        ::json::rjson_serialize(w, _time);
        w.Key("type_uid");
        ::json::rjson_serialize(w, _type_uid);
    }

private:
    category_uid _category_uid;
    class_uid _class_uid;
    // Making this mutable allow for the increment method to be const as
    // the expected container that will hold these events will only allow
    // const access.  Modifying these values _do NOT_ change whether
    // this instance is equal to another instance of the event.
    mutable unsigned long _count{1};
    mutable timestamp_t _end_time;
    metadata _metadata;
    severity_id _severity_id;
    timestamp_t _start_time;
    timestamp_t _time;
    type_uid _type_uid;

    /// Method to estimate the size of an ocsf message
    ///
    /// This works by iterating on the fields within the tuple returned by the
    /// 'equality_fields()' method. This method is implemented on all ocsf
    /// structures.
    ///
    /// The returned size is 'estimated' because it doesn't take into account
    /// things like padding and any fields that a struct contains that it did
    /// not include in its return value for 'equality_fields()'. Currently
    /// the only example of this is the 'network_endpoint' struct.
    template<typename T>
    size_t estimated_ocsf_size(const T& t) const noexcept {
        size_t sz = 0;
        if constexpr (reflection::is_std_vector<T>) {
            sz += sizeof(t);
            for (const auto& element : t) {
                sz += estimated_ocsf_size(element);
            }
        } else if constexpr (reflection::is_std_optional<T>) {
            sz += sizeof(t);
            if (t.has_value()) {
                sz += estimated_ocsf_size(*t);
            }
        } else if constexpr (std::is_same_v<ss::sstring, T>) {
            sz += sizeof(ss::sstring) + t.size();
        } else if constexpr (std::is_enum_v<T>) {
            sz += sizeof(std::underlying_type_t<T>(t));
        } else if constexpr (
          std::is_integral_v<T> || reflection::is_ss_bool_class<T>) {
            sz += sizeof(t);
        } else if constexpr (reflection::is_rp_named_type<T>) {
            sz += estimated_ocsf_size(t());
        } else if constexpr (has_equality_fields<T>) {
            std::apply(
              [this, &sz](auto&&... xs) {
                  ((sz += this->estimated_ocsf_size(xs)), ...);
              },
              t.equality_fields());
        } else {
            static_assert(
              always_false_v<T>, "Unsupported type passed to ocsf_size()");
        }
        return sz;
    }
};
} // namespace security::audit
