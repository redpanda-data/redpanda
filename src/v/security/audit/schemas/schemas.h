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

#include "security/audit/schemas/types.h"

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
    virtual ~ocsf_base_impl() = default;

    virtual ss::sstring to_json() const = 0;
    virtual size_t key() const noexcept = 0;
    virtual void increment(timestamp_t) const = 0;
};

template<typename Derived>
class ocsf_base_event : public ocsf_base_impl {
public:
    void increment(timestamp_t event_time) const final {
        this->_count++;
        this->_end_time = event_time;
    }

    size_t key() const noexcept final {
        size_t h = 0;
        boost::hash_combine(h, this->hash());
        boost::hash_combine(h, this->base_hash());
        return h;
    }

    ss::sstring to_json() const final {
        return ::security::audit::rjson_serialize(
          *(static_cast<const Derived*>(this)));
    }

    timestamp_t get_time() const { return _time; }

    ocsf_base_event(const ocsf_base_event&) = delete;
    ocsf_base_event& operator=(const ocsf_base_event&) = delete;
    ocsf_base_event(ocsf_base_event&&) = default;
    ocsf_base_event& operator=(ocsf_base_event&&) = default;
    ~ocsf_base_event() override = default;

protected:
    template<typename T>
    ocsf_base_event(
      category_uid category_uid,
      class_uid class_uid,
      severity_id severity_id,
      timestamp_t time,
      T activity_id)
      : _category_uid(category_uid)
      , _class_uid(class_uid)
      , _metadata(ocsf_redpanda_metadata())
      , _severity_id(severity_id)
      , _start_time(time)
      , _time(time)
      , _type_uid(get_ocsf_type(this->_class_uid, activity_id)) {}

    virtual size_t hash() const = 0;

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

    size_t base_hash() const {
        size_t h = 0;
        boost::hash_combine(h, std::hash<category_uid>()(_category_uid));
        boost::hash_combine(h, std::hash<class_uid>()(_class_uid));
        boost::hash_combine(h, std::hash<metadata>()(_metadata));
        boost::hash_combine(h, std::hash<severity_id>()(_severity_id));
        boost::hash_combine(h, std::hash<type_uid>()(_type_uid));

        return h;
    }
};
} // namespace security::audit
