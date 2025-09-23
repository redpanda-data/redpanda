/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "model/ktp.h"
#include "model/metadata.h"
#include "model/namespace.h"

namespace model {

/*
 * @brief A kitp_view is a view of a topic partition with an optional topic_id,
 * in the kafka namespace.
 *
 * It hashes the same as a topic_partition_view, that is, without the topic id.
 *
 * It compares equal when:
 * - Both have a topic_id, and all fields are equal, otherwise
 * - Both have the same topic_partition_view
 */
class kitp_view {
public:
    kitp_view(topic_id id, topic_partition_view tp_view)
      : _id(id)
      , _tp_view{tp_view} {}

    kitp_view(topic_id id, const topic& tp, partition_id pid)
      : _id(id)
      , _tp_view{tp, pid} {}

    topic_id get_topic_id() const { return _id; }
    topic_view get_topic() const { return _tp_view.topic; }
    partition_id get_partition() const { return _tp_view.partition; }
    topic_namespace_view as_tn_view() const {
        return {kafka_namespace, _tp_view.topic};
    }
    topic_partition_view as_tp_view() const { return _tp_view; }
    ktp as_ktp() const { return {_tp_view.topic, _tp_view.partition}; }

private:
    // Note: the id is not hashed, as it's only available if the request
    // specified a non-default topic_id
    template<typename H>
    friend inline H AbslHashValue(H h, const kitp_view& kitp) {
        return H::combine(std::move(h), kitp._tp_view);
    }

    // Note: the id is compared iff both have an id, otherwise the tp_view is
    // compared.
    friend inline bool operator==(const kitp_view& lhs, const kitp_view& rhs) {
        const auto no_id = topic_id{};
        if (lhs._id == no_id || rhs._id == no_id) {
            return lhs._tp_view == rhs._tp_view;
        }
        return std::tie(lhs._id, lhs._tp_view)
               == std::tie(rhs._id, rhs._tp_view);
    };

    friend inline std::ostream&
    operator<<(std::ostream& os, const kitp_view& v) {
        fmt::print(os, "{}, topic_id: {}", v._tp_view, v._id);
        return os;
    }

    topic_id _id;
    topic_partition_view _tp_view;
};

/*
 * Represents an ntp kafka namespace, with an optional topic_id.
 */
class kitp : private model::ktp {
public:
    kitp(topic_id id, topic t, partition_id pid)
      : ktp{std::move(t), pid}
      , _id(id) {}

    kitp_view as_kitp_view() const { return {_id, as_tp_view()}; }

    /**
     * @brief Returns a the kitp's topic_id.
     *
     * The kitp must not be in a moved-from state and this is checked via assert
     * in non-release builds.
     */
    topic_id get_topic_id() const {
        check();
        return _id;
    }

    /**
     * @brief Returns a reference to the kitp's topic.
     *
     * The kitp must not be in a moved-from state and this is checked via assert
     * in non-release builds.
     */
    const topic& get_topic() const { return as_ktp().get_topic(); }

    /**
     * @brief Returns the kitp's partition id.
     *
     * The kitp must not be in a moved-from state and this is checked via assert
     * in non-release builds.
     */
    partition_id get_partition() const { return as_ktp().get_partition(); }

    topic_namespace_view as_tn_view() const {
        return {kafka_namespace, get_topic()};
    }

    const ktp& as_ktp() const { return *this; }

private:
    template<typename H>
    friend inline H AbslHashValue(H h, const kitp& kitp) {
        return H::combine(std::move(h), kitp.as_kitp_view());
    }

    friend inline bool operator==(const kitp& lhs, const kitp& rhs) {
        return lhs.as_kitp_view() == rhs.as_kitp_view();
    }

    friend inline std::ostream& operator<<(std::ostream& os, const kitp& v) {
        return os << v.as_kitp_view();
    }

    topic_id _id;
};

/*
 * @brief A kitp that remembers its hash code.
 */
struct kitp_with_hash : public kitp {
    kitp_with_hash(topic_id id, class topic t, partition_id pid)
      : kitp{id, std::move(t), pid}
      , _hash_code(hash_code()) {}

private:
    template<typename T>
    friend class std::hash; // std::hash needs _hash_code

    template<typename H>
    friend inline H AbslHashValue(H h, const kitp_with_hash& kitp) {
        return H::combine(std::move(h), kitp._hash_code);
    }

    /**
     * Return the hash code for this object.
     */
    inline size_t hash_code() const {
        return absl::Hash<kitp>{}(static_cast<const kitp&>(*this));
    }

    size_t _hash_code;
};

} // namespace model

namespace std {

template<>
struct hash<model::kitp_view> {
    size_t operator()(const model::kitp_view& kitp) const {
        return absl::Hash<model::kitp_view>{}(kitp);
    }
};

template<>
struct hash<model::kitp> {
    size_t operator()(const model::kitp& kitp) const {
        return absl::Hash<model::kitp>{}(kitp);
    }
};

template<>
struct hash<model::kitp_with_hash> {
    size_t operator()(const model::kitp_with_hash& kitp) const {
        return kitp._hash_code;
    }
};

} // namespace std
