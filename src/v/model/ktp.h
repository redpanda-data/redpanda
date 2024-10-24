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

#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "utils/move_canary.h"

#include <seastar/core/sstring.hh>

#include <absl/container/flat_hash_map.h>
#include <absl/container/node_hash_map.h>

#include <concepts>
#include <functional>
#include <string_view>

namespace model {

constexpr std::string_view kafka_ns_view = "kafka";

/**
 * @brief A ktp is an ntp in the kafka namespace.
 *
 * The namespace is implicitly kafka, but for performance reasons, the namespace
 * is not actually stored. I.e., an instance of this type logically represents
 * the 3-tuple ("kafka", <topic>, <partition>) but only the (<topic>,
 * <partition>) part is stored.
 *
 * Avoiding storing the namespace makes creating, copying, hashing and
 * equality checking (among others) considerably faster. For example,
 * hashing a ktp object with a 30-characer topic name takes ~67 cycles,
 * versus 117 cycles for the equivalent ntp object. The relative difference
 * is larger for shorter topic names.
 *
 * This class can be compared with an ntp and will be equal if the
 * 3-tuple is equal. Similarly it can be hashed with the same result
 * as an ntp in the kafka namespace. This means you can replace a use
 * of ntp with ktp for topics in the kafka namespace, but still inter-
 * operate with ntp objects, e.g., as keys in a hash table (a transparent
 * comparator is needed).
 */
class ktp {
    // The undelrying topic_partition. This must be hidden from users
    // since it cannot be modified without invalidating the hash
    // in the ktp_with_hash variant. So we make it private and offer
    // accessors which return const referneces or copies.
    topic_partition tp;
    [[no_unique_address]] debug_move_canary mc;

public:
    /**
     * @brief Default-construct a ktp object.
     *
     * Equivalent to a ktp with empty topic name
     * and partition id 0.
     */
    ktp()
      : ktp{"", 0} {}

    /**
     * @brief Construct a new ktp from topic and partition id.
     *
     * The namespace is implitly "kafka".
     *
     * @param t the topic for the ktp
     * @param id the partition id for the ktp
     */
    ktp(model::topic t, model::partition_id pid)
      : tp(std::move(t), pid) {}

    /**
     * @brief Weakly typed ktp constructor.
     *
     * @param topic_name the topic for the ktp
     * @param partition_id the partition id for the ktp
     */
    ktp(ss::sstring topic_name, int32_t partition_id)
      : ktp{
          model::topic(std::move(topic_name)),
          model::partition_id(partition_id)} {}

    /**
     * @brief Return the equivalent ntp object.
     *
     * Returns an ntp object in the kafka namespace, with the
     * same topic and partition as this object. For any ktp
     * object k, it is always the case that:
     *
     * k == k.to_ntp()
     *
     * This object returns by-value, so in general may perform
     * an allocation for the topic name if it is longer than
     * the sstring SSO threshold: the "kafka" namespace is
     * smaller than the SSO threshold and won't allocate.
     */
    ntp to_ntp() const { return {kafka_namespace, tp}; };

    /**
     * @brief Return a topic namespace view corresponding to this object.
     *
     * Return a topic_namespace_view with kafka as the namespace and
     * this topic.
     *
     * The view points to inside this object, so its lifetime (or at least
     * the accesses to the view) must be nested within this object's lifetime.
     *
     * Does not allocate as the view simply points to existing objects.
     */
    topic_namespace_view as_tn_view() const {
        return {kafka_namespace, get_topic()};
    }

    /**
     * An explicit conversion operator of `as_tn_view`.
     */
    explicit operator topic_namespace_view() const { return as_tn_view(); }

    /**
     * @brief Return a topic partition view corresponding to this object.
     *
     * Return a topic_partition_view over this object's topic and partition.
     * Note that a topic_partition_view does not have an implicit "kafka"
     * namespace so this conversion loses some information.
     *
     * The view points inside this object, so its lifetime must be a subset of
     * this object's lifetime.
     *
     * Does not need to allocate as the view points to existing objects.
     */
    topic_partition_view as_tp_view() const {
        return {get_topic(), get_partition()};
    }

    /**
     * @brief Returns a reference to the ktp's topic.
     *
     * The ktp must not be in a moved-from state and this is checked via assert
     * in non-release builds.
     */
    const model::topic& get_topic() const {
        check();
        return tp.topic;
    }

    /**
     * @brief Returns the ktp's partition id.
     *
     * The ktp must not be in a moved-from state and this is checked via assert
     * in non-release builds.
     */
    model::partition_id get_partition() const {
        check();
        return tp.partition;
    }

    /**
     * @brief Returns the ktp's namespace as a string_view.
     *
     * Of course, the namespace is always "kafka", but this method
     * is provided to ease interop with other types which may have
     * variable namespaces.
     */
    constexpr const std::string_view get_ns() const { return kafka_ns_view; }

    /**
     * @brief Compare this object with another ktp.
     *
     * This is more efficient than ntp-only or mixed ntp <-> ktp comparison
     * since it does not need to compare the namespace at all.
     */
    bool operator==(const ktp& other) const { return tp_equals(tp, other.tp); }

    /**
     * @brief Compare this object with an ntp.
     *
     * Although this must compare the namespaces it is still in practice faster
     * than an ntp <-> ntp comparison, because one side of the namespace
     * comparison the constexpr string "kafka", which leads to very efficient
     * code generation (an inlined, fixed-size memcmp) for that comparison.
     */
    bool operator==(const ntp& other) const {
        return tp_equals(other.tp, tp) && kafka_ns_view == other.ns();
    }

    auto operator<=>(const ktp& other) const noexcept {
        return tp <=> other.tp;
    }

    friend std::ostream& operator<<(std::ostream& os, const ktp& t) {
        os << t.to_ntp();
        return os;
    }

protected:
    /**
     * @brief Check object consistency.
     */
    void check() const {
        vassert(
          !mc.is_moved_from(), "ktp object was used in a moved-from state");
    }

private:
    /**
     * @brief Helper to compare two topic_partition objects efficiently.
     *
     * When using libc++, comparing two ss::sstring objects uses a relatively
     * slow byte-by-byte comparison, because ss::string uses std::equal_to with
     * a character range, and this is not recognized as a memcmp (though similar
     * patterns are). By explicitly using std::string_view::operator==, as
     * here, we get a recognized memcmp and a generally faster comparison.
     */
    static bool tp_equals(
      const model::topic_partition& l, const model::topic_partition& r) {
        return l.partition == r.partition
               && std::equal_to<std::string_view>{}(l.topic(), r.topic());
    }
};
} // namespace model

namespace std {
template<>
struct hash<model::ktp> {
    /**
     * @brief The hash specialization for ktp.
     *
     * Compatible with the hash for ntp: a ktp will return the same hash as an
     * ntp with the same topic, partition and the "kafka" namespace.
     */
    size_t operator()(const model::ktp& ktp) const {
        return detail::ntp_hash(
          model::kafka_ns_view, ktp.get_topic()(), ktp.get_partition());
    }
};
} // namespace std

namespace model {

/**
 * @brief A ktp object that remembers its hash code.
 *
 * In some contexts, a ktp/ntp object will be repeatedly hashed, which
 * is a relatively expensive operation. Instead, we can swap in this type
 * which remembers its hash code. It is a subclass of ktp, and can safely
 * be passed as a reference to a ktp or sliced down to a ktp for convenience.
 *
 * The hash code is calculated once at constructon and never changes, except via
 * assignment, since the underlying topic and partition cannot be changed (but
 * this invariant must be enforced by the ktp class as noted there). Among other
 * things this means that assignment of a ktp_with_hash from a ktp object does
 * not compile: for this to be safe we'd need to recalculate the hash after
 * assignment.
 *
 * Moved-from ktp_with_hash objects have an invalid cached hash code (i.e.,
 * their cached hash code is not consistent with their topic partition values)
 * and should be used only after being assigned from a valid ktp_with_hash
 * object. Getting the hash code of a moved-from ktp object will assert in
 * non-release builds.
 */
struct ktp_with_hash : public ktp {
    /**
     * @brief Default-construct a ktp_with_hash object.
     *
     * Equivalent to an object with empty topic name
     * and partition id 0.
     */
    ktp_with_hash()
      : ktp_with_hash{"", 0} {}

    /**
     * @brief Construct a ktp_with_hash from topic and partition id.
     *
     * The namespace is implitly "kafka" and the hash is calculated at
     * construction and cached within the object.
     */
    ktp_with_hash(model::topic t, model::partition_id id)
      : ktp{std::move(t), id}
      , _hash_code{hash_code()} {}

    /**
     * @brief Weakly typed ktp_with_hash constructor.
     *
     * @param topic_name the topic for the ktp
     * @param partition_id the partition id for the ktp
     */
    ktp_with_hash(ss::sstring topic_name, int32_t partition_id)
      : ktp_with_hash{
          model::topic(std::move(topic_name)),
          model::partition_id(partition_id)} {}

    template<typename T>
    friend class std::hash; // std::hash needs _hash_code

private:
    /**
     * Return the hash code for this object.
     */
    size_t hash_code() const { return std::hash<ktp>{}(*this); }

    size_t _hash_code;
};

/**
 * @brief A concept covering ntp-alikes.
 *
 * This includes the OG ntp, plus ktp and any type dervied from it, such
 * ktp_with_hash.
 *
 * Note that it does not accept ntp views nor the model::topic_partition
 * type.
 */
template<typename T>
concept any_ntp = std::same_as<T, ntp> || std::derived_from<T, ktp>;

struct ktp_hash_eq {
    using is_transparent = void;

    template<model::any_ntp T>
    size_t operator()(const T& v) const {
        return std::hash<T>{}(v);
    }

    bool operator()(
      const model::any_ntp auto& lhs, const model::any_ntp auto& rhs) const {
        return lhs == rhs;
    }
};

/**
 * @brief Helper alias to declare a flat map from ntp to V.
 *
 * Uses transparent comparator to allow any ntp object to be used for lookup.
 */
template<typename V>
using ntp_flat_map_type
  = absl::flat_hash_map<model::ntp, V, ktp_hash_eq, ktp_hash_eq>;

/**
 * @brief Helper alias to declare a flat map from ntp to V.
 *
 * Uses transparent comparator to allow any ntp object to be used for lookup.
 */
template<typename V>
using ntp_node_map_type
  = absl::node_hash_map<model::ntp, V, ktp_hash_eq, ktp_hash_eq>;

} // namespace model

namespace std {
template<>
struct hash<model::ktp_with_hash> {
    size_t operator()(const model::ktp_with_hash& ktp) const {
        ktp.check();
        return ktp._hash_code;
    }
};
} // namespace std
