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

#include "base/seastarx.h"
#include "config/property.h"
#include "model/fundamental.h"
#include "pandaproxy/schema_registry/subject_name_strategy.h"
#include "pandaproxy/schema_registry/types.h"

#include <boost/multi_index/indexed_by.hpp>
#include <boost/multi_index/key.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index_container.hpp>

#include <iterator>
#include <utility>

namespace pandaproxy::schema_registry {

///\brief An MRU cache of valid schemas for a given topic.
class schema_id_cache {
public:
    enum class field : uint8_t { key, val };
    using offsets_t = std::optional<std::vector<int32_t>>;

    explicit schema_id_cache(config::binding<size_t> cap)
      : _capacity{std::move(cap)} {
        _capacity.watch([this]() { shrink_to_capacity(); });
    }

    bool has(
      model::topic_view topic,
      field field,
      subject_name_strategy sns,
      schema_id s_id,
      const offsets_t& offsets) {
        auto& map = _cache.get<underlying_map>();
        auto it = map.find(entry::view_t(topic, field, sns, s_id, offsets));
        bool has = it != map.end();
        if (has) {
            auto& list = _cache.get<underlying_list>();
            list.relocate(list.begin(), _cache.project<underlying_list>(it));
        }
        return has;
    };

    void put(
      model::topic_view topic,
      field field,
      subject_name_strategy sns,
      schema_id s_id,
      offsets_t offsets) {
        auto& list = _cache.get<underlying_list>();
        auto [it, i] = list.emplace_front(
          topic, field, sns, s_id, std::move(offsets));
        if (i) {
            shrink_to_capacity();
        }
    }

    size_t invalidate(model::topic_view topic) {
        auto& map = _cache.get<underlying_map>();
        auto [b, e] = map.equal_range(topic, entry::topic_less{});
        auto count = std::distance(b, e);
        map.erase(b, e);
        return count;
    }

private:
    // Truncate the cache from the back of the sequence
    void shrink_to_capacity() {
        auto& list = _cache.get<underlying_list>();
        if (list.size() > _capacity()) {
            list.resize(_capacity());
        }
    }

    struct entry {
        entry() = default;
        entry(
          model::topic_view tp,
          field f,
          subject_name_strategy sns,
          schema_id s_id,
          offsets_t offsets = std::nullopt)
          : topic{tp}
          , s_id{s_id}
          , offsets{std::move(offsets)}
          , f{f}
          , sns{sns} {}

        model::topic topic;
        schema_id s_id{invalid_schema_id};
        std::optional<std::vector<int32_t>> offsets;
        field f{};
        subject_name_strategy sns{};

        using view_t = std::tuple<
          model::topic_view,
          field,
          subject_name_strategy,
          schema_id,
          const offsets_t&>;
        view_t view() const { return {topic, f, sns, s_id, offsets}; }

        // Full comparison of entry
        struct less {
            using is_transparent = void;
            bool operator()(const entry& lhs, const entry& rhs) const {
                return lhs.view() < rhs.view();
            }
            bool operator()(view_t lhs, const entry& rhs) const {
                return lhs < rhs.view();
            }
            bool operator()(const entry& lhs, view_t rhs) const {
                return lhs.view() < rhs;
            }
        };

        // Compare only the topic
        struct topic_less {
            using is_transparent = void;
            bool operator()(model::topic_view lhs, const entry& rhs) const {
                return lhs < model::topic_view(rhs.topic);
            }
            bool operator()(const entry& lhs, model::topic_view rhs) const {
                return model::topic_view(lhs.topic) < rhs;
            }
        };
    };

    struct underlying_list {};
    struct underlying_map {};
    using underlying_t = boost::multi_index::multi_index_container<
      entry,
      boost::multi_index::indexed_by<
        // Sequenced list of entries
        boost::multi_index::sequenced<boost::multi_index::tag<underlying_list>>,
        // Set of entries by topic, field, sns, prefix
        boost::multi_index::ordered_unique<
          boost::multi_index::tag<underlying_map>,
          boost::multi_index::identity<entry>,
          entry::less>>>;

    underlying_t _cache;
    config::binding<size_t> _capacity;
};

} // namespace pandaproxy::schema_registry
