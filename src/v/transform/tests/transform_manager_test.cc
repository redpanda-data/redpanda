#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "model/transform.h"
#include "transform/logger.h"
#include "transform/tests/test_fixture.h"
#include "transform/transform_manager.h"
#include "transform/transform_processor.h"
#include "utils/human.h"
#include "utils/uuid.h"

#include <seastar/core/idle_cpu_handler.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/print.hh>
#include <seastar/core/reactor.hh>
#include <seastar/util/later.hh>
#include <seastar/util/noncopyable_function.hh>

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/strings/charconv.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace transform {

namespace {

struct transform_entry {
    model::transform_id id;
    model::transform_metadata meta;
    bool name_was_reused;
};

class fake_registry : public registry {
public:
    absl::flat_hash_set<model::partition_id>
    get_leader_partitions(model::topic_namespace_view tp_ns) const override {
        const auto& leader_ntps = _states.back().leader_ntps;
        absl::flat_hash_set<model::partition_id> leaders;
        for (const auto& ntp : leader_ntps) {
            if (ntp.ns == tp_ns.ns && ntp.tp.topic == tp_ns.tp) {
                leaders.emplace(ntp.tp.partition);
            }
        }
        return leaders;
    }

    absl::flat_hash_map<model::transform_id, model::transform_metadata>
    lookup_by_input_topic(model::topic_namespace_view tp_ns) const override {
        const auto& transforms = _states.back().transforms;
        absl::flat_hash_map<model::transform_id, model::transform_metadata>
          result;
        for (const auto& entry : transforms) {
            const auto& input = entry.second.input_topic;
            if (input.ns == tp_ns.ns && input.tp == tp_ns.tp) {
                result.emplace(entry.first, entry.second);
            }
        }
        return result;
    }

    std::optional<model::transform_metadata>
    lookup_by_id(model::transform_id id) const override {
        const auto& transforms = _states.back().transforms;
        auto it = transforms.find(id);
        if (it == transforms.end()) {
            return std::nullopt;
        }
        return it->second;
    }

    std::optional<std::pair<model::transform_id, model::transform_metadata>>
    lookup_by_name(const model::transform_name& name) const {
        for (const auto& entry : _states.back().transforms) {
            if (entry.second.name == name) {
                return entry;
            }
        }
        return std::nullopt;
    }

    model::transform_id put_transform(const model::transform_metadata& meta) {
        auto existing = lookup_by_name(meta.name);
        state& new_state = _states.emplace_back(_states.back());
        if (existing) {
            new_state.transforms[existing->first] = meta;
            return existing->first;
        } else {
            _names.emplace(meta.name);
            new_state.transforms.emplace(++_next_id, meta);
            return _next_id;
        }
    }
    model::transform_id delete_transform(const model::transform_name& name) {
        auto existing = lookup_by_name(name);
        if (!existing) {
            throw std::runtime_error(
              ss::format("trying to delete non-existant transform {}", name));
        }
        state& new_state = _states.emplace_back(_states.back());
        new_state.transforms.erase(existing->first);
        return existing->first;
    }
    void add_leader(model::ntp ntp) {
        state& new_state = _states.emplace_back(_states.back());
        new_state.leader_ntps.emplace(ntp);
    }
    void delete_leader(const model::ntp& ntp) {
        state& new_state = _states.emplace_back(_states.back());
        new_state.leader_ntps.erase(ntp);
    }

    /** All transforms that ever existed. */
    std::vector<transform_entry> all_transforms() {
        absl::flat_hash_map<model::transform_id, transform_entry> deduped;
        for (const auto& state : _states) {
            for (const auto& [id, meta] : state.transforms) {
                bool name_was_reused = _names.count(meta.name) > 1;
                deduped.insert_or_assign(id, {id, meta, name_was_reused});
            }
        }
        std::vector<transform_entry> result;
        for (const auto& [_, entry] : deduped) {
            result.push_back(entry);
        }
        return result;
    }

private:
    model::transform_id _next_id = model::transform_id(1);
    absl::btree_multiset<model::transform_name> _names;

    // We use MVCC state here so we can do nice things like refer to specific
    // versions of a given transform.
    struct state {
        absl::flat_hash_map<model::transform_id, model::transform_metadata>
          transforms;
        absl::flat_hash_set<model::ntp> leader_ntps;
    };

    std::vector<state> _states = {state{}};
};

enum class lifecycle_status { created, active, inactive, destroyed };
std::ostream& operator<<(std::ostream& os, lifecycle_status s) {
    switch (s) {
    case lifecycle_status::created:
        return os << "lifecycle::created";
    case lifecycle_status::active:
        return os << "lifecycle::active";
    case lifecycle_status::inactive:
        return os << "lifecycle::inactive";
    case lifecycle_status::destroyed:
        return os << "lifecycle::destroyed";
    }
    return os << "lifecycle::unknown";
}

class processor_tracker : public processor_factory {
    class tracked_processor : public processor {
        static std::vector<std::unique_ptr<sink>> make_sink() {
            std::vector<std::unique_ptr<sink>> sinks;
            sinks.push_back(std::make_unique<testing::fake_sink>());
            return sinks;
        }

    public:
        tracked_processor(
          std::function<void(lifecycle_status)> cb,
          model::transform_id id,
          model::ntp ntp,
          model::transform_metadata meta,
          probe* p)
          : processor(
            id,
            std::move(ntp),
            std::move(meta),
            std::make_unique<testing::fake_wasm_engine>(),
            [](auto, auto, auto) {},
            std::make_unique<testing::fake_source>(model::offset(1)),
            make_sink(),
            p)
          , _track_fn(std::move(cb)) {
            _track_fn(lifecycle_status::created);
        }
        tracked_processor(const tracked_processor&) = delete;
        tracked_processor(tracked_processor&&) = delete;
        tracked_processor& operator=(const tracked_processor&) = delete;
        tracked_processor& operator=(tracked_processor&&) = delete;

        ss::future<> start() override {
            _track_fn(lifecycle_status::active);
            co_return;
        }
        ss::future<> stop() override {
            _track_fn(lifecycle_status::inactive);
            co_return;
        }
        ~tracked_processor() override {
            _track_fn(lifecycle_status::destroyed);
        }

    private:
        std::function<void(lifecycle_status)> _track_fn;
    };

public:
    // Create a processor with the given metadata and input partition.
    ss::future<std::unique_ptr<processor>> create_processor(
      model::transform_id id,
      model::ntp ntp,
      model::transform_metadata meta,
      probe* probe) override {
        EXPECT_NE(probe, nullptr);
        co_return std::make_unique<tracked_processor>(
          [this, id, ntp](lifecycle_status change) {
              handle_lifecycle_change(id, ntp, change);
          },
          id,
          ntp,
          meta,
          probe);
    }

    absl::flat_hash_map<
      std::pair<model::transform_id, model::ntp>,
      lifecycle_status>
    statuses() {
        return _status;
    }

private:
    void handle_lifecycle_change(
      model::transform_id id, model::ntp ntp, lifecycle_status change) {
        _status.insert_or_assign(std::make_pair(id, std::move(ntp)), change);
    }

    absl::flat_hash_map<
      std::pair<model::transform_id, model::ntp>,
      lifecycle_status>
      _status;
};

using status_map = absl::flat_hash_map<std::string, lifecycle_status>;

template<typename... Args>
concept EmptyPack = sizeof...(Args) == 0;

template<typename... Rest>
void make_status_map(status_map& output)
requires EmptyPack<Rest...>
{}

template<typename... Rest>
void make_status_map(
  status_map& output, ss::sstring name, lifecycle_status status, Rest... rest) {
    output.emplace(name, status);
    make_status_map(output, rest...);
}

template<typename... Args>
auto status_is(Args... args) {
    status_map m;
    make_status_map(m, args...);
    return ::testing::Eq(m);
}

} // namespace

class TransformManagerTest : public ::testing::Test {
public:
    void SetUp() override {
        auto r = std::make_unique<fake_registry>();
        _registry = r.get();
        auto t = std::make_unique<processor_tracker>();
        _tracker = t.get();
        _manager = std::make_unique<manager<ss::manual_clock>>(
          std::move(r), std::move(t));
    }
    void TearDown() override {
        _registry = nullptr;
        _tracker = nullptr;
        _manager.reset();
    }
    void become_leader(std::string_view np_str) {
        auto ntp = parse_ntp(np_str);
        _registry->add_leader(ntp);
        _manager->on_leadership_change(ntp, ntp_leader::yes);
    }
    void lose_leadership(std::string_view np_str) {
        auto ntp = parse_ntp(np_str);
        _registry->delete_leader(ntp);
        _manager->on_leadership_change(ntp, ntp_leader::no);
    }
    void deploy_transform(std::string_view name) {
        auto meta = parse_transform(name);
        auto id = _registry->put_transform(meta);
        _manager->on_plugin_change(id);
    }
    void delete_transform(std::string_view name) {
        auto meta = parse_transform(name);
        auto id = _registry->delete_transform(meta.name);
        _manager->on_plugin_change(id);
    }
    void drain_queue() { _manager->drain_queue_for_test().get(); }
    status_map status() {
        status_map result;
        auto statuses = _tracker->statuses();
        for (const auto& entry : _registry->all_transforms()) {
            auto name = entry.meta.name();
            if (entry.name_was_reused) {
                name += "#";
                name += std::to_string(entry.meta.source_ptr());
            }
            for (const auto& [key, status] : statuses) {
                auto [id, ntp] = key;
                if (id == entry.id) {
                    result.emplace(
                      ss::format("{}/{}", name, ntp.tp.partition()), status);
                }
            }
        }
        return result;
    }

private:
    model::ntp parse_ntp(std::string_view str) {
        std::vector<std::string_view> parts = absl::StrSplit(str, '/');
        if (parts.size() != 2) {
            throw std::runtime_error(ss::format("invalid ntp: {}", str));
        }
        int32_t partition_id = 0;
        if (!absl::SimpleAtoi(parts[1], &partition_id)) {
            throw std::runtime_error(
              ss::format("invalid partition: {}", parts[1]));
        }
        return {
          model::kafka_namespace,
          model::topic(parts[0]),
          model::partition_id(partition_id)};
    }

    /**
     * Parse a transform from a given string.
     *
     * Format is: (input_topic)->(output_topic)(#(version_number))?
     *
     * The version number is useful for multiple transforms that have been
     * deleted or created with the same name, we can refer to a specific one.
     */
    model::transform_metadata parse_transform(std::string_view str) {
        int version = 1;
        std::vector<std::string_view> version_parts = absl::StrSplit(str, "#");
        if (version_parts.size() == 2) {
            str = version_parts[0];
            if (!absl::SimpleAtoi(version_parts[1], &version)) {
                throw std::runtime_error(
                  ss::format("invalid version: {}", version_parts[1]));
            }
        } else if (version_parts.size() != 1) {
            throw std::runtime_error(
              ss::format("invalid transform name: {}", str));
        }
        std::vector<std::string_view> topic_parts = absl::StrSplit(str, "->");
        if (topic_parts.size() != 2) {
            throw std::runtime_error(ss::format("invalid transform: {}", str));
        }
        return {
          .name = model::transform_name(str),
          .input_topic = {model::kafka_namespace, model::topic(topic_parts[0])},
          .output_topics
          = {{model::kafka_namespace, model::topic(topic_parts[1])}},
          .environment = {},
          .uuid = uuid_t::create(),
          // As a hack to track the version, we use the source ptr
          .source_ptr = model::offset(version),
        };
    }

    fake_registry* _registry;
    processor_tracker* _tracker;
    std::unique_ptr<manager<ss::manual_clock>> _manager;
};

TEST_F(TransformManagerTest, FullLifecycle) {
    become_leader("foo/1");
    deploy_transform("foo->bar");
    drain_queue();
    EXPECT_THAT(status(), status_is("foo->bar/1", lifecycle_status::active));
    lose_leadership("foo/1");
    drain_queue();
    EXPECT_THAT(status(), status_is("foo->bar/1", lifecycle_status::destroyed));
}

TEST_F(TransformManagerTest, DeleteTransform) {
    become_leader("foo/1");
    deploy_transform("foo->bar");
    drain_queue();
    EXPECT_THAT(status(), status_is("foo->bar/1", lifecycle_status::active));
    delete_transform("foo->bar");
    drain_queue();
    EXPECT_THAT(status(), status_is("foo->bar/1", lifecycle_status::destroyed));
}

} // namespace transform
