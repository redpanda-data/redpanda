/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/object_utils.h"

#include "base/vassert.h"
#include "ssx/sformat.h"

#include <charconv>

constexpr auto level_zero_data_dir_str = "level_zero/data/";

/*
 * We are using full-width int64 padding here. Using that many digits means
 * we don't need to think at all about the choice in terms of future growth.
 * We can revisit the width if for some reason we want a lower limit.
 */
constexpr size_t epoch_digits = 18;
static_assert(std::numeric_limits<int64_t>::digits10 == epoch_digits);

constexpr size_t prefix_digits = 3;

namespace cloud_topics {

cloud_storage_clients::object_key
object_path_factory::level_zero_path(object_id id) {
    vassert(id.epoch() >= 0, "level zero object has negative epoch: {}", id);
    return cloud_storage_clients::object_key(
      ssx::sformat(
        "{0}{5:0{4}}/{2:0{1}}/{3}",
        level_zero_data_dir_str,
        epoch_digits,
        id.epoch(),
        id.name,
        prefix_digits,
        id.prefix));
}

cloud_storage_clients::object_key
object_path_factory::level_zero_path_prefix(std::string_view ext) {
    return cloud_storage_clients::object_key{
      ssx::sformat("{}{}", level_zero_data_dir_str, ext)};
}

cloud_storage_clients::object_key object_path_factory::level_zero_data_dir() {
    return cloud_storage_clients::object_key(level_zero_data_dir_str);
}

std::expected<cluster_epoch, std::string>
object_path_factory::level_zero_path_to_epoch(std::string_view key) {
    // find the level zero prefix and chop it off
    auto name = key;
    auto it = name.find(level_zero_data_dir_str);
    if (it == std::string_view::npos) {
        return std::unexpected(
          fmt::format("L0 object name missing prefix: {}", key));
    }
    name.remove_prefix(it + std::strlen(level_zero_data_dir_str));

    if (name.size() < prefix_digits + 1) {
        return std::unexpected(
          fmt::format("L0 object name is too short: {}", key));
    }
    name.remove_prefix(prefix_digits + 1);

    if (name.size() < epoch_digits) {
        return std::unexpected(
          fmt::format("L0 object name is too short: {}", key));
    }

    // remove the tail so that all should be left is the epoch
    name.remove_suffix(name.size() - epoch_digits);

    // parse the epoch into an integer
    int64_t epoch{0};
    auto res = std::from_chars(name.data(), name.data() + name.size(), epoch);
    if (res.ptr != name.data() + name.size() || res.ec != std::errc{}) {
        return std::unexpected(
          fmt::format("L0 object name has invalid epoch: {}", key));
    }

    return cluster_epoch(epoch);
}

std::expected<object_id::prefix_t, std::string>
object_path_factory::level_zero_path_to_prefix(std::string_view key) {
    // find the level zero prefix and chop it off
    auto name = key;
    auto it = name.find(level_zero_data_dir_str);
    if (it == std::string_view::npos) {
        return std::unexpected(
          fmt::format("L0 object name missing prefix: {}", key));
    }
    name.remove_prefix(it + std::strlen(level_zero_data_dir_str));

    if (name.size() < prefix_digits + 1) {
        return std::unexpected(
          fmt::format("L0 object name is too short: {}", key));
    }
    name.remove_suffix(name.size() - prefix_digits);

    // parse the prefix into a uint16_t
    object_id::prefix_t pfx{0};
    auto res = std::from_chars(name.data(), name.data() + name.size(), pfx);
    if (res.ptr != name.data() + name.size() || res.ec != std::errc{}) {
        return std::unexpected(
          fmt::format("L0 object name has invalid prefix: {}", key));
    }

    return pfx;
}

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
prefix_range_inclusive::prefix_range_inclusive(T min, T max)
  : min(min)
  , max(max) {
    vassert(max <= t_max, "prefix_range: Invalid max: {}", max);
}

bool prefix_range_inclusive::contains(T v) const {
    return v >= min && v <= max;
}

bool prefix_range_inclusive::operator==(
  const prefix_range_inclusive& other) const {
    return min == other.min && max == other.max;
}

fmt::iterator prefix_range_inclusive::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "[{},{}]", min, max);
}

struct trie::node {
    node() = default;
    explicit node(char c, const node& parent)
      : is_leaf(true)
      , depth_(parent.depth_ + 1)
      , char_(c)
      , parent_(&parent) {}

    void insert(int v, std::string_view s) {
        vassert(
          s.size() <= prefix_digits, "String representation too long: {}", s);
        node* next = this;
        while (next != nullptr) {
            if (s.empty()) {
                next->min = next->max = v;
                break;
            }
            next->min = std::min(next->min, v);
            next->max = std::max(next->max, v);
            next->is_leaf = false;
            auto c = s[0];
            vassert(c >= '0' && c <= '9', "Invalid char: {}", c);
            size_t idx = c - '0';
            auto& child = next->children.at(idx);
            if (child == nullptr) {
                child = std::make_unique<node>(c, *next);
            }
            next = child.get();
            s = s.substr(1);
        }
    }

    ss::sstring str() const {
        ss::sstring result{ss::sstring::initialized_later{}, prefix_digits};
        size_t i{0};
        for (const auto* curr{this};
             curr->parent_ != nullptr && i < result.size();
             curr = curr->parent_, i++) {
            result[i] = curr->char_;
        }
        result.resize(i);
        std::ranges::reverse(result);
        return result;
    }

    fmt::iterator format_to(fmt::iterator it) const {
        if (depth_ == 0) {
            it = fmt::format_to(it, "ROOT ");
        } else {
            it = fmt::format_to(it, "{}: D_{} ", char_, depth_);
        }
        it = fmt::format_to(
          it, "[{},{}] '{}'\n", min, max, is_leaf ? str() : "...");
        for (const auto& child : children) {
            if (child == nullptr) {
                continue;
            }
            it = fmt::format_to(it, "{0:\t>{1}}: {2}", "", depth_ + 1, *child);
        }
        return it;
    }

    bool saturated() const {
        static constexpr std::array<int, 4> saturation = {999, 99, 9, 0};
        if (is_leaf) {
            return true;
        }

        vassert(min <= max, "Range should be increasing: [{},{}]", min, max);
        vassert(depth_ < saturation.size(), "Trie node too deep: {}", depth_);
        return (max - min) == saturation.at(depth_);
    }

    void prune() {
        traverse([](node& n) -> bool {
            if (n.saturated()) {
                n.is_leaf = true;
                n.children = {};
                return false;
            }
            return true;
        });
    }

    chunked_vector<ss::sstring> collect_prefixes() const {
        chunked_vector<ss::sstring> result;
        traverse([&result](const node& n) -> bool {
            if (n.is_leaf) {
                result.emplace_back(n.str());
                return false;
            }
            return true;
        });
        return result;
    }

private:
    /**
     * @brief Traverse the trie in an iterative depth first fashion, applying
     * some function to each node.
     *
     * @param fn - The function to apply to each node. Should return true if the
     * traversal should continue past this point, false otherwise.
     */
    void traverse(const std::function<bool(node&)>& fn) {
        chunked_vector<node*> stk;
        stk.push_back(this);
        while (!stk.empty()) {
            auto* node = stk.back();
            vassert(node != nullptr, "Node pointer unexpectedly null");
            stk.pop_back();
            if (!std::invoke(fn, *node)) {
                continue;
            }
            std::ranges::transform(
              node->children | std::views::filter([](const auto& n) {
                  return n != nullptr;
              }),
              std::back_inserter(stk),
              [](const auto& c) { return c.get(); });
        }
    }

    /**
     * @brief Traverse the trie in an iterative depth first fashion, applying
     * some function to each node. Const version.
     *
     * @param fn - The function to apply to each node. Should return true if the
     * traversal should continue past this point, false otherwise.
     */
    void traverse(std::function<bool(const node&)> fn) const {
        chunked_vector<const node*> stk;
        stk.push_back(this);
        while (!stk.empty()) {
            auto* node = stk.back();
            vassert(node != nullptr, "Node pointer unexpectedly null");
            stk.pop_back();
            if (!std::invoke(fn, *node)) {
                continue;
            }
            std::ranges::transform(
              node->children | std::views::filter([](const auto& n) {
                  return n != nullptr;
              }),
              std::back_inserter(stk),
              [](const auto& c) { return c.get(); });
        }
    }

    int min{std::numeric_limits<int>::max()};
    int max{std::numeric_limits<int>::min()};
    std::array<std::unique_ptr<node>, 10> children{};
    // is_leaf is false by default so an empty trie isn't treated as saturated
    bool is_leaf{false};
    size_t depth_{0};
    char char_{'\0'};
    const node* parent_{nullptr};
};

trie::trie()
  : root(std::make_unique<node>()) {}
trie::~trie() = default;

void trie::insert(prefix_range_inclusive prefixes) {
    ss::sstring buf;
    for (auto i = prefixes.min; i <= prefixes.max; ++i) {
        buf = ssx::sformat("{0:0{1}}", i, prefix_digits);
        root->insert(i, buf);
    }
}

void trie::prune() { root->prune(); }

chunked_vector<ss::sstring> trie::collect() const {
    return root->collect_prefixes();
}

void trie::clear() { std::exchange(root, std::make_unique<node>()); }

fmt::iterator trie::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{}", *root);
}

void prefix_compressor::set_range(std::optional<prefix_range_inclusive> range) {
    if (range_ == range) {
        // nothing changed, nothing to do
        return;
    }
    range_ = range;
    trie_.clear();
    raw_prefixes_.clear();
    prefix_idx_.reset();
    if (range_.has_value()) {
        trie_.insert(range_.value());
        trie_.prune();
        raw_prefixes_ = trie_.collect();
        prefix_idx_ = 0;
    }
}

std::optional<cloud_storage_clients::object_key>
prefix_compressor::consume_prefix() {
    if (!prefix_idx_.has_value() || raw_prefixes_.empty()) {
        return std::nullopt;
    }
    if (prefix_idx_.value() >= raw_prefixes_.size()) {
        prefix_idx_ = 0;
        return std::nullopt;
    }
    return object_path_factory::level_zero_path_prefix(
      raw_prefixes_.at(prefix_idx_.value()++));
}

chunked_vector<cloud_storage_clients::object_key>
prefix_compressor::compressed_key_prefixes() const {
    return raw_prefixes_ | std::views::transform([](const ss::sstring& s) {
               return object_path_factory::level_zero_path_prefix(s);
           })
           | std::ranges::to<
             chunked_vector<cloud_storage_clients::object_key>>();
}

} // namespace cloud_topics
