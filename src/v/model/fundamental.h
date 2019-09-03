#pragma once

#include <cstdint>
#include <limits>

namespace model {

struct partition {
    using type = int32_t;
    static constexpr const type min = std::numeric_limits<type>::min();
    partition() noexcept = default;
    constexpr explicit partition(type id) noexcept
      : value(id) {
    }
    const type value = min;
};

class topic_view {
public:
    explicit topic_view(std::string_view topic_name) noexcept
      : _topic_name(topic_name) {
    }

    std::string_view name() const {
        return _topic_name;
    }

    bool operator==(const topic_view& other) const {
        return _topic_name == other._topic_name;
    }

    bool operator!=(const topic_view& other) const {
        return !(*this == other);
    }

private:
    std::string_view _topic_name;
};

struct topic {
    topic() noexcept = default;
    explicit topic(sstring topic_name) noexcept
      : name(std::move(topic_name)) {
    }

    topic_view view() const {
        return topic_view(name);
    }

    bool operator==(const topic& other) const {
        return name == other.name;
    }

    bool operator!=(const topic& other) const {
        return !(*this == other);
    }

    const sstring name;
};

/// \brief namespace is reserved in c++;  use ns
struct ns {
    ns() noexcept = default;
    explicit ns(sstring namespace_name) noexcept
      : name(std::move(namespace_name)) {
    }

    bool operator==(const topic& other) const {
        return name == other.name;
    }

    bool operator!=(const topic& other) const {
        return !(*this == other);
    }

    const sstring name;
};
} // namespace model
