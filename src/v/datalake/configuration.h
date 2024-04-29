#pragma once

#include <optional>
#include <string_view>

namespace datalake {
class topic_config {
public:
    // Return config, or nullopt if datalake is disabled for this topic.
    static std::optional<topic_config> get_config(const std::string_view topic);
};
}; // namespace datalake
