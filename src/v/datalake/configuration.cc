#include "datalake/configuration.h"

#include "configuration.h"

#include <iostream>
#include <optional>
#include <string>

std::optional<datalake::topic_config>
datalake::topic_config::get_config(const std::string_view topic) {
    if (!topic.starts_with("experimental_datalake_")) {
        return std::nullopt;
    }
    return datalake::topic_config();
}
