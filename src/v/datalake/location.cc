/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "datalake/location.h"

#include <optional>

namespace datalake {

scoped_location
scoped_location::append_path(const std::filesystem::path& path) const {
    auto new_location = *this;
    new_location.path_ /= path.relative_path();
    return new_location;
}

scoped_location
scoped_location::replace_path(const std::filesystem::path& path) const {
    auto relative_path = path.relative_path();
    auto new_location = *this;

    if (!relative_path.string().starts_with(scope_key_.native())) {
        throw std::invalid_argument(fmt::format(
          "Path {} is not rooted with scope key {}",
          relative_path,
          scope_key_));
    } else {
        // Remove the key from the path as it is already part of the `scope`.
        auto key_size = scope_key_.native().size();
        if (relative_path.native().size() > key_size) {
            new_location.path_ = relative_path.native().substr(key_size + 1);
        } else {
            // The path is equal to the key, so we set the path to empty.
            new_location.path_ = "";
        }
    }

    return new_location;
}

iceberg::uri scoped_location::to_uri() const {
    if (path_.empty()) {
        return scope_;
    } else {
        return iceberg::uri(fmt::format("{}/{}", scope_, path_.native()));
    }
}

std::filesystem::path scoped_location::to_key() const {
    if (path_.empty()) {
        return scope_key_;
    } else {
        std::cout << "scope_key_: " << scope_key_ << std::endl;
        std::cout << "path_: " << path_ << std::endl;
        return scope_key_ / path_;
    }
}

std::optional<scoped_location>
location_provider::make_scoped_location(const iceberg::uri& uri) const {
    return uri_converter_.from_uri(bucket_, uri)
      .and_then([&uri](std::filesystem::path p) {
          std::string_view uri_without_trailing_slash = uri();
          while (uri_without_trailing_slash.back() == '/') {
              uri_without_trailing_slash.remove_suffix(1);
          }

          while (p.native().back() == '/') {
              p = p.parent_path();
          }

          // We successfully parsed the key from the uri. We create a scoped
          // location by remembering the original uri format and the parsed
          // key.
          return std::make_optional<datalake::scoped_location>(
            datalake::scoped_location::ctor_key{},
            iceberg::uri(uri_without_trailing_slash),
            p);
      });
}

} // namespace datalake
