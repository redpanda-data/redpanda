#pragma once
#include <string_view>

std::string_view redpanda_git_version() noexcept;

std::string_view redpanda_git_revision() noexcept;

std::string_view redpanda_version() noexcept;
