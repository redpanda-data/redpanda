"""
Platform definitions for cross-compilation.
"""

# ====================================
# Linux Platforms (Target)
# ====================================

platform(
    name = "linux_x86_64",
    constraint_values = [
        "@platforms//os:linux",
        "@platforms//cpu:x86_64",
    ],
    visibility = ["//visibility:public"],
)

platform(
    name = "linux_aarch64",
    constraint_values = [
        "@platforms//os:linux",
        "@platforms//cpu:aarch64",
    ],
    visibility = ["//visibility:public"],
)

# ====================================
# macOS Platforms (Exec/Host)
# ====================================
# Note: Using @platforms//os:osx (not macos) to match Rust platform constraints

platform(
    name = "macos_x86_64",
    constraint_values = [
        "@platforms//os:osx",
        "@platforms//cpu:x86_64",
    ],
    visibility = ["//visibility:public"],
)

platform(
    name = "macos_aarch64",
    constraint_values = [
        "@platforms//os:osx",
        "@platforms//cpu:aarch64",
    ],
    visibility = ["//visibility:public"],
)
