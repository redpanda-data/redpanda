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

platform(
    name = "macos_x86_64",
    constraint_values = [
        "@platforms//os:macos",
        "@platforms//cpu:x86_64",
    ],
    visibility = ["//visibility:public"],
)

platform(
    name = "macos_aarch64",
    constraint_values = [
        "@platforms//os:macos",
        "@platforms//cpu:aarch64",
    ],
    visibility = ["//visibility:public"],
)
