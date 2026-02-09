"""
Configuration for macOS cross-compilation toolchain.

This file will be used to configure the LLVM toolchain for macOS hosts
cross-compiling to Linux targets.
"""

# Once the LLVM build completes, we'll have a tarball at:
# /tmp/llvm-build/llvm-20.1.8-darwin-aarch64-YYYY-MM-DD.tar.zst
#
# The configuration will be:
#
# llvm.toolchain(
#     name = "macos_toolchain",
#     compile_flags = COMPILE_FLAGS,
#     cxx_standard = {"": CXX_STANDARD},
#     llvm_version = "20.1.8",
#     sha256 = {
#         "darwin-aarch64": "<SHA256_WILL_BE_CALCULATED>",
#     },
#     urls = {
#         "darwin-aarch64": ["<GITHUB_RELEASE_URL>"],
#     },
#     strip_prefix = {
#         "darwin-aarch64": "",  # No prefix needed if packed correctly
#     },
# )
#
# llvm.sysroot(
#     name = "macos_toolchain",
#     label = "@x86_64_sysroot//:sysroot",
#     targets = ["linux-x86_64"],
# )
#
# llvm.sysroot(
#     name = "macos_toolchain",
#     label = "@aarch64_sysroot//:sysroot",
#     targets = ["linux-aarch64"],
# )
#
# use_repo(llvm, "macos_toolchain")
# use_repo(llvm, "macos_toolchain_llvm")
#
# register_toolchains(
#     "@macos_toolchain//:all",
#     dev_dependency = True,
# )

# Testing with local file:
# urls = {"darwin-aarch64": ["file:///tmp/llvm-build/llvm-20.1.8-darwin-aarch64-YYYY-MM-DD.tar.zst"]}
