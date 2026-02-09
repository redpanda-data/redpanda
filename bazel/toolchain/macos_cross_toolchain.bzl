"""
macOS cross-compilation toolchain configuration.

This module configures LLVM toolchains for cross-compiling from macOS to Linux.
"""

def register_macos_cross_toolchains():
    """Register macOS to Linux cross-compilation toolchains.

    This should be called from MODULE.bazel after the LLVM extension is configured.
    It sets up toolchains that use local Homebrew LLVM for execution on macOS
    while targeting Linux platforms with hermetic sysroots.
    """
    # This is a placeholder for now - the actual configuration
    # will be done directly in MODULE.bazel with llvm.toolchain()
    pass
