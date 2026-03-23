# Extensible fixup rules for nixifying Bazel-extracted repos.
#
# After `bazel fetch` extracts archives into output_base/external/,
# the nixify pipeline applies these rules to make everything work
# in the Nix sandbox.
#
# Rule types:
#   patchelf      — fix ELF interpreter and rpath for Nix sandbox
#   fix-shebangs  — replace hardcoded shebangs with Nix paths
#   patch         — apply patch files to specific module directories
#   substitute    — sed-style replacements in specific files
#
# Designed to be reusable across Bazel projects — add interpreter
# entries to support new scripting languages, add module-shebang
# entries for modules that need shebangs fixed at extraction time.
#
{ lib, bash, perl, python3, glibc, gcc-unwrapped, zlib, openssl, curl }:

let
  nixInterp = "${glibc}/lib/ld-linux-x86-64.so.2";
  nixRpath = lib.concatStringsSep ":" [
    "${glibc}/lib"
    "${gcc-unwrapped.lib}/lib"
    "${zlib}/lib"
    "${openssl.out}/lib"
    "${curl.out}/lib"
  ];
in
[
  # ── Global fixups (applied to ALL extracted repos) ──

  {
    type = "patchelf";
    interpreter = nixInterp;
    rpath = nixRpath;
  }

  # ── Interpreter map ──
  # Table of known interpreters and their Nix store paths.
  # The generic shebang patcher uses this to resolve shebangs like:
  #   #!/usr/bin/env bash  →  #!/nix/store/.../bin/bash
  #   #! /bin/perl          →  #!/nix/store/.../bin/perl
  #   #!/usr/bin/python3    →  #!/nix/store/.../bin/python3
  #
  # Matching is resilient: handles optional spaces after #!, various
  # path prefixes (/usr/bin/env, /bin, /usr/bin, /usr/local/bin),
  # and normalizes all to the Nix store path.
  #
  # To add a new interpreter, just add an entry to this list.
  {
    type = "fix-shebangs";
    interpreters = {
      bash    = "${bash}/bin/bash";
      sh      = "${bash}/bin/bash";
      perl    = "${perl}/bin/perl";
      python  = "${python3}/bin/python3";
      python3 = "${python3}/bin/python3";
    };
  }

  # ── Module-specific shebang fixes ──
  # These are applied via single_version_override patch_cmds in
  # MODULE.bazel. They run during Bazel's module extraction, so they
  # persist even if Bazel re-extracts the repo.
  #
  # Each entry specifies a module, the files to fix, and what interpreter
  # name to search for. The sed pattern `/^#!.*<name>/s|.*|#!<path>|`
  # replaces any line starting with #! that contains the interpreter name.
  # Handles all shebang variants AND embedded shebangs in .bzl templates.
  #
  # To add a fix for a new module, just add an entry here and re-run
  # patch-module-bazel.py.
  {
    type = "module-shebangs";
    modules = [
      {
        module = "rules_foreign_cc";
        interpreter = "bash";  # key into interpreters map above
        files = [
          "foreign_cc/private/framework/toolchains/linux_commands.bzl"
          "foreign_cc/private/framework/toolchains/macos_commands.bzl"
          "foreign_cc/private/framework/toolchains/freebsd_commands.bzl"
          "foreign_cc/private/runnable_binary_wrapper.sh"
        ];
      }
      {
        module = "rules_cc";
        interpreter = "bash";
        files = [
          "cc/private/toolchain/generate_system_module_map.sh"
          "cc/private/toolchain/grep-includes.sh"
          "cc/private/toolchain/link_dynamic_library.sh"
        ];
      }
    ];
  }

  # ── Per-module patches ──
  # Add entries here as needed, e.g.:
  #
  # {
  #   type = "patch";
  #   module = "rules_cc";
  #   patches = [ ./patches/rules_cc-nix.patch ];
  # }

  # ── Arbitrary substitutions ──
  # {
  #   type = "substitute";
  #   module = "some_module";
  #   file = "some/script.sh";
  #   from = "/usr/bin/python3";
  #   to = "${python3}/bin/python3";
  # }
]
