load("@rules_go//go:def.bzl", "go_binary", "go_library")
load("@rules_oci//oci:defs.bzl", "oci_image", "oci_load")
load(":packaging.bzl", "redpanda_deb_package", "redpanda_package", "script_package")

# this is the real binary. the rpk alias at the root //:rpk is a wrapper.
alias(
    name = "rpk",
    actual = "//src/go/rpk/cmd/rpk:rpk",
)

# Sysroot libs and loader shipped alongside packaged binaries, selected per
# target arch from the sysroot repos defined in //bazel:repositories.bzl.
alias(
    name = "sysroot_runtime",
    actual = select({
        "@platforms//cpu:x86_64": "@x86_64_sysroot_runtime//:runtime",
        "@platforms//cpu:aarch64": "@aarch64_sysroot_runtime//:runtime",
    }),
)

redpanda_package(
    name = "redpanda_tar",
    out = "redpanda.tar.gz",
    default_yaml_config = "//conf:redpanda.yaml",
    include_sysroot_libs = True,
    redpanda_binary = "//:redpanda",
    rpk_binary = ":rpk",
    sysroot_runtime = ":sysroot_runtime",
)

redpanda_package(
    name = "vtools_input",
    out = "bazel_out.tar.gz",
    fips_config = "@openssl-fips//:fipsmodule_cnf",
    fips_module = "@openssl-fips//:fipsmodule_so",
    hwloc_calc = "@hwloc//:hwloc_calc",
    hwloc_distrib = "@hwloc//:hwloc_distrib",
    include_sysroot_libs = True,
    iotune = "@seastar//:iotune",
    openssl = "@openssl//:openssl_binary",
    redpanda_binary = "//:redpanda",
    rpath_override = "$ORIGIN/../lib",
    rpk_binary = ":rpk",
    sysroot_runtime = ":sysroot_runtime",
)

# This is "bazel-dir" packaging mode used for local DT tests.
redpanda_package(
    name = "ducktape",
    out = "redpanda_ducktape",
    fips_config = "@openssl-fips//:fipsmodule_cnf",
    fips_module = "@openssl-fips//:fipsmodule_so",
    include_sysroot_libs = True,
    install_path = "/opt/redpanda_installs/redpanda",
    redpanda_binary = "//:redpanda",
    rpk_binary = ":rpk",
    sysroot_runtime = ":sysroot_runtime",
)

# This is the ID for the nonroot user in the distroless containers.
NONROOT_USER = 65532

redpanda_package(
    name = "oci_image_contents",
    out = "redpanda_nonroot.tar.gz",
    default_yaml_config = "//conf:redpanda.yaml",
    owner = NONROOT_USER,
    redpanda_binary = "//:redpanda",
    rpk_binary = ":rpk",
)

redpanda_deb_package(
    name = "redpanda_deb",
    package_name = "redpanda",
    out = "redpanda.deb",
    depends = [
        "redpanda-rpk",
        "redpanda-tuner",
    ],
    install_path = "/opt/redpanda",
    package_dirs = [
        "/var/lib/redpanda",
        "/etc/redpanda",
    ],
    postinst = "scripts/redpanda.postinst",
    postrm = "scripts/redpanda.postrm",
    preinst = "scripts/redpanda.preinst",
    prerm = "scripts/redpanda.prerm",
    redpanda_binary = "//:redpanda",
    sysroot_runtime = ":sysroot_runtime",
    systemd_service = "scripts/redpanda.service",
    systemd_slice = "scripts/redpanda.slice",
)

redpanda_deb_package(
    name = "redpanda_rpk_deb",
    package_name = "redpanda-rpk",
    out = "redpanda-rpk.deb",
    description = "Redpanda CLI & toolbox",
    include_sysroot_libs = False,
    install_path = "/opt/redpanda",
    package_symlinks = {
        "/usr/bin/rpk": "/opt/redpanda/bin/rpk",
    },
    rpk_binary = "//src/go/rpk/cmd/rpk:rpk",
)

redpanda_deb_package(
    name = "redpanda_tuner_deb",
    package_name = "redpanda-tuner",
    out = "redpanda-tuner.deb",
    description = "Redpanda tuner",
    hwloc_calc = "@hwloc//:hwloc_calc",
    hwloc_distrib = "@hwloc//:hwloc_distrib",
    include_sysroot_libs = False,
    install_path = "/usr",
    systemd_service = "scripts/redpanda-tuner.service",
)

filegroup(
    name = "all_debs",
    srcs = [
        ":redpanda_deb",
        ":redpanda_rpk_deb",
        ":redpanda_tuner_deb",
    ],
    visibility = ["//visibility:public"],
)

script_package(
    name = "controller_forced_reconfiguration_ducktape",
    out = "controller_forced_reconfiguration_ducktape",
    scripts = {
        "//tools/controller_forced_reconfiguration:invoke_controller_reconfiguration.py": "invoke_controller_reconfiguration",
    },
)

# NOTE: this image is currently experimental, don't use this for production.
oci_image(
    name = "image",
    base = "@distroless_cc_debian12",
    cmd = [
        "redpanda",
        "start",
        "--overprovisioned",
    ],
    entrypoint = ["/opt/redpanda/bin/rpk"],
    exposed_ports = [
        "8081",
        "8082",
        "9092",
        "9644",
    ],
    labels = {"org.opencontainers.image.authors": "Redpanda Data <hi@redpanda.com>"},
    tars = [
        ":oci_image_contents",
    ],
    user = "nonroot",
    volumes = ["/var/lib/redpanda/data"],
)

oci_load(
    name = "image_load",
    image = ":image",
    repo_tags = ["redpandadata/redpanda-dev:latest"],
)

go_binary(
    name = "tool",
    embed = [":packaging_lib"],
    visibility = ["//visibility:private"],
)

go_library(
    name = "packaging_lib",
    srcs = ["tool.go"],
    importpath = "github.com/redpanda-data/redpanda/bazel/packaging",
    visibility = ["//visibility:private"],
    deps = [
        "@com_github_goreleaser_nfpm_v2//:nfpm",
        "@com_github_goreleaser_nfpm_v2//deb",
        "@com_github_goreleaser_nfpm_v2//files",
    ],
)
