load("@bazel_skylib//rules:common_settings.bzl", "string_flag")
load("@rules_go//go:def.bzl", "go_binary", "go_library")
load("@rules_oci//oci:defs.bzl", "oci_image", "oci_load")
load(":packaging.bzl", "native_package", "redpanda_package")

redpanda_package(
    name = "redpanda_tar",
    out = "redpanda.tar.gz",
    default_yaml_config = "//conf:redpanda.yaml",
    include_sysroot_libs = True,
    redpanda_binary = "//:redpanda",
    rpath_override = "$ORIGIN/../lib",
    rpk_binary = "//:rpk",
)

# Flag to control the install path of the redpanda package.
# This flag is used to set the interpret path for redpanda binary.
string_flag(
    name = "install_path",
    build_setting_default = "/opt/redpanda",
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
    rpk_binary = "//:rpk",
)

redpanda_package(
    name = "ducktape",
    out = "redpanda_ducktape",
    include_sysroot_libs = True,
    install_path = ":install_path",
    redpanda_binary = "//:redpanda",
    rpath_override = "$ORIGIN/../lib",
    rpk_binary = "//:rpk",
)

# This is the ID for the nonroot user in the distroless containers.
NONROOT_USER = 65532

redpanda_package(
    name = "oci_image_contents",
    out = "redpanda_nonroot.tar.gz",
    default_yaml_config = "//conf:redpanda.yaml",
    owner = NONROOT_USER,
    redpanda_binary = "//:redpanda",
    rpath_override = "$ORIGIN/../lib",
    rpk_binary = "//:rpk",
)

native_package(
    name = "direct_consumer_verifier_ducktape",
    out = "direct_consumer_verifier_ducktape",
    cc_binaries = [
        "//src/v/kafka/client/direct_consumer/verifier:direct_consumer_verifier",
    ],
    include_sysroot_libs = True,
    install_path = ":install_path",
    rpath_override = "$ORIGIN/../lib",
)

native_package(
    name = "direct_consumer_verifier_tar",
    out = "direct_consumer_verifier.tar.gz",
    cc_binaries = [
        "//src/v/kafka/client/direct_consumer/verifier:direct_consumer_verifier",
    ],
    include_sysroot_libs = True,
    install_path = ":install_path",
    rpath_override = "$ORIGIN/../lib",
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
)
