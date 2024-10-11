"""
This module contains the sources for all third party dependencies.
"""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def data_dependency():
    """
    Define third party dependency sources.

    `integrity` can be generated using:

    openssl dgst -sha256 -binary ARCHIVE.tar.gz | openssl base64 -A | sed 's/^/sha256-/'
    """
    http_archive(
        name = "ada",
        build_file = "//bazel/thirdparty:ada.BUILD",
        sha256 = "8e222d536d237269488f7d454544eedf12847f47b3d42651e8c9963c3fb0cf5e",
        strip_prefix = "ada-2.7.3",
        url = "https://vectorized-public.s3.us-west-2.amazonaws.com/dependencies/ada-2.7.3.tar.gz",
    )

    http_archive(
        name = "avro",
        build_file = "//bazel/thirdparty:avro.BUILD",
        sha256 = "f1a7d13b28ce5cc8812f26c705a6ea27b8bc63554d82d556c63b437da0338cf1",
        strip_prefix = "avro-e54bf712fce903652f3eab7a6c16264ac5d17285",
        url = "https://github.com/redpanda-data/avro/archive/e54bf712fce903652f3eab7a6c16264ac5d17285.tar.gz",
        patches = ["//bazel/thirdparty:avro-snappy-includes.patch"],
        patch_args = ["-p1"],
    )

    http_archive(
        name = "base64",
        build_file = "//bazel/thirdparty:base64.BUILD",
        sha256 = "b21be58a90d31302ba86056db7ef77a481393b9359c505be5337d7d54e8a0559",
        strip_prefix = "base64-0.5.0",
        url = "https://vectorized-public.s3.amazonaws.com/dependencies/base64-v0.5.0.tar.gz",
    )

    http_archive(
        name = "c-ares",
        build_file = "//bazel/thirdparty:c-ares.BUILD",
        sha256 = "321700399b72ed0e037d0074c629e7741f6b2ec2dda92956abe3e9671d3e268e",
        strip_prefix = "c-ares-1.19.1",
        url = "https://vectorized-public.s3.amazonaws.com/dependencies/c-ares-1.19.1.tar.gz",
    )

    http_archive(
        name = "hdrhistogram",
        build_file = "//bazel/thirdparty:hdrhistogram.BUILD",
        integrity = "sha256-u5U1GmqLJC3Jvh8oVidhqE1M8Kh0/8kKm2MHcKZGjpQ=",
        strip_prefix = "HdrHistogram_c-0.11.8",
        url = "https://github.com/HdrHistogram/HdrHistogram_c/archive/refs/tags/0.11.8.tar.gz",
    )

    http_archive(
        name = "hwloc",
        build_file = "//bazel/thirdparty:hwloc.BUILD",
        sha256 = "1f6d0f3edddd0070717f9f17e3a090a26d203e026c192144aa5f587725cf11e3",
        strip_prefix = "hwloc-hwloc-2.9.3",
        url = "https://vectorized-public.s3.amazonaws.com/dependencies/hwloc-2.9.3.tar.gz",
    )

    http_archive(
        name = "jsoncons",
        build_file = "//bazel/thirdparty:jsoncons.BUILD",
        sha256 = "078ba32cd1198cbeb1903fbf4881d4960b226bdf8083d9f5a927b96f0aa8d6dd",
        strip_prefix = "jsoncons-ffd2540bc9cfb54c16ef4d29d80622605d8dfbe8",
        url = "https://github.com/danielaparker/jsoncons/archive/ffd2540bc9cfb54c16ef4d29d80622605d8dfbe8.tar.gz",
    )

    http_archive(
        name = "krb5",
        build_file = "//bazel/thirdparty:krb5.BUILD",
        sha256 = "ec3861c3bec29aa8da9281953c680edfdab1754d1b1db8761c1d824e4b25496a",
        strip_prefix = "krb5-krb5-1.20.1-final",
        url = "https://vectorized-public.s3.us-west-2.amazonaws.com/dependencies/krb5-krb5-1.20.1-final.tar.gz",
    )

    http_archive(
        name = "libpciaccess",
        build_file = "//bazel/thirdparty:libpciaccess.BUILD",
        sha256 = "d0d0d53c2085d21ab37ae5989e55a3de13d4d80dc2c0a8d5c77154ea70f4783c",
        strip_prefix = "libpciaccess-2ec2576cabefef1eaa5dd9307c97de2e887fc347",
        url = "https://gitlab.freedesktop.org/xorg/lib/libpciaccess/-/archive/2ec2576cabefef1eaa5dd9307c97de2e887fc347/libpciaccess-2ec2576cabefef1eaa5dd9307c97de2e887fc347.tar.gz",
    )

    http_archive(
        name = "libprotobuf_mutator",
        build_file = "//bazel/thirdparty:libprotobuf-mutator.BUILD",
        integrity = "sha256-KWUbFgNpDJtAO6Kr0eTo1v6iczEOta72jSle9oivFhg=",
        strip_prefix = "libprotobuf-mutator-b922c8ab9004ef9944982e4f165e2747b13223fa",
        url = "https://github.com/google/libprotobuf-mutator/archive/b922c8ab9004ef9944982e4f165e2747b13223fa.zip",
    )

    http_archive(
        name = "libxml2",
        build_file = "//bazel/thirdparty:libxml2.BUILD",
        sha256 = "cdf9f952582c32a00513468585fb517270047ac0cfaed013ef08aa7b775fd6b4",
        strip_prefix = "libxml2-3b1742b8391e966be780bdc43fdf959f7b3a118c",
        url = "https://github.com/GNOME/libxml2/archive/3b1742b8391e966be780bdc43fdf959f7b3a118c.tar.gz",
    )

    http_archive(
        name = "lksctp",
        build_file = "//bazel/thirdparty:lksctp.BUILD",
        sha256 = "0c8fac0a5c66eea339dce6be857101b308ce1064c838b81125b0dde3901e8032",
        strip_prefix = "lksctp-tools-lksctp-tools-1.0.19",
        url = "https://vectorized-public.s3.amazonaws.com/dependencies/lksctp-tools-1.0.19.tar.gz",
    )

    http_archive(
        name = "numactl",
        build_file = "//bazel/thirdparty:numactl.BUILD",
        sha256 = "1ee27abd07ff6ba140aaf9bc6379b37825e54496e01d6f7343330cf1a4487035",
        strip_prefix = "numactl-2.0.14",
        url = "https://vectorized-public.s3.amazonaws.com/dependencies/numactl-v2.0.14.tar.gz",
    )

    #
    # ** IMPORTANT - OpenSSL and FIPS **
    #
    # Below there are two OpenSSL archives that are retrieved. The first, named
    # simply "openssl", may reference any desired version of OpenSSL 3.0.0 and above.
    #
    # The second archive retrieved is named "openssl-fips", and *MUST* reference
    # the specific version of OpenSSL, 3.0.9, which is the latest FIPS approved
    # version as of 2/26/24. Do not change this version. For more info visit:
    # https://csrc.nist.gov/projects/cryptographic-module-validation-program/certificate/4282
    #
    # This 2 build approach is described in more detail in the FIPS README here:
    # https://github.com/openssl/openssl/blob/master/README-FIPS.md
    #
    http_archive(
        name = "openssl",
        build_file = "//bazel/thirdparty:openssl.BUILD",
        sha256 = "23c666d0edf20f14249b3d8f0368acaee9ab585b09e1de82107c66e1f3ec9533",
        strip_prefix = "openssl-3.0.15",
        url = "https://github.com/openssl/openssl/releases/download/openssl-3.0.15/openssl-3.0.15.tar.gz",
    )

    http_archive(
        name = "openssl-fips",
        build_file = "//bazel/thirdparty:openssl-fips.BUILD",
        sha256 = "eb1ab04781474360f77c318ab89d8c5a03abc38e63d65a603cabbf1b00a1dc90",
        strip_prefix = "openssl-3.0.9",
        url = "https://vectorized-public.s3.us-west-2.amazonaws.com/dependencies/openssl-3.0.9.tar.gz",
    )

    http_archive(
        name = "rapidjson",
        build_file = "//bazel/thirdparty:rapidjson.BUILD",
        sha256 = "d085ef6d175d9b20800958c695c7767d65f9c1985a73d172150e57e84f6cd61c",
        strip_prefix = "rapidjson-14a5dd756e9bef26f9b53d3b4eb1b73c6a1794d5",
        url = "https://github.com/redpanda-data/rapidjson/archive/14a5dd756e9bef26f9b53d3b4eb1b73c6a1794d5.tar.gz",
    )

    http_archive(
        name = "roaring",
        build_file = "//bazel/thirdparty:roaring.BUILD",
        sha256 = "78487658b774f27546e79de2ddd37fca56679b23f256425d2c86aabf7d1b8066",
        strip_prefix = "CRoaring-c433d1c70c10fb2e40f049e019e2abbcafa6e69d",
        url = "https://github.com/redpanda-data/CRoaring/archive/c433d1c70c10fb2e40f049e019e2abbcafa6e69d.tar.gz",
    )

    http_archive(
        name = "seastar",
        build_file = "//bazel/thirdparty:seastar.BUILD",
        sha256 = "948e5f8ad769114a4721c81d5829f675ac51482e4a9a106d57ab313104308666",
        strip_prefix = "seastar-4350d7960ec5ac8a1cf9274316c8f1ab1896d24e",
        url = "https://github.com/redpanda-data/seastar/archive/4350d7960ec5ac8a1cf9274316c8f1ab1896d24e.tar.gz",
        patches = ["//bazel/thirdparty:seastar-fortify-source.patch"],
        patch_args = ["-p1"],
    )

    http_archive(
        name = "unordered_dense",
        build_file = "//bazel/thirdparty:unordered_dense.BUILD",
        sha256 = "8393d08b2a41949c70345926515036df55643e80118b608bcec6f4202d4a3026",
        strip_prefix = "unordered_dense-f30ed41b58af8c79788e8581fe57a6faf856258e",
        url = "https://github.com/martinus/unordered_dense/archive/f30ed41b58af8c79788e8581fe57a6faf856258e.tar.gz",
    )

    http_archive(
        name = "wasmtime",
        build_file = "//bazel/thirdparty:wasmtime.BUILD",
        sha256 = "a7f989b170d109696b928b4b3d1ec1d930064af7df47178e1341bd96e5c34465",
        strip_prefix = "wasmtime-9e1084ffac08b1bf9c82de40c0efc1baff14b9ad",
        url = "https://github.com/bytecodealliance/wasmtime/archive/9e1084ffac08b1bf9c82de40c0efc1baff14b9ad.tar.gz",
    )

    http_archive(
        name = "xxhash",
        build_file = "//bazel/thirdparty:xxhash.BUILD",
        sha256 = "716fbe4fc85ecd36488afbbc635b59b5ab6aba5ed3b69d4a32a46eae5a453d38",
        strip_prefix = "xxHash-bbb27a5efb85b92a0486cf361a8635715a53f6ba",
        url = "https://github.com/Cyan4973/xxHash/archive/bbb27a5efb85b92a0486cf361a8635715a53f6ba.tar.gz",
    )
