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
        sha256 = "bd89fcf57c93e965e6e2488448ab9d1cf8005311808c563b288f921d987e4924",
        url = "https://vectorized-public.s3.us-west-2.amazonaws.com/dependencies/ada-3.2.4.single-header.zip",
    )

    http_archive(
        name = "avro",
        build_file = "//bazel/thirdparty:avro.BUILD",
        sha256 = "791d9f163f458d0ba4c94251f58ef5af9157952a9569ce0968d89aeb585af34f",
        strip_prefix = "avro-46fe1e36f680d75219cba46368de38321f1810ed",
        url = "https://github.com/redpanda-data/avro/archive/46fe1e36f680d75219cba46368de38321f1810ed.tar.gz",
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
        sha256 = "912dd7cc3b3e8a79c52fd7fb9c0f4ecf0aaa73e45efda880266a2d6e26b84ef5",
        strip_prefix = "c-ares-1.34.6",
        url = "https://vectorized-public.s3.amazonaws.com/dependencies/c-ares-1.34.6.tar.gz",
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
        sha256 = "866ac8ef07b350a6a2ba0c6826c37d78e8994dcbcd443bdd2b436350de19d540",
        strip_prefix = "hwloc-2.11.2",
        url = "https://vectorized-public.s3.amazonaws.com/dependencies/hwloc-2.11.2.tar.gz",
    )

    http_archive(
        name = "jsoncons",
        build_file = "//bazel/thirdparty:jsoncons.BUILD",
        sha256 = "078ba32cd1198cbeb1903fbf4881d4960b226bdf8083d9f5a927b96f0aa8d6dd",
        strip_prefix = "jsoncons-ffd2540bc9cfb54c16ef4d29d80622605d8dfbe8",
        url = "https://github.com/danielaparker/jsoncons/archive/ffd2540bc9cfb54c16ef4d29d80622605d8dfbe8.tar.gz",
        patches = ["//bazel/thirdparty:jsoncons-pr-603.patch"],
        patch_args = ["-p1"],
    )

    http_archive(
        name = "krb5",
        build_file = "//bazel/thirdparty:krb5.BUILD",
        sha256 = "2157d92020d408ed63ebcd886a92d1346a1383b0f91123a0473b4f69b4a24861",
        strip_prefix = "krb5-krb5-1.21.3-final",
        url = "https://github.com/krb5/krb5/archive/refs/tags/krb5-1.21.3-final.tar.gz",
        patches = ["//bazel/thirdparty:0001-Fix-two-unlikely-memory-leaks.patch"],
        patch_args = ["-p1"],
    )

    http_archive(
        name = "libpciaccess",
        build_file = "//bazel/thirdparty:libpciaccess.BUILD",
        sha256 = "d0d0d53c2085d21ab37ae5989e55a3de13d4d80dc2c0a8d5c77154ea70f4783c",
        strip_prefix = "libpciaccess-2ec2576cabefef1eaa5dd9307c97de2e887fc347",
        url = "https://vectorized-public.s3.amazonaws.com/dependencies/libpciaccess-2ec2576cabefef1eaa5dd9307c97de2e887fc347.tar.gz",
    )

    http_archive(
        name = "libprotobuf_mutator",
        build_file = "//bazel/thirdparty:libprotobuf-mutator.BUILD",
        sha256 = "0847a2ee65552a92643131e934b50164e8b79fadc7be24b180a1c8d6dbb05952",
        strip_prefix = "libprotobuf-mutator-dc4ced337a9fb4047e2dc727268fbac55ca82f73",
        url = "https://github.com/google/libprotobuf-mutator/archive/dc4ced337a9fb4047e2dc727268fbac55ca82f73.zip",
    )

    http_archive(
        name = "libxml2",
        build_file = "//bazel/thirdparty:libxml2.BUILD",
        sha256 = "2769234c4fe2fab9b0b043e891c2af0f1ae51c8d4b94799472981e676ed8009e",
        strip_prefix = "libxml2-2.15.2",
        url = "https://vectorized-public.s3.us-west-2.amazonaws.com/dependencies/libxml2-v2.15.2.tar.gz",
    )

    http_archive(
        name = "lksctp",
        build_file = "//bazel/thirdparty:lksctp.BUILD",
        sha256 = "0c8fac0a5c66eea339dce6be857101b308ce1064c838b81125b0dde3901e8032",
        strip_prefix = "lksctp-tools-lksctp-tools-1.0.19",
        url = "https://vectorized-public.s3.amazonaws.com/dependencies/lksctp-tools-1.0.19.tar.gz",
    )

    http_archive(
        name = "ragel",
        build_file = "//bazel/thirdparty:ragel.BUILD",
        sha256 = "5f156edb65d20b856d638dd9ee2dfb43285914d9aa2b6ec779dac0270cd56c3f",
        strip_prefix = "ragel-6.10",
        url = "http://www.colm.net/files/ragel/ragel-6.10.tar.gz",
    )

    #
    # ** IMPORTANT - OpenSSL and FIPS **
    #
    # Below there are two OpenSSL archives that are retrieved. The first, named
    # simply "openssl", may reference any desired version of OpenSSL 3.0.0 and above.
    #
    # The second archive retrieved is named "openssl-fips", and *MUST* reference
    # the specific version of OpenSSL, 3.1.2, which is the latest FIPS approved
    # version as of 3/11/2025. Do not change this version. For more info visit:
    # https://csrc.nist.gov/projects/cryptographic-module-validation-program/certificate/4985
    #
    # This 2 build approach is described in more detail in the FIPS README here:
    # https://github.com/openssl/openssl/blob/master/README-FIPS.md
    #
    http_archive(
        name = "openssl",
        build_file = "//bazel/thirdparty:openssl.BUILD",
        sha256 = "b28c91532a8b65a1f983b4c28b7488174e4a01008e29ce8e69bd789f28bc2a89",
        strip_prefix = "openssl-3.5.5",
        url = "https://vectorized-public.s3.amazonaws.com/dependencies/openssl-3.5.5.tar.gz",
    )

    http_archive(
        name = "openssl-fips",
        build_file = "//bazel/thirdparty:openssl-fips.BUILD",
        sha256 = "a0ce69b8b97ea6a35b96875235aa453b966ba3cba8af2de23657d8b6767d6539",
        strip_prefix = "openssl-3.1.2",
        url = "https://vectorized-public.s3.amazonaws.com/dependencies/openssl-3.1.2.tar.gz",
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

    # branch: v26.2.x-pre
    http_archive(
        name = "seastar",
        build_file = "//bazel/thirdparty:seastar.BUILD",
        sha256 = "9c572c5111ef6b4f0afc6ba358696c0e2a78e118bc3c3617cf6454a9d98970de",
        strip_prefix = "seastar-617d3c421a949f449942d7ce0d275af0997efb48",
        url = "https://github.com/redpanda-data/seastar/archive/617d3c421a949f449942d7ce0d275af0997efb48.tar.gz",
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

    sysroot_build_file = """
filegroup(
  name = "sysroot",
  srcs = glob(["*/**"]),
  visibility = ["//visibility:public"],
)"""
    http_archive(
        name = "x86_64_sysroot",
        build_file_content = sysroot_build_file,
        sha256 = "282b7eb89ca45d2309217d5d2099cc087c1e7bd55f7891b9d2ddca648b6663b7",
        urls = ["https://github.com/redpanda-data/llvm-project/releases/download/llvmorg-19.1.7/sysroot-ubuntu-22.04-x86_64-2025-02-24.tar.zst"],
    )

    http_archive(
        name = "aarch64_sysroot",
        build_file_content = sysroot_build_file,
        sha256 = "39e3d368d57a40d36f6735dcfe3ed699c6a5962cd47c5b1f652254f077632688",
        urls = ["https://github.com/redpanda-data/llvm-project/releases/download/llvmorg-19.1.7/sysroot-ubuntu-22.04-aarch64-2025-02-27.tar.zst"],
    )
