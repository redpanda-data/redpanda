load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def data_dependency():
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
        sha256 = "d4043000885812223c7a8a444781f9ded46aca4dc6216d86bbde18fd94c1562a",
        strip_prefix = "avro-836cb1004090435a985becda1daa619c407e1376",
        url = "https://github.com/redpanda-data/avro/archive/836cb1004090435a985becda1daa619c407e1376.tar.gz",
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
        name = "cryptopp",
        build_file = "//bazel/thirdparty:cryptopp.BUILD",
        sha256 = "11a6dbc749c27687ddf13195950589596f51361210a1f67e3c383cc4961f3e61",
        strip_prefix = "cryptopp-CRYPTOPP_8_7_0",
        url = "https://vectorized-public.s3.amazonaws.com/dependencies/CRYPTOPP_8_7_0.tar.xz",
    )

    http_archive(
        name = "gmp",
        build_file = "//bazel/thirdparty:gmp.BUILD",
        sha256 = "fd4829912cddd12f84181c3451cc752be224643e87fac497b69edddadc49b4f2",
        strip_prefix = "gmp-6.2.1",
        url = "https://vectorized-public.s3.amazonaws.com/dependencies/gmp-6.2.1.tar.xz",
    )

    http_archive(
        name = "gnutls",
        build_file = "//bazel/thirdparty:gnutls.BUILD",
        sha256 = "f74fc5954b27d4ec6dfbb11dea987888b5b124289a3703afcada0ee520f4173e",
        strip_prefix = "gnutls-3.8.3",
        url = "https://vectorized-public.s3.us-west-2.amazonaws.com/dependencies/gnutls-3.8.3.tar.xz",
    )

    http_archive(
        name = "hdrhistogram",
        build_file = "//bazel/thirdparty:hdrhistogram.BUILD",
        sha256 = "f81a192b62ae25bcebe63c9f3c74f371d04f88a74c1867532ec8a0012a9e482c",
        strip_prefix = "HdrHistogram_c-0.11.5",
        url = "https://vectorized-public.s3.amazonaws.com/dependencies/HdrHistogram_c-0.11.5.tar.gz",
    )

    http_archive(
        name = "hwloc",
        build_file = "//bazel/thirdparty:hwloc.BUILD",
        sha256 = "1f6d0f3edddd0070717f9f17e3a090a26d203e026c192144aa5f587725cf11e3",
        strip_prefix = "hwloc-hwloc-2.9.3",
        url = "https://vectorized-public.s3.amazonaws.com/dependencies/hwloc-2.9.3.tar.gz",
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
        sha256 = "9938b18509553452c6e13f79e16cbaffec9ea67119aa2de5d75e83c4f67ff400",
        strip_prefix = "libpciaccess-libpciaccess-0.16",
        url = "https://vectorized-public.s3.amazonaws.com/dependencies/libpciaccess-0.16.tar.bz2",
    )

    http_archive(
        name = "libxml2",
        build_file = "//bazel/thirdparty:libxml2.BUILD",
        sha256 = "ed0c91c5845008f1936739e4eee2035531c1c94742c6541f44ee66d885948d45",
        strip_prefix = "libxml2-2.10.4",
        url = "https://vectorized-public.s3.amazonaws.com/dependencies/libxml2-2.10.4.tar.xz",
    )

    http_archive(
        name = "lksctp",
        build_file = "//bazel/thirdparty:lksctp.BUILD",
        sha256 = "0c8fac0a5c66eea339dce6be857101b308ce1064c838b81125b0dde3901e8032",
        strip_prefix = "lksctp-tools-lksctp-tools-1.0.19",
        url = "https://vectorized-public.s3.amazonaws.com/dependencies/lksctp-tools-1.0.19.tar.gz",
    )

    http_archive(
        name = "nettle",
        build_file = "//bazel/thirdparty:nettle.BUILD",
        sha256 = "ccfeff981b0ca71bbd6fbcb054f407c60ffb644389a5be80d6716d5b550c6ce3",
        strip_prefix = "nettle-3.9.1",
        url = "https://vectorized-public.s3.amazonaws.com/dependencies/nettle-3.9.1.tar.gz",
        patches = ["//bazel/thirdparty:nettle.patch"],
        patch_args = ["-p1"],
    )

    http_archive(
        name = "numactl",
        build_file = "//bazel/thirdparty:numactl.BUILD",
        sha256 = "1ee27abd07ff6ba140aaf9bc6379b37825e54496e01d6f7343330cf1a4487035",
        strip_prefix = "numactl-2.0.14",
        url = "https://vectorized-public.s3.amazonaws.com/dependencies/numactl-v2.0.14.tar.gz",
    )

    ###############################################################################
    # IMPORTANT
    # DO NOT CHANGE THIS VERSION
    # As of 2/26 - 3.0.9 is the latest FIPS _approved_ version:
    # https://csrc.nist.gov/projects/cryptographic-module-validation-program/certificate/4282
    ###############################################################################
    http_archive(
        name = "openssl",
        build_file = "//bazel/thirdparty:openssl.BUILD",
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
        sha256 = "34fe74d82caa50873b74520f5233c609213bc6ff7b46762f467e4fccd3a70d30",
        strip_prefix = "seastar-debb8f3792fb18b1490e13ce33580883c80e089a",
        url = "https://github.com/redpanda-data/seastar/archive/debb8f3792fb18b1490e13ce33580883c80e089a.tar.gz",
    )

    http_archive(
        name = "snappy",
        build_file = "//bazel/thirdparty:snappy.BUILD",
        sha256 = "774c337a545d6b818163c12455627ba61ed8d2daa9a9236b3aa88c64f081560e",
        strip_prefix = "snappy-ca541bd6c80d97cf93d1a33e613521947701ea53",
        url = "https://github.com/redpanda-data/snappy/archive/ca541bd6c80d97cf93d1a33e613521947701ea53.tar.gz",
    )

    http_archive(
        name = "unordered_dense",
        build_file = "//bazel/thirdparty:unordered_dense.BUILD",
        sha256 = "98c9d02ff8761d50a2cb6ebd53f78f7d311f6980aef509efdcdaa5f3868ca06c",
        strip_prefix = "unordered_dense-9338f301522a965309ecec58ce61f54a52fb5c22",
        url = "https://github.com/redpanda-data/unordered_dense/archive/9338f301522a965309ecec58ce61f54a52fb5c22.tar.gz",
    )

    http_archive(
        name = "wasmtime",
        build_file = "//bazel/thirdparty:wasmtime.BUILD",
        sha256 = "a7f989b170d109696b928b4b3d1ec1d930064af7df47178e1341bd96e5c34465",
        strip_prefix = "wasmtime-9e1084ffac08b1bf9c82de40c0efc1baff14b9ad",
        url = "https://github.com/bytecodealliance/wasmtime/archive/9e1084ffac08b1bf9c82de40c0efc1baff14b9ad.tar.gz",
    )

    http_archive(
        name = "xz",
        build_file = "//bazel/thirdparty:xz.BUILD",
        sha256 = "0d2b89629f13dd1a0602810529327195eff5f62a0142ccd65b903bc16a4ac78a",
        strip_prefix = "xz-5.2.5",
        url = "https://vectorized-public.s3.amazonaws.com/dependencies/xz-v5.2.5.tar.gz",
    )
