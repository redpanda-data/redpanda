load("@bazel_skylib//rules:expand_template.bzl", "expand_template")
load("@bazel_skylib//rules:write_file.bzl", "write_file")

cc_library(
    name = "lksctp",
    srcs = [
        "src/lib/addrs.c",
        "src/lib/bindx.c",
        "src/lib/connectx.c",
        "src/lib/opt_info.c",
        "src/lib/peeloff.c",
        "src/lib/recvmsg.c",
        "src/lib/sendmsg.c",
        ":conf_header",
    ],
    hdrs = [":gen_header"],
    # This library does some wild symbol versioning that
    # requires libtool and I'm not quite sure how to replicate
    # that in bazel, so just link it statically, there are only
    # a few functions and seastar only needs the header anyways.
    # (no functions are actually used).
    linkstatic = True,
    strip_include_prefix = "src/include",
    visibility = ["//visibility:public"],
)

write_file(
    name = "conf_header",
    out = "config.h",
    # There are no symbols from this file needed or used
    # so an empty file makes the compiler happy.
    content = [],
)

expand_template(
    name = "gen_header",
    out = "src/include/netinet/sctp.h",
    substitutions = {
        "#undef HAVE_SCTP_STREAM_RESET_EVENT": "#define HAVE_SCTP_STREAM_RESET_EVENT 1",
        "#undef HAVE_SCTP_ASSOC_RESET_EVENT": "/* #undef HAVE_SCTP_ASSOC_RESET_EVENT */",
        "#undef HAVE_SCTP_STREAM_CHANGE_EVENT": "/* #undef HAVE_SCTP_STREAM_CHANGE_EVENT */",
        "#undef HAVE_SCTP_STREAM_RECONFIG": "#define HAVE_SCTP_STREAM_RECONFIG 1",
        "#undef HAVE_SCTP_PEELOFF_FLAGS": "#define HAVE_SCTP_PEELOFF_FLAGS 1",
        "#undef HAVE_SCTP_PDAPI_EVENT_PDAPI_STREAM": "#define HAVE_SCTP_PDAPI_EVENT_PDAPI_STREAM 1",
        "#undef HAVE_SCTP_PDAPI_EVENT_PDAPI_SEQ": "#define HAVE_SCTP_PDAPI_EVENT_PDAPI_SEQ 1",
        "#undef HAVE_SCTP_SENDV": "#define HAVE_SCTP_SENDV 1",
        "#undef HAVE_SCTP_AUTH_NO_AUTH": "#define HAVE_SCTP_AUTH_NO_AUTH 1",
        "#undef HAVE_SCTP_SPP_IPV6_FLOWLABEL": "#define HAVE_SCTP_SPP_IPV6_FLOWLABEL 1",
        "#undef HAVE_SCTP_SPP_DSCP": "#define HAVE_SCTP_SPP_DSCP 1",
    },
    template = "src/include/netinet/sctp.h.in",
)
