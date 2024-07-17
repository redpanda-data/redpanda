#
# This build is a translation from the official autotools-based build contained
# in the numactl source tree.
#
# TODO(bazel)
# - inspect implementation to see what should go into config.h
# - understand more if we are using versions.ldscript correctly
# - check if libtool -version-info flag is needed
# - look into this warning
#     external/_main~_repo_rules~numactl/libnuma.c:68:2: warning: "not threadsafe" [-W#warnings]
#     #warning "not threadsafe"
# - probably needs a header prefix
#
genrule(
    name = "config_h",
    outs = ["config.h"],
    cmd = "touch $(location config.h)",
)

cc_library(
    name = "numactl",
    srcs = [
        "affinity.c",
        "affinity.h",
        "distance.c",
        "libnuma.c",
        "numaint.h",
        "rtnetlink.c",
        "rtnetlink.h",
        "syscall.c",
        "sysfs.c",
        "sysfs.h",
        "util.h",
        ":config.h",
    ],
    hdrs = [
        "numa.h",
        "numacompat1.h",
        "numaif.h",
    ],
    linkopts = [
        "-Wl,--version-script,$(location :versions.ldscript)",
        "-Wl,-init,numa_init",
        "-Wl,-fini,numa_fini",
    ],
    visibility = [
        "//visibility:public",
    ],
    deps = [
        ":versions.ldscript",
    ],
)
