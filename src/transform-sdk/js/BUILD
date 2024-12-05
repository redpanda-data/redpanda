cc_library(
    name = "js_vm",
    srcs = ["js_vm.cc"],
    hdrs = ["js_vm.h"],
    copts = [
        "-std=c++23",
        "-fno-exceptions",
    ],
    deps = ["@quickjs"],
)

cc_test(
    name = "js_vm_test",
    size = "small",
    srcs = ["js_vm_test.cc"],
    copts = [
        "-std=c++23",
        "-fno-exceptions",
    ],
    deps = [
        ":js_vm",
        "@googletest//:gtest_main",
    ],
)
