load("@buildifier_prebuilt//:rules.bzl", "buildifier")

buildifier(
    name = "buildifier.check",
    exclude_patterns = [
        "./.git/*",
        "./build/*",
        "./vbuild/*",
    ],
    lint_mode = "warn",
    mode = "diff",
)

buildifier(
    name = "buildifier.fix",
    exclude_patterns = [
        "./.git/*",
        "./build/*",
        "./vbuild/*",
    ],
    lint_mode = "warn",
    mode = "fix",
)
