{ fetchFromGitHub }:

# Pinned snapshot of the Bazel Central Registry.
# Must contain all module versions referenced in MODULE.bazel.
# Newest BCR-sourced dep: abseil-cpp 20250814.0, googleapis 0.0.0-20250703-f9d6fe4a
fetchFromGitHub {
  owner = "bazelbuild";
  repo = "bazel-central-registry";
  rev = "4184a02089170a2f4868f4f84fcb8b802481fb31";
  hash = "sha256-mdYDmnvxLvxhTUE1nRkWowDsRtnbQ1OsfyimFIcok5o=";
}
