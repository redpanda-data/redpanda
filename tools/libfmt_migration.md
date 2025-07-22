We need to upgrade from libfmt version 9 to libfmt version 10.

The major blocker for this upgrade is the breaking change of
removing support for using an argument that has `operator<<`
as an argument to `fmt::print` (or other related formatters).

Our codebase heavily uses `operator<<`, and we've been trying
to use `fmt::formatter` in new code. However, most of our 
`operator<<` implementations are using `fmt::print(os, "...", ...)`
as the implementation, so in theory it should be easy to migrate
to `fmt::formatter` everywhere and upgrade libfmt.

In commit `4aa779bfa34fcc32499abfc7d6e2f922f3b25eef` we added
macros to assist in the migration (and also make custom 
`fmt::formatter` less verbose). Please look at that commit and
use the macros there to convert any `operator<<` you see in the
codebase to using these macros instead.

The codebase uses bazel and you can build all the relevant C++
code using `bazel build //src/v/...`. Please ensure the codebase
builds before and after every commit. Adhere to the rules in
CONTRIBUTING.md when creating commit messages. Before committing
run `bazel run //tools:clang_format` to ensure the code is well
formatted.

Migrate up to 10 or so `operator<<` to using these new macros,
then stop (I know you're not good at counting, and it just matters
that we break up these changes for the humans reviewing).

These `VFMT_*` macros also implement `operator<<`, so be sure to
remove old implementations of `operator<<`. If the `operator<<`
is purely defined in the header inline just use `VFMT_INLINE`.
Otherwise use `VFMT_DECL(type)` in the header and `VFMT_IMPL(type) { ... }`
in the corresponding implementation file, never use VFMT_IMPL on
a type from another subsystem. If there is an existing pattern
like this with `operator<<` please flag it.
