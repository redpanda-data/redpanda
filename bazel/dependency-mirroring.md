# Mirroring flaky external Bazel dependencies

Bazel doesn't retry on HTTP 4xx and only sparingly on 5xx, so a single
failed GET against an upstream host (most often `github.com`) aborts the
whole build. Examples from the
[#core-build thread](https://redpandadata.slack.com/archives/C0794GQ5NL9/p1773340892133739):
`zlib` returned 502, `rules_boost` returned 404, `tar.bzl` returned 502
— each took down a round of CI.

To absorb these flakes we route known-flaky URLs through our own s3
mirror at `https://vectorized-public.s3.us-west-2.amazonaws.com/dependencies/`,
falling back to the upstream URL if s3 is somehow down. Bazel's
[`--downloader_config`](https://bazel.build/reference/command-line-reference#flag--downloader_config)
flag (set in `.bazelrc`) points at the rewrite rules in
[`bazel_downloader.cfg`](../bazel_downloader.cfg) at the repo root.

## How it works

Two layers:

1. **vtools `dependencies.csv`** — adding a row to
   `scripts/vectorized-public-dependencies/dependencies.csv` in the
   `redpanda-data/vtools` repo causes the `upload-deps.yml` workflow to
   fetch the upstream URL, verify its sha256, and copy it into the
   `vectorized-public/dependencies/` s3 bucket.

2. **redpanda `bazel_downloader.cfg`** — a per-artifact pair of
   `rewrite` rules:
   ```
   # mirror first
   rewrite UPSTREAM_REGEX_WITH_FILENAME_CAPTURE s3.url/dependencies/$1
   # identity fallback
   rewrite (UPSTREAM_FULL_REGEX) $1
   ```
   The first line substitutes the upstream URL with the s3 mirror URL.
   The second rewrites the upstream URL to itself (`$1`) — required
   because Bazel's `rewrite` is **substitutive**: once any rewrite line
   matches a URL, the original is dropped from the candidate list (see
   [`UrlRewriter.java`](https://github.com/bazelbuild/bazel/blob/master/src/main/java/com/google/devtools/build/lib/bazel/repository/downloader/UrlRewriter.java)).
   Without the identity rewrite, the upstream URL would never be tried.

   Bazel attempts candidates in the order they appear in the file, so
   listing the mirror first makes s3 the primary fetch path. sha256 is
   verified by Bazel against `MODULE.bazel` / `http_archive(...)` on
   whichever candidate succeeded, so a corrupt mirror cannot poison the
   build.

## Adding a new mirror

Use this when an upstream URL has flaked CI (or you can see it's likely
to). One PR per repo:

### 1. vtools — upload to s3

In `redpanda-data/vtools`, run [`/rp-mirror-artifact`](https://github.com/redpanda-data/vtools/blob/main/scripts/vectorized-public-dependencies/README.md)
or by hand: add a row to `scripts/vectorized-public-dependencies/dependencies.csv`
in alphabetical order by s3 filename:

```
<s3_filename>,<upstream_url>,<sha256_hex>
```

The `upload-deps.yml` PR check validates the URL is reachable and the
sha256 matches before merge. After merge, the file is uploaded to
`https://vectorized-public.s3.us-west-2.amazonaws.com/dependencies/<s3_filename>`.

The s3 filename usually mirrors the upstream basename. Prefix it
(`rules_boost-<sha>.tar.gz` rather than just `<sha>.tar.gz`) when the
basename is opaque or could collide with another package.

### 2. redpanda — add the rewrite

After the vtools PR merges and the artifact is in s3, add a paired
rewrite to `bazel_downloader.cfg`:

```
# mylib v1.2.3 (BCR | archive_override | http_archive in MODULE.bazel)
rewrite host\.example\.com/path/(mylib-1\.2\.3\.tar\.gz) vectorized-public.s3.us-west-2.amazonaws.com/dependencies/$1
rewrite (host\.example\.com/path/mylib-1\.2\.3\.tar\.gz) $1
```

Notes on the regex:

- **Java regex with `$N` backrefs.** Escape `.` literals as `\.`.
- **Match by exact path**, not blanket `host.example.com/*`. Catching
  unmirrored URLs would silently break their fetch (s3 would 404, then
  the identity rewrite isn't there to save us — the original URL is
  already gone after the first match).
- The `rewrite` line **does not include the URL scheme** — match starts
  at the host.

### 3. Verify

Locally, confirm Bazel actually exercises the mirror with
`bazel fetch <repo> --output_base=/tmp/test-ob --repository_cache=/tmp/test-rc`
(fresh output_base + repo cache forces a real download). Then check the
repo cache:
```
find /tmp/test-rc -name "*<expected-sha256-prefix>*"
```

If the expected sha256 file shows up and there are no warnings about
download failures from the s3 host in the bazel output, the mirror is
the URL Bazel hit.

For higher-stakes verification, deliberately introduce a typo (regex or
filename) and confirm: a typo'd regex leaves rustc/zlib/etc untouched
(no s3 attempt visible), a typo'd s3 filename causes a `404` warning
and a fallback to the upstream URL — both prove the chain is wired
correctly.

## Why we don't just `block` upstream hosts

We could pair each `rewrite` with a `block` to force traffic exclusively
through s3. We don't, because:

- An s3 outage with no fallback would take CI down hard.
- Mirror-first + identity fallback gives the same speed/reliability win
  on the happy path and self-heals when s3 is the one that hiccups.
- It's incremental: forgetting to mirror something doesn't break the
  build, it just continues fetching from upstream as before.

## When **not** to add a mirror

- Single-source artifacts already served only from `vectorized-public`
  (`@openssl`, `@hwloc`, etc. in [`bazel/repositories.bzl`](repositories.bzl))
  don't need a downloader rewrite — the URL in `http_archive(...)` is
  already s3.
- Tiny one-off downloads from rarely-flaky CDNs aren't worth the bucket
  storage. The bucket isn't free; rule of thumb is ≥1 observed CI flake
  on that URL.
- Files behind a license that forbids redistribution. Don't.
