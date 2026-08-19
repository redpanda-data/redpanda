#!/usr/bin/env python3
"""Generate bazel-deps.nix from MODULE.bazel.lock + BCR + MODULE.bazel.

Parses three sources to build the complete list of archives that Bazel
needs, then emits a Nix expression (list of { url, sha256, name }) for
use with bazel-repo-cache.nix.

Sources:
  1. BCR source.json files — for each module version referenced in the
     lockfile's registryFileHashes, read the corresponding source.json
     from the local BCR snapshot. Extract url + integrity.
  2. generatedRepoSpecs in lockfile — for repos with url/urls and
     sha256/integrity attributes. Handles dict-valued urls/sha256
     (e.g. toolchains_llvm with per-platform entries).
  3. archive_override entries in MODULE.bazel — extract URL + integrity
     directly from the Starlark source.

Usage:
  python3 nix/gen-bazel-deps.py \
    --lockfile MODULE.bazel.lock \
    --bcr /nix/store/...-bcr \
    --module-bazel MODULE.bazel \
    > nix/bazel-deps.nix
"""

import argparse
import base64
import json
import re
import sys
import urllib.parse
from pathlib import Path


def sri_to_hex(sri: str) -> str:
    """Convert SRI hash (sha256-<base64>) to hex string."""
    if sri.startswith("sha256-"):
        b64 = sri[len("sha256-"):]
        return base64.b64decode(b64).hex()
    raise ValueError(f"Unsupported SRI format: {sri}")


def integrity_to_hex(value: str) -> str:
    """Convert either hex sha256 or SRI integrity to hex."""
    if value.startswith("sha256-"):
        return sri_to_hex(value)
    # Already hex
    if len(value) == 64 and all(c in "0123456789abcdef" for c in value):
        return value
    raise ValueError(f"Cannot parse hash: {value}")


def url_to_name(url: str) -> str:
    """Derive a safe Nix store name from a URL."""
    parsed = urllib.parse.urlparse(url)
    basename = parsed.path.rstrip("/").rsplit("/", 1)[-1]
    # Nix store names can't start with a dot
    if basename.startswith("."):
        basename = "_" + basename
    # Replace characters invalid in Nix store paths
    basename = re.sub(r"[^a-zA-Z0-9._+-]", "_", basename)
    # Truncate very long names
    if len(basename) > 200:
        basename = basename[:200]
    return basename or "archive"


def collect_bcr_archives(lockfile: dict, bcr_path: Path) -> list[dict]:
    """Parse BCR source.json files for archive URLs."""
    archives = []
    registry_hashes = lockfile.get("registryFileHashes", {})

    for url in registry_hashes:
        if "/source.json" not in url:
            continue

        # URL pattern: https://bcr.bazel.build/modules/<name>/<version>/source.json
        # Map to local: <bcr>/modules/<name>/<version>/source.json
        match = re.match(
            r"https://bcr\.bazel\.build/modules/(.+)/source\.json$", url
        )
        if not match:
            continue

        rel_path = f"modules/{match.group(1)}/source.json"
        source_json_path = bcr_path / rel_path

        if not source_json_path.exists():
            print(
                f"WARNING: BCR source.json not found: {source_json_path}",
                file=sys.stderr,
            )
            continue

        with open(source_json_path) as f:
            source = json.load(f)

        archive_url = source.get("url")
        integrity = source.get("integrity")

        if not archive_url or not integrity:
            print(
                f"WARNING: Missing url/integrity in {source_json_path}",
                file=sys.stderr,
            )
            continue

        try:
            sha256_hex = integrity_to_hex(integrity)
        except ValueError as e:
            print(f"WARNING: {e} in {source_json_path}", file=sys.stderr)
            continue

        archives.append(
            {
                "url": archive_url,
                "sha256": sha256_hex,
                "name": url_to_name(archive_url),
            }
        )

        # Also collect patch files if any (these are served from BCR but
        # Bazel may also cache them in the repo cache)
        # Patches in BCR source.json are local to the BCR directory,
        # served via --registry=file://, so no need to cache them.

    return archives


def collect_extension_archives(lockfile: dict) -> list[dict]:
    """Parse moduleExtensions.*.generatedRepoSpecs for downloadable repos."""
    archives = []

    for ext_key, ext_val in lockfile.get("moduleExtensions", {}).items():
        for os_key, os_val in ext_val.items():
            specs = os_val.get("generatedRepoSpecs", {})
            for repo_name, repo in specs.items():
                attrs = repo.get("attributes", {})
                archives.extend(
                    _extract_archives_from_attrs(attrs, repo_name)
                )

    return archives


def _extract_archives_from_attrs(
    attrs: dict, repo_name: str
) -> list[dict]:
    """Extract archive entries from a repo's attributes.

    Handles:
    - url (string) + sha256 (string)
    - urls (list) + sha256 (string)
    - url/urls (string) + integrity (SRI string)
    - urls (dict) + sha256 (dict) — per-platform (e.g. toolchains_llvm)
    """
    results = []

    raw_url = attrs.get("url")
    raw_urls = attrs.get("urls")
    raw_sha = attrs.get("sha256")
    raw_integrity = attrs.get("integrity")

    # Determine the hash value(s)
    hash_val = raw_sha if raw_sha else raw_integrity
    if not hash_val:
        return results

    # Skip empty hash strings
    if isinstance(hash_val, str) and not hash_val.strip():
        return results

    # Determine URL value(s)
    url_val = raw_url or raw_urls
    if not url_val:
        return results

    # Case 1: dict-valued (per-platform)
    if isinstance(hash_val, dict):
        url_dict = url_val if isinstance(url_val, dict) else {}
        for platform_key, platform_sha in hash_val.items():
            if not platform_sha:
                continue
            platform_urls = url_dict.get(platform_key, [])
            if isinstance(platform_urls, str):
                platform_urls = [platform_urls]
            if not platform_urls:
                continue
            for u in platform_urls:
                try:
                    sha_hex = integrity_to_hex(platform_sha)
                except ValueError:
                    continue
                results.append(
                    {
                        "url": u,
                        "sha256": sha_hex,
                        "name": url_to_name(u),
                    }
                )
        return results

    # Case 2: scalar values
    try:
        sha_hex = integrity_to_hex(hash_val)
    except ValueError as e:
        print(
            f"WARNING: {e} for repo {repo_name}",
            file=sys.stderr,
        )
        return results

    urls = []
    if isinstance(url_val, str):
        urls = [url_val]
    elif isinstance(url_val, list):
        urls = url_val
    else:
        return results

    if not urls:
        return results

    # Only cache the first URL — Bazel tries them in order but the
    # content-addressed cache only needs one copy.
    results.append(
        {
            "url": urls[0],
            "sha256": sha_hex,
            "name": url_to_name(urls[0]),
        }
    )
    return results


def collect_archive_overrides(module_bazel_path: Path) -> list[dict]:
    """Parse archive_override() entries from MODULE.bazel."""
    archives = []
    content = module_bazel_path.read_text()

    # Match archive_override blocks
    pattern = re.compile(
        r"archive_override\s*\(\s*(.*?)\s*\)",
        re.DOTALL,
    )

    for m in pattern.finditer(content):
        block = m.group(1)

        # Extract integrity
        integrity_match = re.search(
            r'integrity\s*=\s*"([^"]+)"', block
        )
        # Extract sha256 (alternative)
        sha256_match = re.search(r'sha256\s*=\s*"([^"]+)"', block)

        hash_str = None
        if integrity_match:
            hash_str = integrity_match.group(1)
        elif sha256_match:
            hash_str = sha256_match.group(1)

        if not hash_str:
            continue

        try:
            sha_hex = integrity_to_hex(hash_str)
        except ValueError as e:
            print(f"WARNING: {e} in archive_override", file=sys.stderr)
            continue

        # Extract URLs — can be url = "..." or urls = ["..."]
        url_match = re.search(r'url\s*=\s*"([^"]+)"', block)
        urls_match = re.search(
            r'urls\s*=\s*\[(.*?)\]', block, re.DOTALL
        )

        url = None
        if url_match:
            url = url_match.group(1)
        elif urls_match:
            url_items = re.findall(r'"([^"]+)"', urls_match.group(1))
            if url_items:
                url = url_items[0]

        if not url:
            continue

        archives.append(
            {
                "url": url,
                "sha256": sha_hex,
                "name": url_to_name(url),
            }
        )

    return archives


def collect_http_file_overrides(module_bazel_path: Path) -> list[dict]:
    """Parse top-level http_file() and http_archive() entries from MODULE.bazel."""
    archives = []
    content = module_bazel_path.read_text()

    for func_name in ("http_file", "http_archive"):
        pattern = re.compile(
            rf"^{func_name}\s*\(\s*(.*?)\s*\)",
            re.DOTALL | re.MULTILINE,
        )
        for m in pattern.finditer(content):
            block = m.group(1)

            # Extract hash
            integrity_match = re.search(
                r'integrity\s*=\s*"([^"]+)"', block
            )
            sha256_match = re.search(r'sha256\s*=\s*"([^"]+)"', block)

            hash_str = None
            if integrity_match:
                hash_str = integrity_match.group(1)
            elif sha256_match:
                hash_str = sha256_match.group(1)

            if not hash_str:
                continue

            try:
                sha_hex = integrity_to_hex(hash_str)
            except ValueError as e:
                print(
                    f"WARNING: {e} in {func_name}", file=sys.stderr
                )
                continue

            # Extract URLs
            url_match = re.search(r'url\s*=\s*"([^"]+)"', block)
            urls_match = re.search(
                r'urls\s*=\s*\[(.*?)\]', block, re.DOTALL
            )

            url = None
            if url_match:
                url = url_match.group(1)
            elif urls_match:
                url_items = re.findall(r'"([^"]+)"', urls_match.group(1))
                if url_items:
                    url = url_items[0]

            if not url:
                continue

            archives.append(
                {
                    "url": url,
                    "sha256": sha_hex,
                    "name": url_to_name(url),
                }
            )

    return archives


def deduplicate(archives: list[dict]) -> list[dict]:
    """Remove duplicate entries by (url, sha256)."""
    seen = set()
    result = []
    for a in archives:
        key = (a["url"], a["sha256"])
        if key not in seen:
            seen.add(key)
            result.append(a)
    return result


def emit_nix(archives: list[dict]) -> str:
    """Generate Nix expression for the archive list."""
    lines = [
        "# Auto-generated by gen-bazel-deps.py — do not edit manually.",
        "# Re-generate with:",
        "#   python3 nix/gen-bazel-deps.py \\",
        "#     --lockfile MODULE.bazel.lock \\",
        "#     --bcr $(nix-build --no-out-link -E "
        "'with import <nixpkgs> {}; callPackage ./nix/bcr.nix {}') \\",
        "#     --module-bazel MODULE.bazel \\",
        "#     > nix/bazel-deps.nix",
        f"# {len(archives)} archives",
        "[",
    ]

    for a in sorted(archives, key=lambda x: x["url"]):
        lines.append("  {")
        lines.append(f'    url = "{a["url"]}";')
        lines.append(f'    sha256 = "{a["sha256"]}";')
        lines.append(f'    name = "{a["name"]}";')
        lines.append("  }")

    lines.append("]")
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(
        description="Generate bazel-deps.nix from lockfile + BCR + MODULE.bazel"
    )
    parser.add_argument(
        "--lockfile",
        required=True,
        type=Path,
        help="Path to MODULE.bazel.lock",
    )
    parser.add_argument(
        "--bcr",
        required=True,
        type=Path,
        help="Path to local BCR snapshot directory",
    )
    parser.add_argument(
        "--module-bazel",
        required=True,
        type=Path,
        help="Path to MODULE.bazel",
    )
    args = parser.parse_args()

    with open(args.lockfile) as f:
        lockfile = json.load(f)

    all_archives = []

    # 1. BCR source.json archives
    bcr_archives = collect_bcr_archives(lockfile, args.bcr)
    print(
        f"BCR source.json: {len(bcr_archives)} archives",
        file=sys.stderr,
    )
    all_archives.extend(bcr_archives)

    # 2. Extension-generated repos
    ext_archives = collect_extension_archives(lockfile)
    print(
        f"Extension repos: {len(ext_archives)} archives",
        file=sys.stderr,
    )
    all_archives.extend(ext_archives)

    # 3. archive_override entries
    override_archives = collect_archive_overrides(args.module_bazel)
    print(
        f"archive_override: {len(override_archives)} archives",
        file=sys.stderr,
    )
    all_archives.extend(override_archives)

    # 4. Top-level http_file / http_archive entries
    http_archives = collect_http_file_overrides(args.module_bazel)
    print(
        f"http_file/http_archive: {len(http_archives)} archives",
        file=sys.stderr,
    )
    all_archives.extend(http_archives)

    # Deduplicate
    all_archives = deduplicate(all_archives)
    print(
        f"Total (deduplicated): {len(all_archives)} archives",
        file=sys.stderr,
    )

    print(emit_nix(all_archives))


if __name__ == "__main__":
    main()
