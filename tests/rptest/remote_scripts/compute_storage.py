"""
A script that computes all the files (and optionally their sizes) in the data directory of redpanda.

Useful in tests if you want to know what files exist on a node or if they are a specific size.
"""

import collections
import hashlib
import io
import json
import os
import struct
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterator


# NB: SegmentReader is duplicated in si_utils.py for deployment reasons. If
# making changes please adapt both.
class SegmentReader:
    HDR_FMT_RP = "<IiqbIhiqqqhii"
    HEADER_SIZE = struct.calcsize(HDR_FMT_RP)
    Header = collections.namedtuple(
        "Header",
        (
            "header_crc",
            "batch_size",
            "base_offset",
            "type",
            "crc",
            "attrs",
            "delta",
            "first_ts",
            "max_ts",
            "producer_id",
            "producer_epoch",
            "base_seq",
            "record_count",
        ),
    )

    def __init__(self, stream):
        self.stream = stream
        self.max_partial_reads_tolerated = 5
        self.sleep_between_read_retries_sec = 0.5
        self.partial_reads = 0

    def read_batch(self):
        pos_before_hdr = self.stream.tell()
        data = self.stream.read(self.HEADER_SIZE)
        if len(data) == self.HEADER_SIZE:
            header = self.Header(*struct.unpack(self.HDR_FMT_RP, data))
            if all(map(lambda v: v == 0, header)):
                return None

            # The segment may be written to while this script is running. In this case the batch
            # may be partially written. If so try to rewind to the position before header, and do
            # another read (upto max_partial_reads_tolerated times) of the same batch.
            if (
                header.batch_size == 0
                and self.partial_reads < self.max_partial_reads_tolerated
            ):
                self.partial_reads += 1
                time.sleep(self.sleep_between_read_retries_sec)
                self.stream.seek(pos_before_hdr)
                return self.read_batch()
            self.partial_reads = 0

            records_size = header.batch_size - self.HEADER_SIZE
            data = self.stream.read(records_size)
            if len(data) < records_size:
                return None
            assert len(data) == records_size, (
                f"data len is {len(data)} but the expected records size is {records_size}, "
                f"parsed header: {header}"
            )
            return header
        return None

    def __iter__(self) -> Iterator[Header]:
        while True:
            it = self.read_batch()
            if it is None:
                return
            yield it


def safe_isdir(p: Path) -> bool:
    """
    It's valid for files to be deleted at any time,
    in that case that the file is missing, just return
    that it's not a directory
    """
    try:
        return p.is_dir()
    except FileNotFoundError:
        return False


def safe_listdir(p: Path) -> list[Path]:
    """
    It's valid for directories to be deleted at any time,
    in that case that the directory is missing, just return
    that there are no files.
    """
    if not safe_isdir(p):
        return []

    try:
        return [f for f in p.iterdir()]
    except FileNotFoundError:
        return []


def md5_for_bytes(calculate_md5: bool, data: bytes) -> str:
    return hashlib.md5(data, usedforsecurity=False).hexdigest() if calculate_md5 else ""


def md5_for_filename(calculate_md5: bool, file: Path) -> str:
    return (
        subprocess.check_output(["md5sum", file.absolute()])
        .decode("utf-8")
        .split(" ")[0]
        if calculate_md5
        else ""
    )


def compute_size_for_file(file: Path, calc_md5: bool):
    file_size = file.stat().st_size
    if file.suffix == ".log":
        page_size = 4096

        # just read segments for small files
        if file_size < 4 * page_size:
            data = file.read_bytes()
            reader = SegmentReader(io.BytesIO(data))
            return md5_for_bytes(calc_md5, data), sum(h.batch_size for h in reader)
        else:
            # if the last page is not a null page this is a properly closed and
            # truncated segment and hence we can just use filesize otherwise
            # compute the size of the segment
            with file.open("rb") as f:
                f.seek(-page_size, io.SEEK_END)
                end_page = f.read(page_size)
                if end_page != b"\x00" * page_size:
                    return md5_for_filename(calc_md5, file), file_size

                f.seek(0)
                data = f.read()

                # Pass the file handle directly to segment reader. Since we sometimes want to rewind
                # and re-read the stream, passing a static view of data is not useful, we want the
                # current data on disk.
                f.seek(0)
                reader = SegmentReader(f)
                return md5_for_bytes(calc_md5, data), sum(h.batch_size for h in reader)
    else:
        return md5_for_filename(calc_md5, file), file_size


def compute_size(
    data_dir: Path,
    sizes: bool,
    calculate_md5: bool,
    print_flat: bool,
    compaction_footers: bool,
):
    output = {}
    for ns in safe_listdir(data_dir):
        if not safe_isdir(ns):
            continue
        if ns.name in ["cloud_storage_cache", "crash_reports", "datalake_staging"]:
            continue
        ns_output = {}
        for topic in safe_listdir(ns):
            topic_output = {}
            for partition in safe_listdir(topic):
                part_output = {}
                for segment in safe_listdir(partition):
                    seg_output = {}
                    if sizes:
                        try:
                            md5, size = compute_size_for_file(segment, calculate_md5)
                            seg_output["size"] = size
                            if calculate_md5:
                                seg_output["md5"] = md5
                            if print_flat:
                                print(f"{segment.absolute()} {size} {md5}")
                        except FileNotFoundError:
                            # It's valid to have a segment deleted
                            # at anytime
                            continue
                    if compaction_footers:
                        try:
                            if segment.suffix == ".compaction_index":
                                seg_output["compaction_footer"] = (
                                    read_compaction_footer(segment)
                                )
                        except FileNotFoundError:
                            # It's valid to have a segment deleted
                            # at anytime
                            continue
                    part_output[segment.name] = seg_output
                topic_output[partition.name] = part_output
            ns_output[topic.name] = topic_output
        output[ns.name] = ns_output
    return output


def read_compaction_footer(file_path):
    compacted_index_file = open(file_path, "rb")
    fsize = os.stat(file_path).st_size
    # Keep up to date with compacted_index::footer impl
    # Footers are encoded with little-endian.

    # v1: uint32_t, uint32_t, uint32_t, uint32_t, int8_t
    u64 = 8
    u32 = 4
    i8 = 1
    FOOTER_SIZE_V1 = sum([u32 + u32 + u32 + u32 + i8])
    FOOTER_V1 = "<IIIIb"

    # v2/v3: uint64_t, uint64_t, uint32_t, uint32_t, uint32_t, uint32_t, int8_t
    FOOTER_SIZE_V2 = sum([u64 + u64 + u32 + u32 + u32 + u32 + i8])
    FOOTER_V2 = "<QQIIIIb"

    assert fsize >= FOOTER_SIZE_V1, (
        f"Error reading compaction footer {file_path}, file size too small ({fsize})"
    )

    footer_buf_size = min(fsize, FOOTER_SIZE_V2)
    offset = fsize - footer_buf_size
    compacted_index_file.seek(offset)
    footer = compacted_index_file.read(footer_buf_size)
    FOOTER_FLAG_TRUNCATION = 1
    FOOTER_FLAG_SELF_COMPACTION = 1 << 1
    FOOTER_FLAG_INCOMPLETE = 1 << 2
    res = dict()
    # Try to parse as V1
    try:
        footer_v1 = footer[footer_buf_size - FOOTER_SIZE_V1 :]
        unpacked_footer = struct.unpack(FOOTER_V1, footer_v1)
        res["size"] = unpacked_footer[0]
        res["keys"] = unpacked_footer[1]
        res["truncation"] = bool(
            (unpacked_footer[2] & FOOTER_FLAG_TRUNCATION) == FOOTER_FLAG_TRUNCATION
        )
        res["self_compaction"] = bool(
            (unpacked_footer[2] & FOOTER_FLAG_SELF_COMPACTION)
            == FOOTER_FLAG_SELF_COMPACTION
        )
        res["incomplete"] = bool(
            (unpacked_footer[2] & FOOTER_FLAG_INCOMPLETE) == FOOTER_FLAG_INCOMPLETE
        )
        res["crc"] = unpacked_footer[3]
        res["version"] = unpacked_footer[4]
    except Exception:
        footer_v2 = footer[:]
        unpacked_footer = struct.unpack(FOOTER_V2, footer_v2)
        res["size"] = unpacked_footer[0]
        res["keys"] = unpacked_footer[1]
        # Deprecated size [2]
        # Deprecated keys [3]
        res["truncation"] = bool(
            (unpacked_footer[4] & FOOTER_FLAG_TRUNCATION) == FOOTER_FLAG_TRUNCATION
        )
        res["self_compaction"] = bool(
            (unpacked_footer[4] & FOOTER_FLAG_SELF_COMPACTION)
            == FOOTER_FLAG_SELF_COMPACTION
        )
        res["incomplete"] = bool(
            (unpacked_footer[4] & FOOTER_FLAG_INCOMPLETE) == FOOTER_FLAG_INCOMPLETE
        )
        res["crc"] = unpacked_footer[5]
        res["version"] = unpacked_footer[6]
    return res


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Compute")
    parser.add_argument(
        "--data-dir", type=str, help="The redpanda data dir", required=True
    )
    parser.add_argument(
        "--sizes", action="store_true", help="Also compute sizes of files"
    )
    parser.add_argument("--md5", action="store_true", help="Also compute md5 checksums")
    parser.add_argument(
        "--compaction-footers",
        action="store_true",
        help="Also read footers for compacted indices (if they exist)",
    )
    parser.add_argument(
        "--print-flat",
        action="store_true",
        help="Print output for each file instead of returning as json",
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    assert data_dir.exists(), f"{data_dir} must exist"
    output = compute_size(
        data_dir, args.sizes, args.md5, args.print_flat, args.compaction_footers
    )
    if not args.print_flat:
        json.dump(output, sys.stdout)
