"""
A script that computes all the files (and optionally their sizes) in the data directory of redpanda.

Useful in tests if you want to know what files exist on a node or if they are a specific size.
"""
import time
from pathlib import Path
import sys
import json
import io
import struct
import collections
import hashlib
import subprocess
from typing import Iterator


# NB: SegmentReader is duplicated in si_utils.py for deployment reasons. If
# making changes please adapt both.
class SegmentReader:
    HDR_FMT_RP = "<IiqbIhiqqqhii"
    HEADER_SIZE = struct.calcsize(HDR_FMT_RP)
    Header = collections.namedtuple(
        'Header', ('header_crc', 'batch_size', 'base_offset', 'type', 'crc',
                   'attrs', 'delta', 'first_ts', 'max_ts', 'producer_id',
                   'producer_epoch', 'base_seq', 'record_count'))

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
            if header.batch_size == 0 and self.partial_reads < self.max_partial_reads_tolerated:
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
                f"parsed header: {header}")
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
    try:
        return [f for f in p.iterdir()]
    except FileNotFoundError:
        return []


def md5_for_bytes(calculate_md5: bool, data: bytes) -> str:
    return hashlib.md5(data).hexdigest() if calculate_md5 else ''


def md5_for_filename(calculate_md5: bool, file: Path) -> str:
    return subprocess.check_output([
        'md5sum', file.absolute()
    ]).decode('utf-8').split(' ')[0] if calculate_md5 else ''


def compute_size_for_file(file: Path, calc_md5: bool):
    file_size = file.stat().st_size
    if file.suffix == '.log':
        page_size = 4096

        # just read segments for small files
        if file_size < 4 * page_size:
            data = file.read_bytes()
            reader = SegmentReader(io.BytesIO(data))
            return md5_for_bytes(calc_md5,
                                 data), sum(h.batch_size for h in reader)
        else:
            # if the last page is not a null page this is a properly closed and
            # truncated segment and hence we can just use filesize otherwise
            # compute the size of the segment
            with file.open('rb') as f:
                f.seek(-page_size, io.SEEK_END)
                end_page = f.read(page_size)
                if end_page != b'\x00' * page_size:
                    return md5_for_filename(calc_md5, file), file_size

                f.seek(0)
                data = f.read()

                # Pass the file handle directly to segment reader. Since we sometimes want to rewind
                # and re-read the stream, passing a static view of data is not useful, we want the
                # current data on disk.
                f.seek(0)
                reader = SegmentReader(f)
                return md5_for_bytes(calc_md5,
                                     data), sum(h.batch_size for h in reader)
    else:
        return md5_for_filename(calc_md5, file), file_size


def compute_size(data_dir: Path, sizes: bool, calculate_md5: bool,
                 print_flat: bool):
    output = {}
    for ns in safe_listdir(data_dir):
        if not safe_isdir(ns):
            continue
        if ns.name == "cloud_storage_cache":
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
                            md5, size = compute_size_for_file(
                                segment, calculate_md5)
                            seg_output["size"] = size
                            if calculate_md5:
                                seg_output["md5"] = md5
                            if print_flat:
                                print(f"{segment.absolute()} {size} {md5}")
                        except FileNotFoundError:
                            # It's valid to have a segment deleted
                            # at anytime
                            continue
                    part_output[segment.name] = seg_output
                topic_output[partition.name] = part_output
            ns_output[topic.name] = topic_output
        output[ns.name] = ns_output
    return output


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Compute')
    parser.add_argument('--data-dir',
                        type=str,
                        help='The redpanda data dir',
                        required=True)
    parser.add_argument('--sizes',
                        action="store_true",
                        help='Also compute sizes of files')
    parser.add_argument('--md5',
                        action="store_true",
                        help='Also compute md5 checksums')
    parser.add_argument(
        '--print-flat',
        action="store_true",
        help='Print output for each file instead of returning as json')
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    assert data_dir.exists(), f"{data_dir} must exist"
    output = compute_size(data_dir, args.sizes, args.md5, args.print_flat)
    if not args.print_flat:
        json.dump(output, sys.stdout)
