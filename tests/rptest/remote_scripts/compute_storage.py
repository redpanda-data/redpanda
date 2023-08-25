"""
A script that computes all the files (and optionally their sizes) in the data directory of redpanda.

Useful in tests if you want to know what files exist on a node or if they are a specific size.
"""

from pathlib import Path
import sys
import json


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


def compute_size(data_dir: Path, sizes: bool):
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
                            seg_output["size"] = segment.stat().st_size
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
    args = parser.parse_args()
    data_dir = Path(args.data_dir)
    assert data_dir.exists(), f"{data_dir} must exist"
    output = compute_size(data_dir, args.sizes)
    json.dump(output, sys.stdout)
