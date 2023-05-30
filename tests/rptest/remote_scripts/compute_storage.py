"""
A script that computes all the files (and optionally their sizes) in the data directory of redpanda.

Useful in tests if you want to know what files exist on a node or if they are a specific size.
"""

from pathlib import Path
import sys
import json


def compute_size(data_dir: Path, sizes: bool):
    output = {}
    for ns in data_dir.iterdir():
        if not ns.is_dir():
            continue
        if ns.name == ".coprocessor_offset_checkpoints":
            continue
        if ns.name == "cloud_storage_cache":
            continue
        ns_output = {}
        for topic in ns.iterdir():
            topic_output = {}
            for partition in topic.iterdir():
                part_output = {}
                for segment in partition.iterdir():
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
