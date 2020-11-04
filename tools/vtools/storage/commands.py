import click
from ..vlib import storage as vstorage


@click.group(short_help='storage')
def storage():
    pass


@storage.command(short_help='Report storage summary')
@click.option("--path", help="Path to storage root")
def summary(path):
    store = vstorage.Store(path)
    for ntp in store.ntps:
        for path in ntp.segments:
            try:
                s = vstorage.Segment(path)
            except vstorage.CorruptBatchError as e:
                print("corruption detected in batch {} of segment: {}".format(
                    e.batch.index, path))
                print("header of corrupt batch: {}".format(e.batch.header))
                continue
            print("successfully decoded segment: {}".format(path))
