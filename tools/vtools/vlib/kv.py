import os
import datetime

from absl import logging
from peewee import *
from playhouse.kv import KeyValue


def vectorized_kv(root):
    root = os.path.abspath(root)
    db_file = f'{root}/.vtools.db'
    logging.info(f"key=value db in {db_file}")
    db = SqliteDatabase(db_file)
    return KeyValue(ordered=True, database=db, table_name="vectorized")
