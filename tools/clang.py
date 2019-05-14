import os
from constants import *


def clang_env_from_path(clang_path):
    env = os.environ.copy()
    env["CC"] = clang_path
    env["CXX"] = "%s++" % clang_path
    return env