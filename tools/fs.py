import os


def force_link(src, target):
    _force_link(os.link, src, target)


def force_symlink(src, target):
    _force_link(os.symlink, src, target)


def _force_link(link_op, src, target):
    if os.path.exists(target):
        os.unlink(target)
    link_op(src, target)
