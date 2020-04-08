import tarfile
import io
import urllib

from absl import logging


#If topdir is None, topdir is the name of the first element in the tarball
#this use case is added to support jave installation. see install/commands.py
def download_and_extract(url, name, directory, topdir, index=0):
    logging.info("Downloading " + url)
    handle = urllib.request.urlopen(url)
    io_bytes = io.BytesIO(handle.read())
    logging.info(f'Extracting {name} tarball to {directory}')

    tar = tarfile.open(fileobj=io_bytes, mode='r')

    if topdir is None:
        topdir = f'{tar.getmembers()[0].name}/'

    for member in tar.getmembers()[index:]:
        member.name = member.name.replace(topdir, '')
        tar.extract(member, directory)
