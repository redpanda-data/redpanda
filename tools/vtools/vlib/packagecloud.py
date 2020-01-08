import pathlib
import requests
import threading

from absl import logging


def create_session_token(access_token):
    url = f'{_get_api_url()}/token.json'
    req = requests.get(url, auth=(access_token, ''))
    req.raise_for_status()
    return req.json()['token']


def create_master_token(token, customer_id):
    url = f'{_get_api_repo_url()}/master_tokens'
    data = {'master_token[name]': customer_id}
    req = requests.post(url, data=data, auth=(token, ''))
    req.raise_for_status()
    return req.json()


def create_read_token(token, master_token_id, name):
    url = (f'{_get_api_repo_url()}/master_tokens/'
           f'{master_token_id}/read_tokens.json')
    data = {'read_token[name]': name}
    req = requests.post(url, data=data, auth=(token, ''))
    req.raise_for_status()
    return req.json()['value']


def revoke_master_token(token, master_token_id):
    url = f'{_get_api_repo_url()}/master_tokens/{master_token_id}'
    req = requests.delete(url, auth=(token, ''))
    req.raise_for_status()


def publish_packages(vconfig, access_token):
    supported_distros = {
        "deb": {
            "debian": ["jessie", "stretch", "buster", "bullseye"],
            "ubuntu": ["trusty", "utopic", "vivid", "wily", "xenial",
                       "yakkety", "zesty", "artful", "bionic", "cosmic",
                       "disco", "eoan"]
        },
        "rpm": {
            "el": ["6", "7", "8"], # red hat enterprise linux
            "fedora": ["30", "31"],
            "ol": ["7"], # oracle linux
        }
    }

    token = create_session_token(access_token)
    distro_md = _get_packagecloud_distro_metadata(token, vconfig,
                                                  supported_distros)
    threads = []
    for md in distro_md:
        t = threading.Thread(target=_push_pkg, args=(token, md))
        threads.append(t)
        t.start()

    for x in threads:
        x.join()


def _get_packagecloud_distro_metadata(token, vconfig, supported_distros):
    pc_distros = _get_packagecloud_distro_list(token)

    distro_md = []
    for fmt, distros in supported_distros.items():

        package_files = _get_package_files(vconfig, fmt)

        for distro_name, releases in distros.items():
            for release_name in releases:
                md = {'fmt': fmt, 'name': distro_name, 'release': release_name,
                      'files': package_files}

                for d in pc_distros[fmt]:
                    if d['index_name'] == distro_name:
                        for r in d['versions']:
                            if r['index_name'] == release_name:
                                md['id'] = int(r['id'])
                                break

                if not md.get('id', None):
                    raise Exception(f"Could not find id for {release_name}")

                distro_md.append(md)

    return distro_md


def _get_package_files(vconfig, fmt):
    if fmt == 'deb':
        pkg_dir = f'{vconfig.build_dir}/dist/debian/'
        pattern = "*.deb"
    else:
        pkg_dir = f'{vconfig.build_dir}/dist/rpm/RPMS/x86_64/'
        pattern = "*.rpm"

    files = list(pathlib.Path(pkg_dir).glob(pattern))

    if not files:
        raise Exception(f"No '{fmt}' found in '{vconfig.build_dir}/dist'")

    return files


def _push_pkg(token, distro_md):
    url = f'{_get_api_repo_url()}/packages.json'
    logging.debug(f"Uploading {distro_md}.")
    for f in distro_md['files']:
        logging.debug(f"file: {f}.")
        data = {
            'package[distro_version_id]': distro_md['id'],
        }
        files = {
            'package[package_file]': (str(f), f.open('rb'))
        }
        res = requests.post(url, auth=(token, ''), data=data, files=files)
        logging.debug(res.text)
        res.raise_for_status()


def show_master_token(token, master_token_id):
    url = f'{_get_api_repo_url()}/master_tokens/{master_token_id}'
    req = requests.get(url, auth=(token, ''))
    req.raise_for_status()
    return req.json()


def _get_base_url():
    return f'https://packagecloud.io'


def _get_api_url():
    return f'{_get_base_url()}/api/v1'


def _get_api_repo_url():
    return f'{_get_api_url()}/repos/vectorizedio/v'


def _get_packagecloud_distro_list(token):
    # packagecloud distro list schema:
    # {
    #   "deb": [{
    #     "display_name": "Ubuntu",
    #     "index_name": "ubuntu",
    #     "versions": [{
    #       "id": 4,
    #       "display_name": "5.10 Breezy Badger",
    #       "index_name": "breezy"
    #     },
    #     {
    #     ...
    url = f'{_get_api_url()}/distributions/'
    req = requests.get(url, auth=(token, ''))
    req.raise_for_status()
    return req.json()
