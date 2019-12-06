import requests
import urllib.parse


def create_session_token(access_token):
    url = f'{_get_api_url()}/token.json'
    req = requests.get(url, auth=(access_token, ''))
    req.raise_for_status()
    return req.json()['token']


def create_master_token(token, client_id):
    url = f'{_get_api_repo_url()}/master_tokens'
    data = {'master_token[name]': client_id}
    req = requests.post(url, data=data, auth=(token, ''))
    req.raise_for_status()
    return req.json()


def create_read_token(token, master_token_id, name):
    url = f'{_get_api_repo_url()}/master_tokens/{master_token_id}/read_tokens.json'
    data = {'read_token[name]': name}
    req = requests.post(url, data=data, auth=(token, ''))
    req.raise_for_status()
    return req.json()['value']


def revoke_master_token(token, master_token_id):
    url = f'{_get_api_repo_url()}/master_tokens/{master_token_id}'
    req = requests.delete(url, auth=(token, ''))
    req.raise_for_status()


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
    user = 'vectorizedio'
    repo = 'v'
    return f'{_get_api_url()}/repos/{user}/{repo}'
