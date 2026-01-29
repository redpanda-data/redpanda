from __future__ import annotations
from collections.abc import AsyncIterator
from collections.abc import Iterator
from collections.abc import Iterable
import aiohttp
import urllib3
import typing
import sys
from connectrpc.client_async import AsyncConnectClient
from connectrpc.client_sync import ConnectClient
from connectrpc.client_protocol import ConnectProtocol
from connectrpc.client_connect import ConnectProtocolError
from connectrpc.headers import HeaderInput
from connectrpc.server import ClientRequest
from connectrpc.server import ClientStream
from connectrpc.server import ServerResponse
from connectrpc.server import ServerStream
from connectrpc.server_sync import ConnectWSGI
from connectrpc.streams import StreamInput
from connectrpc.streams import AsyncStreamOutput
from connectrpc.streams import StreamOutput
from connectrpc.unary import UnaryOutput
from connectrpc.unary import ClientStreamingOutput
if typing.TYPE_CHECKING:
    if sys.version_info >= (3, 11):
        from wsgiref.types import WSGIApplication
    else:
        from _typeshed.wsgi import WSGIApplication
from ...... import proto

class SecurityServiceClient:

    def __init__(self, base_url: str, http_client: urllib3.PoolManager | None=None, protocol: ConnectProtocol=ConnectProtocol.CONNECT_PROTOBUF):
        self.base_url = base_url
        self._connect_client = ConnectClient(http_client, protocol)

    def call_create_scram_credential(self, req: proto.redpanda.core.admin.v2.security_pb2.CreateScramCredentialRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.security_pb2.CreateScramCredentialResponse]:
        """Low-level method to call CreateScramCredential, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.SecurityService/CreateScramCredential'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.security_pb2.CreateScramCredentialResponse, extra_headers, timeout_seconds)

    def create_scram_credential(self, req: proto.redpanda.core.admin.v2.security_pb2.CreateScramCredentialRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.security_pb2.CreateScramCredentialResponse:
        response = self.call_create_scram_credential(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_get_scram_credential(self, req: proto.redpanda.core.admin.v2.security_pb2.GetScramCredentialRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.security_pb2.GetScramCredentialResponse]:
        """Low-level method to call GetScramCredential, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.SecurityService/GetScramCredential'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.security_pb2.GetScramCredentialResponse, extra_headers, timeout_seconds)

    def get_scram_credential(self, req: proto.redpanda.core.admin.v2.security_pb2.GetScramCredentialRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.security_pb2.GetScramCredentialResponse:
        response = self.call_get_scram_credential(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_list_scram_credentials(self, req: proto.redpanda.core.admin.v2.security_pb2.ListScramCredentialsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.security_pb2.ListScramCredentialsResponse]:
        """Low-level method to call ListScramCredentials, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.SecurityService/ListScramCredentials'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.security_pb2.ListScramCredentialsResponse, extra_headers, timeout_seconds)

    def list_scram_credentials(self, req: proto.redpanda.core.admin.v2.security_pb2.ListScramCredentialsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.security_pb2.ListScramCredentialsResponse:
        response = self.call_list_scram_credentials(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_update_scram_credential(self, req: proto.redpanda.core.admin.v2.security_pb2.UpdateScramCredentialRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.security_pb2.UpdateScramCredentialResponse]:
        """Low-level method to call UpdateScramCredential, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.SecurityService/UpdateScramCredential'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.security_pb2.UpdateScramCredentialResponse, extra_headers, timeout_seconds)

    def update_scram_credential(self, req: proto.redpanda.core.admin.v2.security_pb2.UpdateScramCredentialRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.security_pb2.UpdateScramCredentialResponse:
        response = self.call_update_scram_credential(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_delete_scram_credential(self, req: proto.redpanda.core.admin.v2.security_pb2.DeleteScramCredentialRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.security_pb2.DeleteScramCredentialResponse]:
        """Low-level method to call DeleteScramCredential, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.SecurityService/DeleteScramCredential'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.security_pb2.DeleteScramCredentialResponse, extra_headers, timeout_seconds)

    def delete_scram_credential(self, req: proto.redpanda.core.admin.v2.security_pb2.DeleteScramCredentialRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.security_pb2.DeleteScramCredentialResponse:
        response = self.call_delete_scram_credential(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_create_role(self, req: proto.redpanda.core.admin.v2.security_pb2.CreateRoleRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.security_pb2.CreateRoleResponse]:
        """Low-level method to call CreateRole, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.SecurityService/CreateRole'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.security_pb2.CreateRoleResponse, extra_headers, timeout_seconds)

    def create_role(self, req: proto.redpanda.core.admin.v2.security_pb2.CreateRoleRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.security_pb2.CreateRoleResponse:
        response = self.call_create_role(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_get_role(self, req: proto.redpanda.core.admin.v2.security_pb2.GetRoleRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.security_pb2.GetRoleResponse]:
        """Low-level method to call GetRole, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.SecurityService/GetRole'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.security_pb2.GetRoleResponse, extra_headers, timeout_seconds)

    def get_role(self, req: proto.redpanda.core.admin.v2.security_pb2.GetRoleRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.security_pb2.GetRoleResponse:
        response = self.call_get_role(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_list_roles(self, req: proto.redpanda.core.admin.v2.security_pb2.ListRolesRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.security_pb2.ListRolesResponse]:
        """Low-level method to call ListRoles, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.SecurityService/ListRoles'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.security_pb2.ListRolesResponse, extra_headers, timeout_seconds)

    def list_roles(self, req: proto.redpanda.core.admin.v2.security_pb2.ListRolesRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.security_pb2.ListRolesResponse:
        response = self.call_list_roles(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_add_role_members(self, req: proto.redpanda.core.admin.v2.security_pb2.AddRoleMembersRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.security_pb2.AddRoleMembersResponse]:
        """Low-level method to call AddRoleMembers, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.SecurityService/AddRoleMembers'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.security_pb2.AddRoleMembersResponse, extra_headers, timeout_seconds)

    def add_role_members(self, req: proto.redpanda.core.admin.v2.security_pb2.AddRoleMembersRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.security_pb2.AddRoleMembersResponse:
        response = self.call_add_role_members(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_remove_role_members(self, req: proto.redpanda.core.admin.v2.security_pb2.RemoveRoleMembersRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.security_pb2.RemoveRoleMembersResponse]:
        """Low-level method to call RemoveRoleMembers, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.SecurityService/RemoveRoleMembers'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.security_pb2.RemoveRoleMembersResponse, extra_headers, timeout_seconds)

    def remove_role_members(self, req: proto.redpanda.core.admin.v2.security_pb2.RemoveRoleMembersRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.security_pb2.RemoveRoleMembersResponse:
        response = self.call_remove_role_members(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_delete_role(self, req: proto.redpanda.core.admin.v2.security_pb2.DeleteRoleRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.security_pb2.DeleteRoleResponse]:
        """Low-level method to call DeleteRole, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.SecurityService/DeleteRole'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.security_pb2.DeleteRoleResponse, extra_headers, timeout_seconds)

    def delete_role(self, req: proto.redpanda.core.admin.v2.security_pb2.DeleteRoleRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.security_pb2.DeleteRoleResponse:
        response = self.call_delete_role(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_list_current_user_roles(self, req: proto.redpanda.core.admin.v2.security_pb2.ListCurrentUserRolesRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.security_pb2.ListCurrentUserRolesResponse]:
        """Low-level method to call ListCurrentUserRoles, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.SecurityService/ListCurrentUserRoles'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.security_pb2.ListCurrentUserRolesResponse, extra_headers, timeout_seconds)

    def list_current_user_roles(self, req: proto.redpanda.core.admin.v2.security_pb2.ListCurrentUserRolesRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.security_pb2.ListCurrentUserRolesResponse:
        response = self.call_list_current_user_roles(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_resolve_oidc_identity(self, req: proto.redpanda.core.admin.v2.security_pb2.ResolveOidcIdentityRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.security_pb2.ResolveOidcIdentityResponse]:
        """Low-level method to call ResolveOidcIdentity, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.SecurityService/ResolveOidcIdentity'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.security_pb2.ResolveOidcIdentityResponse, extra_headers, timeout_seconds)

    def resolve_oidc_identity(self, req: proto.redpanda.core.admin.v2.security_pb2.ResolveOidcIdentityRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.security_pb2.ResolveOidcIdentityResponse:
        response = self.call_resolve_oidc_identity(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_refresh_oidc_keys(self, req: proto.redpanda.core.admin.v2.security_pb2.RefreshOidcKeysRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.security_pb2.RefreshOidcKeysResponse]:
        """Low-level method to call RefreshOidcKeys, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.SecurityService/RefreshOidcKeys'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.security_pb2.RefreshOidcKeysResponse, extra_headers, timeout_seconds)

    def refresh_oidc_keys(self, req: proto.redpanda.core.admin.v2.security_pb2.RefreshOidcKeysRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.security_pb2.RefreshOidcKeysResponse:
        response = self.call_refresh_oidc_keys(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    def call_revoke_oidc_sessions(self, req: proto.redpanda.core.admin.v2.security_pb2.RevokeOidcSessionsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.security_pb2.RevokeOidcSessionsResponse]:
        """Low-level method to call RevokeOidcSessions, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.SecurityService/RevokeOidcSessions'
        return self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.security_pb2.RevokeOidcSessionsResponse, extra_headers, timeout_seconds)

    def revoke_oidc_sessions(self, req: proto.redpanda.core.admin.v2.security_pb2.RevokeOidcSessionsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.security_pb2.RevokeOidcSessionsResponse:
        response = self.call_revoke_oidc_sessions(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

class AsyncSecurityServiceClient:

    def __init__(self, base_url: str, http_client: aiohttp.ClientSession, protocol: ConnectProtocol=ConnectProtocol.CONNECT_PROTOBUF):
        self.base_url = base_url
        self._connect_client = AsyncConnectClient(http_client, protocol)

    async def call_create_scram_credential(self, req: proto.redpanda.core.admin.v2.security_pb2.CreateScramCredentialRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.security_pb2.CreateScramCredentialResponse]:
        """Low-level method to call CreateScramCredential, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.SecurityService/CreateScramCredential'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.security_pb2.CreateScramCredentialResponse, extra_headers, timeout_seconds)

    async def create_scram_credential(self, req: proto.redpanda.core.admin.v2.security_pb2.CreateScramCredentialRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.security_pb2.CreateScramCredentialResponse:
        response = await self.call_create_scram_credential(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_get_scram_credential(self, req: proto.redpanda.core.admin.v2.security_pb2.GetScramCredentialRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.security_pb2.GetScramCredentialResponse]:
        """Low-level method to call GetScramCredential, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.SecurityService/GetScramCredential'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.security_pb2.GetScramCredentialResponse, extra_headers, timeout_seconds)

    async def get_scram_credential(self, req: proto.redpanda.core.admin.v2.security_pb2.GetScramCredentialRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.security_pb2.GetScramCredentialResponse:
        response = await self.call_get_scram_credential(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_list_scram_credentials(self, req: proto.redpanda.core.admin.v2.security_pb2.ListScramCredentialsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.security_pb2.ListScramCredentialsResponse]:
        """Low-level method to call ListScramCredentials, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.SecurityService/ListScramCredentials'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.security_pb2.ListScramCredentialsResponse, extra_headers, timeout_seconds)

    async def list_scram_credentials(self, req: proto.redpanda.core.admin.v2.security_pb2.ListScramCredentialsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.security_pb2.ListScramCredentialsResponse:
        response = await self.call_list_scram_credentials(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_update_scram_credential(self, req: proto.redpanda.core.admin.v2.security_pb2.UpdateScramCredentialRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.security_pb2.UpdateScramCredentialResponse]:
        """Low-level method to call UpdateScramCredential, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.SecurityService/UpdateScramCredential'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.security_pb2.UpdateScramCredentialResponse, extra_headers, timeout_seconds)

    async def update_scram_credential(self, req: proto.redpanda.core.admin.v2.security_pb2.UpdateScramCredentialRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.security_pb2.UpdateScramCredentialResponse:
        response = await self.call_update_scram_credential(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_delete_scram_credential(self, req: proto.redpanda.core.admin.v2.security_pb2.DeleteScramCredentialRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.security_pb2.DeleteScramCredentialResponse]:
        """Low-level method to call DeleteScramCredential, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.SecurityService/DeleteScramCredential'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.security_pb2.DeleteScramCredentialResponse, extra_headers, timeout_seconds)

    async def delete_scram_credential(self, req: proto.redpanda.core.admin.v2.security_pb2.DeleteScramCredentialRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.security_pb2.DeleteScramCredentialResponse:
        response = await self.call_delete_scram_credential(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_create_role(self, req: proto.redpanda.core.admin.v2.security_pb2.CreateRoleRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.security_pb2.CreateRoleResponse]:
        """Low-level method to call CreateRole, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.SecurityService/CreateRole'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.security_pb2.CreateRoleResponse, extra_headers, timeout_seconds)

    async def create_role(self, req: proto.redpanda.core.admin.v2.security_pb2.CreateRoleRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.security_pb2.CreateRoleResponse:
        response = await self.call_create_role(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_get_role(self, req: proto.redpanda.core.admin.v2.security_pb2.GetRoleRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.security_pb2.GetRoleResponse]:
        """Low-level method to call GetRole, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.SecurityService/GetRole'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.security_pb2.GetRoleResponse, extra_headers, timeout_seconds)

    async def get_role(self, req: proto.redpanda.core.admin.v2.security_pb2.GetRoleRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.security_pb2.GetRoleResponse:
        response = await self.call_get_role(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_list_roles(self, req: proto.redpanda.core.admin.v2.security_pb2.ListRolesRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.security_pb2.ListRolesResponse]:
        """Low-level method to call ListRoles, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.SecurityService/ListRoles'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.security_pb2.ListRolesResponse, extra_headers, timeout_seconds)

    async def list_roles(self, req: proto.redpanda.core.admin.v2.security_pb2.ListRolesRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.security_pb2.ListRolesResponse:
        response = await self.call_list_roles(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_add_role_members(self, req: proto.redpanda.core.admin.v2.security_pb2.AddRoleMembersRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.security_pb2.AddRoleMembersResponse]:
        """Low-level method to call AddRoleMembers, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.SecurityService/AddRoleMembers'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.security_pb2.AddRoleMembersResponse, extra_headers, timeout_seconds)

    async def add_role_members(self, req: proto.redpanda.core.admin.v2.security_pb2.AddRoleMembersRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.security_pb2.AddRoleMembersResponse:
        response = await self.call_add_role_members(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_remove_role_members(self, req: proto.redpanda.core.admin.v2.security_pb2.RemoveRoleMembersRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.security_pb2.RemoveRoleMembersResponse]:
        """Low-level method to call RemoveRoleMembers, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.SecurityService/RemoveRoleMembers'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.security_pb2.RemoveRoleMembersResponse, extra_headers, timeout_seconds)

    async def remove_role_members(self, req: proto.redpanda.core.admin.v2.security_pb2.RemoveRoleMembersRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.security_pb2.RemoveRoleMembersResponse:
        response = await self.call_remove_role_members(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_delete_role(self, req: proto.redpanda.core.admin.v2.security_pb2.DeleteRoleRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.security_pb2.DeleteRoleResponse]:
        """Low-level method to call DeleteRole, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.SecurityService/DeleteRole'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.security_pb2.DeleteRoleResponse, extra_headers, timeout_seconds)

    async def delete_role(self, req: proto.redpanda.core.admin.v2.security_pb2.DeleteRoleRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.security_pb2.DeleteRoleResponse:
        response = await self.call_delete_role(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_list_current_user_roles(self, req: proto.redpanda.core.admin.v2.security_pb2.ListCurrentUserRolesRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.security_pb2.ListCurrentUserRolesResponse]:
        """Low-level method to call ListCurrentUserRoles, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.SecurityService/ListCurrentUserRoles'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.security_pb2.ListCurrentUserRolesResponse, extra_headers, timeout_seconds)

    async def list_current_user_roles(self, req: proto.redpanda.core.admin.v2.security_pb2.ListCurrentUserRolesRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.security_pb2.ListCurrentUserRolesResponse:
        response = await self.call_list_current_user_roles(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_resolve_oidc_identity(self, req: proto.redpanda.core.admin.v2.security_pb2.ResolveOidcIdentityRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.security_pb2.ResolveOidcIdentityResponse]:
        """Low-level method to call ResolveOidcIdentity, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.SecurityService/ResolveOidcIdentity'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.security_pb2.ResolveOidcIdentityResponse, extra_headers, timeout_seconds)

    async def resolve_oidc_identity(self, req: proto.redpanda.core.admin.v2.security_pb2.ResolveOidcIdentityRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.security_pb2.ResolveOidcIdentityResponse:
        response = await self.call_resolve_oidc_identity(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_refresh_oidc_keys(self, req: proto.redpanda.core.admin.v2.security_pb2.RefreshOidcKeysRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.security_pb2.RefreshOidcKeysResponse]:
        """Low-level method to call RefreshOidcKeys, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.SecurityService/RefreshOidcKeys'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.security_pb2.RefreshOidcKeysResponse, extra_headers, timeout_seconds)

    async def refresh_oidc_keys(self, req: proto.redpanda.core.admin.v2.security_pb2.RefreshOidcKeysRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.security_pb2.RefreshOidcKeysResponse:
        response = await self.call_refresh_oidc_keys(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

    async def call_revoke_oidc_sessions(self, req: proto.redpanda.core.admin.v2.security_pb2.RevokeOidcSessionsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> UnaryOutput[proto.redpanda.core.admin.v2.security_pb2.RevokeOidcSessionsResponse]:
        """Low-level method to call RevokeOidcSessions, granting access to errors and metadata"""
        url = self.base_url + '/redpanda.core.admin.v2.SecurityService/RevokeOidcSessions'
        return await self._connect_client.call_unary(url, req, proto.redpanda.core.admin.v2.security_pb2.RevokeOidcSessionsResponse, extra_headers, timeout_seconds)

    async def revoke_oidc_sessions(self, req: proto.redpanda.core.admin.v2.security_pb2.RevokeOidcSessionsRequest, extra_headers: HeaderInput | None=None, timeout_seconds: float | None=None) -> proto.redpanda.core.admin.v2.security_pb2.RevokeOidcSessionsResponse:
        response = await self.call_revoke_oidc_sessions(req, extra_headers, timeout_seconds)
        err = response.error()
        if err is not None:
            raise err
        msg = response.message()
        if msg is None:
            raise ConnectProtocolError('missing response message')
        return msg

@typing.runtime_checkable
class SecurityServiceProtocol(typing.Protocol):

    def create_scram_credential(self, req: ClientRequest[proto.redpanda.core.admin.v2.security_pb2.CreateScramCredentialRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.security_pb2.CreateScramCredentialResponse]:
        ...

    def get_scram_credential(self, req: ClientRequest[proto.redpanda.core.admin.v2.security_pb2.GetScramCredentialRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.security_pb2.GetScramCredentialResponse]:
        ...

    def list_scram_credentials(self, req: ClientRequest[proto.redpanda.core.admin.v2.security_pb2.ListScramCredentialsRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.security_pb2.ListScramCredentialsResponse]:
        ...

    def update_scram_credential(self, req: ClientRequest[proto.redpanda.core.admin.v2.security_pb2.UpdateScramCredentialRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.security_pb2.UpdateScramCredentialResponse]:
        ...

    def delete_scram_credential(self, req: ClientRequest[proto.redpanda.core.admin.v2.security_pb2.DeleteScramCredentialRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.security_pb2.DeleteScramCredentialResponse]:
        ...

    def create_role(self, req: ClientRequest[proto.redpanda.core.admin.v2.security_pb2.CreateRoleRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.security_pb2.CreateRoleResponse]:
        ...

    def get_role(self, req: ClientRequest[proto.redpanda.core.admin.v2.security_pb2.GetRoleRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.security_pb2.GetRoleResponse]:
        ...

    def list_roles(self, req: ClientRequest[proto.redpanda.core.admin.v2.security_pb2.ListRolesRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.security_pb2.ListRolesResponse]:
        ...

    def add_role_members(self, req: ClientRequest[proto.redpanda.core.admin.v2.security_pb2.AddRoleMembersRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.security_pb2.AddRoleMembersResponse]:
        ...

    def remove_role_members(self, req: ClientRequest[proto.redpanda.core.admin.v2.security_pb2.RemoveRoleMembersRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.security_pb2.RemoveRoleMembersResponse]:
        ...

    def delete_role(self, req: ClientRequest[proto.redpanda.core.admin.v2.security_pb2.DeleteRoleRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.security_pb2.DeleteRoleResponse]:
        ...

    def list_current_user_roles(self, req: ClientRequest[proto.redpanda.core.admin.v2.security_pb2.ListCurrentUserRolesRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.security_pb2.ListCurrentUserRolesResponse]:
        ...

    def resolve_oidc_identity(self, req: ClientRequest[proto.redpanda.core.admin.v2.security_pb2.ResolveOidcIdentityRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.security_pb2.ResolveOidcIdentityResponse]:
        ...

    def refresh_oidc_keys(self, req: ClientRequest[proto.redpanda.core.admin.v2.security_pb2.RefreshOidcKeysRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.security_pb2.RefreshOidcKeysResponse]:
        ...

    def revoke_oidc_sessions(self, req: ClientRequest[proto.redpanda.core.admin.v2.security_pb2.RevokeOidcSessionsRequest]) -> ServerResponse[proto.redpanda.core.admin.v2.security_pb2.RevokeOidcSessionsResponse]:
        ...
SECURITY_SERVICE_PATH_PREFIX = '/redpanda.core.admin.v2.SecurityService'

def wsgi_security_service(implementation: SecurityServiceProtocol) -> WSGIApplication:
    app = ConnectWSGI()
    app.register_unary_rpc('/redpanda.core.admin.v2.SecurityService/CreateScramCredential', implementation.create_scram_credential, proto.redpanda.core.admin.v2.security_pb2.CreateScramCredentialRequest)
    app.register_unary_rpc('/redpanda.core.admin.v2.SecurityService/GetScramCredential', implementation.get_scram_credential, proto.redpanda.core.admin.v2.security_pb2.GetScramCredentialRequest)
    app.register_unary_rpc('/redpanda.core.admin.v2.SecurityService/ListScramCredentials', implementation.list_scram_credentials, proto.redpanda.core.admin.v2.security_pb2.ListScramCredentialsRequest)
    app.register_unary_rpc('/redpanda.core.admin.v2.SecurityService/UpdateScramCredential', implementation.update_scram_credential, proto.redpanda.core.admin.v2.security_pb2.UpdateScramCredentialRequest)
    app.register_unary_rpc('/redpanda.core.admin.v2.SecurityService/DeleteScramCredential', implementation.delete_scram_credential, proto.redpanda.core.admin.v2.security_pb2.DeleteScramCredentialRequest)
    app.register_unary_rpc('/redpanda.core.admin.v2.SecurityService/CreateRole', implementation.create_role, proto.redpanda.core.admin.v2.security_pb2.CreateRoleRequest)
    app.register_unary_rpc('/redpanda.core.admin.v2.SecurityService/GetRole', implementation.get_role, proto.redpanda.core.admin.v2.security_pb2.GetRoleRequest)
    app.register_unary_rpc('/redpanda.core.admin.v2.SecurityService/ListRoles', implementation.list_roles, proto.redpanda.core.admin.v2.security_pb2.ListRolesRequest)
    app.register_unary_rpc('/redpanda.core.admin.v2.SecurityService/AddRoleMembers', implementation.add_role_members, proto.redpanda.core.admin.v2.security_pb2.AddRoleMembersRequest)
    app.register_unary_rpc('/redpanda.core.admin.v2.SecurityService/RemoveRoleMembers', implementation.remove_role_members, proto.redpanda.core.admin.v2.security_pb2.RemoveRoleMembersRequest)
    app.register_unary_rpc('/redpanda.core.admin.v2.SecurityService/DeleteRole', implementation.delete_role, proto.redpanda.core.admin.v2.security_pb2.DeleteRoleRequest)
    app.register_unary_rpc('/redpanda.core.admin.v2.SecurityService/ListCurrentUserRoles', implementation.list_current_user_roles, proto.redpanda.core.admin.v2.security_pb2.ListCurrentUserRolesRequest)
    app.register_unary_rpc('/redpanda.core.admin.v2.SecurityService/ResolveOidcIdentity', implementation.resolve_oidc_identity, proto.redpanda.core.admin.v2.security_pb2.ResolveOidcIdentityRequest)
    app.register_unary_rpc('/redpanda.core.admin.v2.SecurityService/RefreshOidcKeys', implementation.refresh_oidc_keys, proto.redpanda.core.admin.v2.security_pb2.RefreshOidcKeysRequest)
    app.register_unary_rpc('/redpanda.core.admin.v2.SecurityService/RevokeOidcSessions', implementation.revoke_oidc_sessions, proto.redpanda.core.admin.v2.security_pb2.RevokeOidcSessionsRequest)
    return app