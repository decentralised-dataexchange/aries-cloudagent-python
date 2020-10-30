"""Classes to manage connection establishment under RFC 23 (DID exchange)."""

import json
import logging

from typing import Sequence, Tuple, Union

from ....cache.base import BaseCache
from ....connections.models.conn23rec import Conn23Record
from ....connections.models.connection_target import ConnectionTarget
from ....connections.models.diddoc import (
    DIDDoc,
    PublicKey,
    PublicKeyType,
    Service,
)
from ....config.base import InjectorError
from ....config.injection_context import InjectionContext
from ....core.error import BaseError
from ....ledger.base import BaseLedger
from ....messaging.decorators.attach_decorator import AttachDecorator
from ....messaging.responder import BaseResponder
from ....storage.base import BaseStorage
from ....storage.error import StorageError, StorageNotFoundError
from ....storage.record import StorageRecord
from ....transport.inbound.receipt import MessageReceipt
from ....wallet.base import BaseWallet, DIDInfo
from ....wallet.crypto import create_keypair, seed_to_did
from ....wallet.error import WalletNotFoundError
from ....wallet.util import bytes_to_b58, naked_to_did_key

from ...didcomm_prefix import DIDCommPrefix
from ...out_of_band.v1_0.message_types import INVITATION as OOB_INVITATION
from ...out_of_band.v1_0.messages.invitation import InvitationMessage as OOBInvitation
from ...out_of_band.v1_0.messages.service import Service as OOBService
from ...routing.v1_0.manager import RoutingManager

from .messages.complete import Conn23Complete
from .messages.request import Conn23Request
from .messages.response import Conn23Response
from .messages.problem_report import ProblemReportReason


class Conn23ManagerError(BaseError):
    """Connection error."""


class Conn23Manager:
    """Class for managing connections under RFC 23 (DID exchange)."""

    RECORD_TYPE_DID_DOC = "did_doc"
    RECORD_TYPE_DID_KEY = "did_key"

    def __init__(self, context: InjectionContext):
        """
        Initialize a Conn23Manager.

        Args:
            context: The context for this connection manager
        """
        self._context = context
        self._logger = logging.getLogger(__name__)

    @property
    def context(self) -> InjectionContext:
        """
        Accessor for the current injection context.

        Returns:
            The injection context for this connection manager

        """
        return self._context

    async def create_invitation(
        self,
        my_label: str = None,
        my_endpoint: str = None,
        auto_accept: bool = None,
        public: bool = False,
        multi_use: bool = False,
        alias: str = None,
        include_handshake: bool = False,
    ) -> Tuple[Conn23Record, OOBInvitation]:
        """
        Generate new connection invitation.

        This interaction represents an out-of-band communication channel. In the future
        and in practice, these sort of invitations will be received over any number of
        channels such as SMS, Email, QR Code, NFC, etc.

        Args:
            my_label: label for this connection
            my_endpoint: endpoint where other party can reach me
            auto_accept: auto-accept a corresponding connection request
                (None to use config)
            public: set to create an invitation from the public DID
            multi_use: set to True to create an invitation for multiple use
            alias: optional alias to apply to connection for later use
            include_handshake: whether to include handshake protocols

        Returns:
            A tuple of the new `Conn23Record` and invitation instance

        """
        if not my_label:
            my_label = self.context.settings.get("default_label")
        wallet: BaseWallet = await self.context.inject(BaseWallet)

        if public:
            if not self.context.settings.get("public_invites"):
                raise Conn23ManagerError("Public invitations are not enabled")

            public_did = await wallet.get_public_did()
            if not public_did:
                raise Conn23ManagerError(
                    "Cannot create public invitation with no public DID"
                )

            if multi_use:
                raise Conn23ManagerError(
                    "Cannot use public and multi_use at the same time"
                )

            invitation = OOBInvitation(
                label=my_label,
                handshake_protocols=(
                    [DIDCommPrefix.qualify_current(OOB_INVITATION)]
                    if include_handshake
                    else None
                ),
                service=[f"did:sov:{public_did.did}"],
            )

            return (None, invitation)

        invitation_mode = (
            Conn23Record.INVITATION_MODE_MULTI
            if multi_use
            else Conn23Record.INVITATION_MODE_ONCE
        )

        if not my_endpoint:
            my_endpoint = self.context.settings.get("default_endpoint")
        accept = (
            Conn23Record.ACCEPT_AUTO
            if (
                auto_accept
                or (
                    auto_accept is None
                    and self.context.settings.get("debug.auto_accept_requests")
                )
            )
            else Conn23Record.ACCEPT_MANUAL
        )

        # Create and store new invitation key
        connection_key = await wallet.create_signing_key()

        # Create connection invitation message
        # Note: Need to split this into two stages to support inbound routing of invites
        # Would want to reuse create_did_document and convert the result
        invitation = OOBInvitation(
            label=my_label,
            handshake_protocols=(
                [DIDCommPrefix.qualify_current("didexchange/1.0/invitation")]
                if include_handshake
                else None
            ),
            service=[
                OOBService(
                    _id="#inline",
                    _type="did-communication",
                    recipient_keys=[naked_to_did_key(connection_key.verkey)],
                    service_endpoint=my_endpoint,
                )
            ],
        )

        # Create connection record
        connection = Conn23Record(
            invitation_key=connection_key.verkey,
            their_role=Conn23Record.Role.REQUESTER.rfc23,
            state=Conn23Record.STATE_INVITATION,
            accept=accept,
            invitation_mode=invitation_mode,
            alias=alias,
        )

        await connection.save(self.context, reason="Created new invitation")
        await connection.attach_invitation(self.context, invitation)

        return (connection, invitation)

    async def receive_invitation(
        self,
        invitation: OOBInvitation,
        auto_accept: bool = None,
        alias: str = None,
    ) -> Conn23Record:
        """
        Create a new connection record to track a received invitation.

        Args:
            invitation: The invitation to store
            auto_accept: set to auto-accept the invitation (None to use config)
            alias: optional alias to set on the record

        Returns:
            The new `Conn23Record` instance

        """
        if not invitation.service_dids:
            if invitation.service_blocks:
                if not all(
                    s.recipient_keys and s.service_endpoint
                    for s in invitation.service_blocks
                ):
                    raise Conn23ManagerError(
                        "All service blocks in invitation with no service DIDs "
                        "must contain recipient key(s) and service endpoint(s)"
                    )
            else:
                raise Conn23ManagerError(
                    "Invitation must contain service blocks or service DIDs"
                )

        accept = (
            Conn23Record.ACCEPT_AUTO
            if (
                auto_accept
                or (
                    auto_accept is None
                    and self.context.settings.get("debug.auto_accept_invites")
                )
            )
            else Conn23Record.ACCEPT_MANUAL
        )

        # Create connection record
        connection = Conn23Record(
            invitation_key=(
                invitation.service_blocks[0].recipient_keys[0]
                if invitation.service_blocks
                else None
            ),
            their_label=invitation.label,
            their_role=Conn23Record.Role.RESPONDER.rfc23,
            state=Conn23Record.STATE_INVITATION,
            accept=accept,
            alias=alias,
        )

        await connection.save(
            self.context,
            reason="Created new connection record from invitation",
            log_params={
                "invitation": invitation,
                "role": Conn23Record.Role.RESPONDER.rfc23,
            },
        )

        # Save the invitation for later processing
        await connection.attach_invitation(self.context, invitation)

        if connection.accept == Conn23Record.ACCEPT_AUTO:
            request = await self.create_request(connection)
            responder: BaseResponder = await self._context.inject(
                BaseResponder, required=False
            )
            if responder:
                await responder.send_reply(
                    request,
                    connection_id=connection.connection_id,
                )

                """
                # refetch connection for accurate state  # TODO is this necessary?
                connection = await Conn23Record.retrieve_by_id(
                    self._context, connection.connection_id
                )
                """
                connection.state = Conn23Record.STATE_REQUEST
                await connection.save(self.context, reason="Sent connection request")
        else:
            self._logger.debug("Connection invitation will await acceptance")

        return connection

    async def create_request(
        self,
        connection: Conn23Record,
        my_label: str = None,
        my_endpoint: str = None,
    ) -> Conn23Request:
        """
        Create a new connection request for a previously-received invitation.

        Args:
            connection: The `Conn23Record` representing the invitation to accept
            my_label: My label
            my_endpoint: My endpoint

        Returns:
            A new `Conn23Request` message to send to the other agent

        """
        wallet: BaseWallet = await self.context.inject(BaseWallet)
        if connection.my_did:
            my_info = await wallet.get_local_did(connection.my_did)
        else:
            # Create new DID for connection
            my_info = await wallet.create_local_did()
            connection.my_did = my_info.did

        # Create connection request message
        if my_endpoint:
            my_endpoints = [my_endpoint]
        else:
            my_endpoints = []
            default_endpoint = self.context.settings.get("default_endpoint")
            if default_endpoint:
                my_endpoints.append(default_endpoint)
            my_endpoints.extend(self.context.settings.get("additional_endpoints", []))
        did_doc = await self.create_did_document(
            my_info, connection.inbound_connection_id, my_endpoints
        )
        pthid = did_doc.service[[s for s in did_doc.service][0]].id
        attach = AttachDecorator.from_indy_dict(did_doc.serialize())
        await attach.data.sign(my_info.verkey, wallet)
        if not my_label:
            my_label = self.context.settings.get("default_label")
        request = Conn23Request(
            label=my_label,
            did=connection.my_did,
            did_doc_attach=attach,
        )
        request.assign_thread_id(thid=request._id, pthid=pthid)

        # Update connection state
        connection.request_id = request._id
        connection.state = Conn23Record.STATE_REQUEST
        await connection.save(self.context, reason="Created connection request")

        return request

    async def receive_request(
        self, request: Conn23Request, receipt: MessageReceipt
    ) -> Conn23Record:
        """
        Receive and store a connection request.

        Args:
            request: The `Conn23Request` to accept
            receipt: The message receipt

        Returns:
            The new or updated `Conn23Record` instance

        """
        Conn23Record.log_state(
            self.context, "Receiving connection request", {"request": request}
        )

        connection = None
        connection_key = None
        wallet: BaseWallet = await self.context.inject(BaseWallet)

        # Determine what key will need to sign the response
        if receipt.recipient_did_public:
            my_info = await wallet.get_local_did(receipt.recipient_did)
            connection_key = my_info.verkey
        else:
            connection_key = receipt.recipient_verkey
            try:
                connection = await Conn23Record.retrieve_by_invitation_key(
                    context=self.context,
                    invitation_key=connection_key,
                    my_role=Conn23Record.Role.RESPONDER,
                )
            except StorageNotFoundError:
                raise Conn23ManagerError("No invitation found for pairwise connection")

        invitation = None
        if connection:
            invitation = await connection.retrieve_invitation(self.context)
            connection_key = connection.invitation_key
            Conn23Record.log_state(
                self.context, "Found invitation", {"invitation": invitation}
            )

            if connection.is_multiuse_invitation:
                wallet: BaseWallet = await self.context.inject(BaseWallet)
                my_info = await wallet.create_local_did()
                new_connection = Conn23Record(
                    invitation_key=connection_key,
                    my_did=my_info.did,
                    state=Conn23Record.STATE_REQUEST,
                    accept=connection.accept,
                    their_role=connection.their_role,
                )

                await new_connection.save(
                    self.context,
                    reason="Received connection request from multi-use invitation DID",
                )
                connection = new_connection

        if not (request.did_doc_attach and request.did_doc_attach.data):
            raise Conn23ManagerError(
                "DID Doc attachment missing or has no data: "
                "cannot connect to public DID"
            )
        if not await request.did_doc_attach.data.verify(wallet):
            raise Conn23ManagerError("DID Doc signature failed verification")
        conn_did_doc = DIDDoc.from_json(request.did_doc_attach.data.signed.decode())
        if request.did != conn_did_doc.did:
            raise Conn23ManagerError(
                (
                    f"Connection DID {request.did} does not match "
                    f"DID Doc id {conn_did_doc.did}"
                ),
                error_code=ProblemReportReason.REQUEST_NOT_ACCEPTED,
            )
        await self.store_did_document(conn_did_doc)

        if connection:
            connection.their_label = request.label
            connection.their_did = request.did
            connection.state = Conn23Record.STATE_REQUEST
            await connection.save(
                self.context, reason="Received connection request from invitation"
            )
        elif not self.context.settings.get("public_invites"):
            raise Conn23ManagerError("Public invitations are not enabled")
        else:
            my_info = await wallet.create_local_did()
            connection = Conn23Record(
                invitation_key=connection_key,
                my_did=my_info.did,
                their_did=request.did,
                their_label=request.label,
                their_role=Conn23Record.Role.REQUESTER.rfc23,
                state=Conn23Record.STATE_REQUEST,
            )
            if self.context.settings.get("debug.auto_accept_requests"):
                connection.accept = Conn23Record.ACCEPT_AUTO

            await connection.save(
                self.context, reason="Received connection request from public DID"
            )

        # Attach the connection request so it can be found and responded to
        await connection.attach_request(self.context, request)

        if connection.accept == Conn23Record.ACCEPT_AUTO:
            response = await self.create_response(connection)
            responder: BaseResponder = await self._context.inject(
                BaseResponder, required=False
            )
            if responder:
                await responder.send_reply(
                    response, connection_id=connection.connection_id
                )
                """
                # refetch connection for accurate state  # TODO is this necessary?
                connection = await Conn23Record.retrieve_by_id(
                    self._context, connection.connection_id
                )
                """
                connection.state = Conn23Record.STATE_RESPONSE
                await connection.save(self.context, reason="Sent connection response")
        else:
            self._logger.debug("Connection request will await acceptance")

        return connection

    async def create_response(
        self, connection: Conn23Record, my_endpoint: str = None
    ) -> Conn23Response:
        """
        Create a connection response for a received connection request.

        Args:
            connection: The `Conn23Record` with a pending connection request
            my_endpoint: Current agent endpoint

        Returns:
            New `Conn23Response` message

        """
        Conn23Record.log_state(
            self.context,
            "Creating connection response",
            {"connection_id": connection.connection_id},
        )

        if connection.state != Conn23Record.STATE_REQUEST:
            raise Conn23ManagerError(
                f"Connection not in state {Conn23Record.STATE_REQUEST}"
            )

        request = await connection.retrieve_request(self.context)
        wallet: BaseWallet = await self.context.inject(BaseWallet)
        if connection.my_did:
            my_info = await wallet.get_local_did(connection.my_did)
        else:
            my_info = await wallet.create_local_did()
            connection.my_did = my_info.did

        # Create connection response message
        if my_endpoint:
            my_endpoints = [my_endpoint]
        else:
            my_endpoints = []
            default_endpoint = self.context.settings.get("default_endpoint")
            if default_endpoint:
                my_endpoints.append(default_endpoint)
            my_endpoints.extend(self.context.settings.get("additional_endpoints", []))
        did_doc = await self.create_did_document(
            my_info, connection.inbound_connection_id, my_endpoints
        )
        attach = AttachDecorator.from_indy_dict(did_doc.serialize())
        await attach.data.sign(connection.invitation_key, wallet)
        response = Conn23Response(did=my_info.did, did_doc_attach=attach)
        # Assign thread information
        response.assign_thread_from(request)
        response.assign_trace_from(request)
        """  # TODO - re-evaluate what code signs? With what key?
        # Sign connection field using the invitation key
        wallet: BaseWallet = await self.context.inject(BaseWallet)
        await response.sign_field("connection", connection.invitation_key, wallet)
        """

        # Update connection state
        connection.state = Conn23Record.STATE_RESPONSE
        await connection.save(
            self.context,
            reason="Created connection response",
            log_params={"response": response},
        )

        return response

    async def accept_response(
        self, response: Conn23Response, receipt: MessageReceipt
    ) -> Conn23Record:
        """
        Accept a connection response under RFC 23 (DID exchange).

        Process a Conn23Response message by looking up
        the connection request and setting up the pairwise connection.

        Args:
            response: The `Conn23Response` to accept
            receipt: The message receipt

        Returns:
            The updated `Conn23Record` representing the connection

        Raises:
            Conn23ManagerError: If there is no DID associated with the
                connection response
            Conn23ManagerError: If the corresponding connection is not
                in the request-sent state

        """
        wallet: BaseWallet = await self.context.inject(BaseWallet)

        connection = None
        if response._thread:
            # identify the request by the thread ID
            try:
                connection = await Conn23Record.retrieve_by_request_id(
                    self.context, response._thread_id
                )
            except StorageNotFoundError:
                pass

        if not connection and receipt.sender_did:
            # identify connection by the DID they used for us
            try:
                connection = await Conn23Record.retrieve_by_did(
                    context=self.context,
                    their_did=receipt.sender_did,
                    my_did=receipt.recipient_did,
                    my_role=Conn23Record.Role.REQUESTER.rfc23,
                )
            except StorageNotFoundError:
                pass

        if not connection:
            raise Conn23ManagerError(
                "No corresponding connection request found",
                error_code=ProblemReportReason.RESPONSE_NOT_ACCEPTED,
            )

        # TODO: RFC impl included STATE_RESPONSE: why?
        if connection.state != Conn23Record.STATE_REQUEST:
            raise Conn23ManagerError(
                "Cannot accept connection response for connection"
                f" in state: {connection.state}"
            )

        their_did = response.did
        if not response.did_doc_attach:
            raise Conn23ManagerError("No DIDDoc attached; cannot connect to public DID")
        conn_did_doc = await self.verify_diddoc(wallet, response.did_doc_attach)
        if their_did != conn_did_doc.did:
            raise Conn23ManagerError(
                f"Connection DID {their_did} "
                f"does not match DID doc id {conn_did_doc.did}"
            )
        await self.store_did_document(conn_did_doc)

        connection.their_did = their_did
        connection.state = Conn23Record.STATE_RESPONSE
        await connection.save(self.context, reason="Accepted connection response")

        # create and send connection-complete message
        complete = Conn23Complete()
        complete.assign_thread_from(response)
        responder: BaseResponder = await self._context.inject(
            BaseResponder, required=False
        )
        if responder:
            await responder.send_reply(complete, connection_id=connection.connection_id)

            connection.state = Conn23Record.STATE_RESPONSE
            await connection.save(self.context, reason="Sent connection complete")

        return connection

    async def accept_complete(
        self, complete: Conn23Complete, receipt: MessageReceipt
    ) -> Conn23Record:
        """
        Accept a connection complete message under RFC 23 (DID exchange).

        Process a Conn23Complete message by looking up
        the connection record and marking the exchange complete.

        Args:
            complete: The `Conn23Complete` to accept
            receipt: The message receipt

        Returns:
            The updated `Conn23Record` representing the connection

        Raises:
            Conn23ManagerError: If the corresponding connection does not exist
                or is not in the response-sent state

        """
        connection = None

        # identify the request by the thread ID
        try:
            connection = await Conn23Record.retrieve_by_request_id(
                self.context, complete._thread_id
            )
        except StorageNotFoundError:
            raise Conn23ManagerError(
                "No corresponding connection request found",
                error_code=ProblemReportReason.COMPLETE_NOT_ACCEPTED,
            )

        connection.state = Conn23Record.STATE_COMPLETED
        await connection.save(self.context, reason="Received connection complete")

        return connection

    async def find_connection(
        self,
        their_did: str,
        my_did: str = None,
        my_verkey: str = None,
        auto_complete=False,
    ) -> Conn23Record:
        """
        Look up existing connection information for a sender verkey.

        Args:
            their_did: Their DID
            my_did: My DID
            my_verkey: My verkey
            auto_complete: Whether to promote connection automatically to completed

        Returns:
            The located `Conn23Record`, if any

        """
        connection = None
        if their_did:
            try:
                connection = await Conn23Record.retrieve_by_did(
                    self.context, their_did, my_did
                )
            except StorageNotFoundError:
                pass

        if (
            connection
            and connection.state == Conn23Record.STATE_RESPONSE
            and auto_complete
        ):
            connection.state = Conn23Record.STATE_COMPLETED

            await connection.save(
                self.context, reason="Connection promoted to completed"
            )

        if not connection and my_verkey:
            try:
                connection = await Conn23Record.retrieve_by_invitation_key(
                    context=self.context,
                    invitation_key=my_verkey,
                    my_role=Conn23Record.Role.REQUESTER.rfc23,
                )
            except StorageError:
                self._logger.warning(
                    "No corresponding connection record found for sender verkey: %s",
                    my_verkey,
                )
                pass

        return connection

    async def find_inbound_connection(self, receipt: MessageReceipt) -> Conn23Record:
        """
        Deserialize an incoming message and further populate the request context.

        Args:
            receipt: The message receipt

        Returns:
            The `Conn23Record` associated with the expanded message, if any

        """

        cache_key = None
        connection = None
        resolved = False

        if receipt.sender_verkey and receipt.recipient_verkey:
            cache_key = (
                f"connection_by_verkey::{receipt.sender_verkey}"
                f"::{receipt.recipient_verkey}"
            )
            cache: BaseCache = await self.context.inject(BaseCache, required=False)
            if cache:
                async with cache.acquire(cache_key) as entry:
                    if entry.result:
                        cached = entry.result
                        receipt.sender_did = cached["sender_did"]
                        receipt.recipient_did_public = cached["recipient_did_public"]
                        receipt.recipient_did = cached["recipient_did"]
                        connection = await Conn23Record.retrieve_by_id(
                            self.context, cached["id"]
                        )
                    else:
                        connection = await self.resolve_inbound_connection(receipt)
                        if connection:
                            cache_val = {
                                "id": connection.connection_id,
                                "sender_did": receipt.sender_did,
                                "recipient_did": receipt.recipient_did,
                                "recipient_did_public": receipt.recipient_did_public,
                            }
                            await entry.set_result(cache_val, 3600)
                        resolved = True

        if not connection and not resolved:
            connection = await self.resolve_inbound_connection(receipt)
        return connection

    async def resolve_inbound_connection(self, receipt: MessageReceipt) -> Conn23Record:
        """
        Populate the receipt DID information and find the related `Conn23Record`.

        Args:
            receipt: The message receipt

        Returns:
            The `Conn23Record` associated with the expanded message, if any

        """

        if receipt.sender_verkey:
            try:
                receipt.sender_did = await self.find_did_for_key(receipt.sender_verkey)
            except StorageNotFoundError:
                self._logger.warning(
                    "No corresponding DID found for sender verkey: %s",
                    receipt.sender_verkey,
                )

        if receipt.recipient_verkey:
            try:
                wallet: BaseWallet = await self.context.inject(BaseWallet)
                my_info = await wallet.get_local_did_for_verkey(
                    receipt.recipient_verkey
                )
                receipt.recipient_did = my_info.did
                if "public" in my_info.metadata and my_info.metadata["public"]:
                    receipt.recipient_did_public = True
            except InjectorError:
                self._logger.warning(
                    "Cannot resolve recipient verkey, no wallet defined by "
                    "context: %s",
                    receipt.recipient_verkey,
                )
            except WalletNotFoundError:
                self._logger.warning(
                    "No corresponding DID found for recipient verkey: %s",
                    receipt.recipient_verkey,
                )

        return await self.find_connection(
            receipt.sender_did, receipt.recipient_did, receipt.recipient_verkey, True
        )

    async def create_did_document(
        self,
        did_info: DIDInfo,
        inbound_connection_id: str = None,
        svc_endpoints: Sequence[str] = None,
    ) -> DIDDoc:
        """Create our DID doc for a given DID.

        Args:
            did_info: The DID information (DID and verkey) used in the connection
            inbound_connection_id: The ID of the inbound routing connection to use
            svc_endpoints: Custom endpoints for the DID Document

        Returns:
            The prepared `DIDDoc` instance

        """

        did_doc = DIDDoc(did=did_info.did)
        did_controller = did_info.did
        did_key = did_info.verkey
        pk = PublicKey(
            did_info.did,
            "1",
            did_key,
            PublicKeyType.ED25519_SIG_2018,
            did_controller,
            True,
        )
        did_doc.set(pk)

        router_id = inbound_connection_id
        routing_keys = []
        router_idx = 1
        while router_id:
            # look up routing connection information
            router = await Conn23Record.retrieve_by_id(self.context, router_id)
            if router.state != Conn23Record.STATE_COMPLETED:
                raise Conn23ManagerError(
                    f"Router connection not completed: {router_id}"
                )
            routing_doc, _ = await self.fetch_did_document(router.their_did)
            if not routing_doc.service:
                raise Conn23ManagerError(
                    f"No services defined by routing DIDDoc: {router_id}"
                )
            for service in routing_doc.service.values():
                if not service.endpoint:
                    raise Conn23ManagerError(
                        "Routing DIDDoc service has no service endpoint"
                    )
                if not service.recip_keys:
                    raise Conn23ManagerError(
                        "Routing DIDDoc service has no recipient key(s)"
                    )
                rk = PublicKey(
                    did_info.did,
                    f"routing-{router_idx}",
                    service.recip_keys[0].value,
                    PublicKeyType.ED25519_SIG_2018,
                    did_controller,
                    True,
                )
                routing_keys.append(rk)
                svc_endpoints = [service.endpoint]
                break
            router_id = router.inbound_connection_id

        for (endpoint_index, svc_endpoint) in enumerate(svc_endpoints or []):
            endpoint_ident = "indy" if endpoint_index == 0 else f"indy{endpoint_index}"
            service = Service(
                did_info.did,
                endpoint_ident,
                "IndyAgent",
                [pk],
                routing_keys,
                svc_endpoint,
            )
            did_doc.set(service)

        return did_doc

    async def fetch_did_document(self, did: str) -> Tuple[DIDDoc, StorageRecord]:
        """Retrieve a DID Document for a given DID.

        Args:
            did: The DID for which to search
        """
        storage: BaseStorage = await self.context.inject(BaseStorage)
        record = await storage.search_records(
            Conn23Manager.RECORD_TYPE_DID_DOC, {"did": did}
        ).fetch_single()
        return (DIDDoc.from_json(record.value), record)

    async def store_did_document(self, did_doc: DIDDoc):
        """Store a DID document.

        Args:
            did_doc: The `DIDDoc` instance to persist
        """
        assert did_doc.did
        storage: BaseStorage = await self.context.inject(BaseStorage)
        try:
            stored_doc, record = await self.fetch_did_document(did_doc.did)
        except StorageNotFoundError:
            record = StorageRecord(
                Conn23Manager.RECORD_TYPE_DID_DOC,
                did_doc.to_json(),
                {"did": did_doc.did},
            )
            await storage.add_record(record)
        else:
            await storage.update_record_value(record, did_doc.to_json())
        await self.remove_keys_for_did(did_doc.did)
        for key in did_doc.pubkey.values():
            if key.controller == did_doc.did:
                await self.add_key_for_did(did_doc.did, key.value)

    async def add_key_for_did(self, did: str, key: str):
        """Store a verkey for lookup against a DID.

        Args:
            did: The DID to associate with this key
            key: The verkey to be added
        """
        record = StorageRecord(
            Conn23Manager.RECORD_TYPE_DID_KEY, key, {"did": did, "key": key}
        )
        storage: BaseStorage = await self.context.inject(BaseStorage)
        await storage.add_record(record)

    async def find_did_for_key(self, key: str) -> str:
        """Find the DID previously associated with a key.

        Args:
            key: The verkey to look up
        """
        storage: BaseStorage = await self.context.inject(BaseStorage)
        record = await storage.search_records(
            Conn23Manager.RECORD_TYPE_DID_KEY, {"key": key}
        ).fetch_single()
        return record.tags["did"]

    async def remove_keys_for_did(self, did: str):
        """Remove all keys associated with a DID.

        Args:
            did: The DID for which to remove keys
        """
        storage: BaseStorage = await self.context.inject(BaseStorage)
        keys = await storage.search_records(
            Conn23Manager.RECORD_TYPE_DID_KEY, {"did": did}
        ).fetch_all()
        for record in keys:
            await storage.delete_record(record)

    async def get_connection_targets(
        self, *, connection_id: str = None, connection: Conn23Record = None
    ):
        """Create a connection target from a `Conn23Record`.

        Args:
            connection_id: The connection ID to search for
            connection: The connection record itself, if already available
        """
        if not connection_id:
            connection_id = connection.connection_id
        cache: BaseCache = await self.context.inject(BaseCache, required=False)
        cache_key = f"connection_target::{connection_id}"
        if cache:
            async with cache.acquire(cache_key) as entry:
                if entry.result:
                    targets = [
                        ConnectionTarget.deserialize(row) for row in entry.result
                    ]
                else:
                    if not connection:
                        connection = await Conn23Record.retrieve_by_id(
                            self.context, connection_id
                        )
                    targets = await self.fetch_connection_targets(connection)
                    await entry.set_result([row.serialize() for row in targets], 3600)
        else:
            targets = await self.fetch_connection_targets(connection)
        return targets

    async def fetch_connection_targets(
        self, connection: Conn23Record
    ) -> Sequence[ConnectionTarget]:
        """Get a list of connection target from a `Conn23Record`.

        Args:
            connection: The connection record (with associated `DIDDoc`)
                used to generate the connection target
        """

        if not connection.my_did:
            self._logger.debug("No local DID associated with connection")
            return None

        wallet: BaseWallet = await self.context.inject(BaseWallet)
        my_info = await wallet.get_local_did(connection.my_did)
        results = None

        """ was (for RFC 160)
            # KEEP THIS COMMENT AROUND until certain the logic maps OK to RFC 23
        if (
            connection.state
            in (ConnectionRecord.STATE_INVITATION, ConnectionRecord.STATE_REQUEST)
            and connection.initiator == ConnectionRecord.INITIATOR_EXTERNAL
        ):
        """
        if (
            connection.state
            in (Conn23Record.STATE_INVITATION, Conn23Record.STATE_REQUEST)
            and connection.their_role == Conn23Record.Role.REQUESTER.rfc23
        ):
            invitation = await connection.retrieve_invitation(self.context)
            if invitation.did:
                # populate recipient keys and endpoint from the ledger
                ledger: BaseLedger = await self.context.inject(
                    BaseLedger, required=False
                )
                if not ledger:
                    raise Conn23ManagerError(
                        "Cannot resolve DID without ledger instance"
                    )
                async with ledger:
                    endpoint = await ledger.get_endpoint_for_did(invitation.did)
                    recipient_keys = [await ledger.get_key_for_did(invitation.did)]
                    routing_keys = []
            else:
                endpoint = invitation.endpoint
                recipient_keys = invitation.recipient_keys
                routing_keys = invitation.routing_keys

            results = [
                ConnectionTarget(
                    did=connection.their_did,
                    endpoint=endpoint,
                    label=invitation.label,
                    recipient_keys=recipient_keys,
                    routing_keys=routing_keys,
                    sender_key=my_info.verkey,
                )
            ]
        else:
            if not connection.their_did:
                self._logger.debug(
                    "No target DID associated with connection %s",
                    connection.connection_id,
                )
                return None

            did_doc, _ = await self.fetch_did_document(connection.their_did)
            results = self.diddoc_connection_targets(
                did_doc, my_info.verkey, connection.their_label
            )

        return results

    async def verify_diddoc(
        self, wallet: BaseWallet, attached: AttachDecorator
    ) -> DIDDoc:
        """Verify DIDDoc attachment and return signed data."""
        signed_diddoc_bytes = attached.data.signed
        if not signed_diddoc_bytes:
            raise Conn23ManagerError("DID doc attachment is not signed.")
        if not await attached.data.verify(wallet):
            raise Conn23ManagerError(
                f"DID doc attachment signature failed verification"
            )

        return DIDDoc.deserialize(json.loads(signed_diddoc_bytes.decode()))

    def diddoc_connection_targets(
        self, doc: DIDDoc, sender_verkey: str, their_label: str = None
    ) -> Sequence[ConnectionTarget]:
        """Get a list of connection targets from a DID Document.

        Args:
            doc: The DID Document to create the target from
            sender_verkey: The verkey we are using
            their_label: The connection label they are using
        """

        if not doc:
            raise Conn23ManagerError("No DIDDoc provided for connection target")
        if not doc.did:
            raise Conn23ManagerError("DIDDoc has no DID")
        if not doc.service:
            raise Conn23ManagerError("No services defined by DIDDoc")

        targets = []
        for service in doc.service.values():
            if service.recip_keys:
                targets.append(
                    ConnectionTarget(
                        did=doc.did,
                        endpoint=service.endpoint,
                        label=their_label,
                        recipient_keys=[
                            key.value for key in (service.recip_keys or ())
                        ],
                        routing_keys=[
                            key.value for key in (service.routing_keys or ())
                        ],
                        sender_key=sender_verkey,
                    )
                )
        return targets

    async def establish_inbound(
        self, connection: Conn23Record, inbound_connection_id: str, outbound_handler
    ) -> str:
        """Assign the inbound routing connection for a connection record.

        Returns: the current routing state ("request")
        """

        # The connection must have a verkey, but in the case of a received
        # invitation we might not have created one yet
        wallet: BaseWallet = await self.context.inject(BaseWallet)
        if connection.my_did:
            my_info = await wallet.get_local_did(connection.my_did)
        else:
            # Create new DID for connection
            my_info = await wallet.create_local_did()
            connection.my_did = my_info.did

        try:
            router = await Conn23Record.retrieve_by_id(
                self.context, inbound_connection_id
            )
        except StorageNotFoundError:
            raise Conn23ManagerError(
                f"Routing connection not found: {inbound_connection_id}"
            )
        if not router.is_ready:
            raise Conn23ManagerError(
                f"Routing connection is not ready: {inbound_connection_id}"
            )
        connection.inbound_connection_id = inbound_connection_id

        route_mgr = RoutingManager(self.context)

        await route_mgr.send_create_route(
            inbound_connection_id, my_info.verkey, outbound_handler
        )
        connection.routing_state = Conn23Record.ROUTING_STATE_REQUEST
        await connection.save(self.context)
        return connection.routing_state

    async def update_inbound(
        self, inbound_connection_id: str, recip_verkey: str, routing_state: str
    ):
        """Activate connections once a route has been established.

        Looks up pending connections associated with the inbound routing
        connection and marks the routing as complete.
        """
        conns = await Conn23Record.query(
            self.context, {"inbound_connection_id": inbound_connection_id}
        )
        wallet: BaseWallet = await self.context.inject(BaseWallet)

        for connection in conns:
            # check the recipient key
            if not connection.my_did:
                continue
            conn_info = await wallet.get_local_did(connection.my_did)
            if conn_info.verkey == recip_verkey:
                connection.routing_state = routing_state
                await connection.save(self.context)
