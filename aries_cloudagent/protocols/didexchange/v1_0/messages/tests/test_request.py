from unittest import mock

from asynctest import TestCase as AsyncTestCase

from ......connections.models.diddoc import (
    DIDDoc,
    PublicKey,
    PublicKeyType,
    Service,
)
from ......messaging.decorators.attach_decorator import AttachDecorator
from ......wallet.basic import BasicWallet

from .....didcomm_prefix import DIDCommPrefix

from ...message_types import CONN23_REQUEST

from ..request import Conn23Request


class TestConfig:
    test_seed = "testseed000000000000000000000001"
    test_did = "55GkHamhTU1ZbTbV2ab9DE"
    test_verkey = "3Dn1SJNPaCXcvvJvSbsFWP2xaCjMom3can8CQNhWrTRx"
    test_label = "Label"
    test_endpoint = "http://localhost"

    def make_did_doc(self):
        doc = DIDDoc(did=self.test_did)
        controller = self.test_did
        ident = "1"
        pk_value = self.test_verkey
        pk = PublicKey(
            self.test_did,
            ident,
            pk_value,
            PublicKeyType.ED25519_SIG_2018,
            controller,
            False,
        )
        doc.set(pk)
        recip_keys = [pk]
        router_keys = []
        service = Service(
            self.test_did,
            "indy",
            "IndyAgent",
            recip_keys,
            router_keys,
            self.test_endpoint,
        )
        doc.set(service)
        return doc


class TestConn23Request(AsyncTestCase, TestConfig):
    async def setUp(self):
        self.wallet = BasicWallet()
        self.did_info = await self.wallet.create_local_did()

        did_doc_attach = AttachDecorator.from_indy_dict(self.make_did_doc().serialize())
        await did_doc_attach.data.sign(self.did_info.verkey, self.wallet)

        self.request = Conn23Request(
            label=TestConfig.test_label,
            did=TestConfig.test_did,
            did_doc_attach=did_doc_attach,
        )

    def test_init(self):
        """Test initialization."""
        assert self.request.label == TestConfig.test_label
        assert self.request.did == TestConfig.test_did

    def test_type(self):
        """Test type."""
        assert self.request._type == DIDCommPrefix.qualify_current(CONN23_REQUEST)

    @mock.patch(
        "aries_cloudagent.protocols.didexchange.v1_0.messages."
        "request.Conn23RequestSchema.load"
    )
    def test_deserialize(self, mock_request_schema_load):
        """
        Test deserialization.
        """
        obj = {"obj": "obj"}

        request = Conn23Request.deserialize(obj)
        mock_request_schema_load.assert_called_once_with(obj)

        assert request is mock_request_schema_load.return_value

    @mock.patch(
        "aries_cloudagent.protocols.didexchange.v1_0.messages."
        "request.Conn23RequestSchema.dump"
    )
    def test_serialize(self, mock_request_schema_dump):
        """
        Test serialization.
        """
        request_dict = self.request.serialize()
        mock_request_schema_dump.assert_called_once_with(self.request)

        assert request_dict is mock_request_schema_dump.return_value


class TestConn23RequestSchema(AsyncTestCase, TestConfig):
    """Test request schema."""

    async def setUp(self):
        self.wallet = BasicWallet()
        self.did_info = await self.wallet.create_local_did()

        did_doc_attach = AttachDecorator.from_indy_dict(self.make_did_doc().serialize())
        await did_doc_attach.data.sign(self.did_info.verkey, self.wallet)

        self.request = Conn23Request(
            label=TestConfig.test_label,
            did=TestConfig.test_did,
            did_doc_attach=did_doc_attach,
        )

    async def test_make_model(self):
        data = self.request.serialize()
        model_instance = Conn23Request.deserialize(data)
        assert type(model_instance) is type(self.request)
