"""List data certificate types message."""
from typing import Sequence
from marshmallow import EXCLUDE, fields

from .....messaging.agent_message import AgentMessage, AgentMessageSchema
from ..message_types import LIST_DATA_CERTIFICATE_TYPES_RESPONSE, PROTOCOL_PACKAGE
from .inner.list_data_certificate_types_result import ListDataCertificateTypesResultSchema, ListDataCertificateTypesResult

HANDLER_CLASS = f"{PROTOCOL_PACKAGE}.handlers.list_data_certificate_types_response_handler.ListDataCertificateTypesResponseHandler"


class ListDataCertificateTypesResponseMessage(AgentMessage):
    """Class defining the structure of list data certificate types response message."""

    class Meta:
        """List data certificate types response message metadata class."""

        handler_class = HANDLER_CLASS
        message_type = LIST_DATA_CERTIFICATE_TYPES_RESPONSE
        schema_class = "ListDataCertificateTypesResponseMessageSchema"

    def __init__(
        self,
        *,
        data_certificate_types: Sequence[ListDataCertificateTypesResult] = None,
        **kwargs
    ):
        """
        Initialize List data certificate types message object.
        """
        super().__init__(**kwargs)
        self.data_certificate_types = list(
            data_certificate_types) if data_certificate_types else []


class ListDataCertificateTypesResponseMessageSchema(AgentMessageSchema):
    """List data certificate types response message schema class."""

    class Meta:
        """List data certificate types response message schema metadata."""

        model_class = ListDataCertificateTypesResponseMessage
        unknown = EXCLUDE

    data_certificate_types = fields.List(
        fields.Nested(ListDataCertificateTypesResultSchema()),
        description="List of data certificate types available.",
    )
