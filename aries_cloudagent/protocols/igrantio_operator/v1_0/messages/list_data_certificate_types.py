"""List data certificate types message."""

from marshmallow import EXCLUDE

from .....messaging.agent_message import AgentMessage, AgentMessageSchema

from ..message_types import LIST_DATA_CERTIFICATE_TYPES, PROTOCOL_PACKAGE

HANDLER_CLASS = f"{PROTOCOL_PACKAGE}.handlers.list_data_certificate_types_handler.ListDataCertificateTypesHandler"


class ListDataCertificateTypesMessage(AgentMessage):
    """Class defining the structure of list data certificate types message."""

    class Meta:
        """List data certificate types message metadata class."""

        handler_class = HANDLER_CLASS
        message_type = LIST_DATA_CERTIFICATE_TYPES
        schema_class = "ListDataCertificateTypesMessageSchema"

    def __init__(self, **kwargs):
        """
        Initialize List data certificate types message object.
        """
        super().__init__(**kwargs)


class ListDataCertificateTypesMessageSchema(AgentMessageSchema):
    """List data certificate types message schema class."""

    class Meta:
        """List data certificate types message schema metadata."""

        model_class = ListDataCertificateTypesMessage
        unknown = EXCLUDE
