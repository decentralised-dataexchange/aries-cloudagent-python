"""Organization info message."""

from marshmallow import EXCLUDE

from .....messaging.agent_message import AgentMessage, AgentMessageSchema

from ..message_types import ORGANIZATION_INFO, PROTOCOL_PACKAGE

HANDLER_CLASS = f"{PROTOCOL_PACKAGE}.handlers.organization_info_handler.OrganizationInfoHandler"


class OrganizationInfoMessage(AgentMessage):
    """Class defining the structure of organization info message."""

    class Meta:
        """Organization info message metadata class."""

        handler_class = HANDLER_CLASS
        message_type = ORGANIZATION_INFO
        schema_class = "OrganizationInfoMessageSchema"

    def __init__(self, **kwargs):
        """
        Initialize organization info object.
        """
        super().__init__(**kwargs)


class OrganizationInfoMessageSchema(AgentMessageSchema):
    """List organization info message schema class."""

    class Meta:
        """List organization info message schema metadata."""

        model_class = OrganizationInfoMessage
        unknown = EXCLUDE
