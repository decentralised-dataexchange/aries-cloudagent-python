"""Organization info response message."""
from typing import Sequence
from marshmallow import EXCLUDE, fields

from .....messaging.agent_message import AgentMessage, AgentMessageSchema
from ..message_types import ORGANIZATION_INFO_RESPONSE, PROTOCOL_PACKAGE
from .inner.organization_privacy_dashboard import OrganizationPrivacyDashboard, OrganizationPrivacyDashboardSchema

HANDLER_CLASS = f"{PROTOCOL_PACKAGE}.handlers.organization_info_response_handler.OrganizationInfoResponseHandler"


class OrganizationInfoResponseMessage(AgentMessage):
    """Class defining the structure of organization info response message."""

    class Meta:
        """List organization info response message metadata class."""

        handler_class = HANDLER_CLASS
        message_type = ORGANIZATION_INFO_RESPONSE
        schema_class = "OrganizationInfoResponseMessageSchema"

    def __init__(
        self,
        *,
        org_id:str = None,
        name: str = None,
        cover_image_url: str = None,
        logo_image_url: str = None,
        location: str = None,
        org_type: str = None,
        description: str = None,
        policy_url: str = None,
        eula_url: str = None,
        privacy_dashboard: OrganizationPrivacyDashboard = None,
        **kwargs
    ):
        """
        Initialize organization info response message object.
        """
        super().__init__(**kwargs)
        self.org_id = org_id
        self.name = name
        self.cover_image_url = cover_image_url
        self.logo_image_url = logo_image_url
        self.location = location
        self.org_type = org_type
        self.description = description
        self.policy_url = policy_url
        self.eula_url = eula_url
        self.privacy_dashboard = privacy_dashboard


class OrganizationInfoResponseMessageSchema(AgentMessageSchema):
    """List organization info response message schema class."""

    class Meta:
        """List organization info response message schema metadata."""

        model_class = OrganizationInfoResponseMessage
        unknown = EXCLUDE
    
    org_id = fields.Str()
    name = fields.Str()
    cover_image_url = fields.Str()
    logo_image_url = fields.Str()
    location = fields.Str()
    org_type = fields.Str()
    description = fields.Str()
    policy_url = fields.Str()
    eula_url = fields.Str()
    privacy_dashboard = fields.Nested(OrganizationPrivacyDashboardSchema())
