"""Organization purpose inner object."""

from typing import Sequence
from marshmallow import fields

from ......messaging.models.base import BaseModel, BaseModelSchema


class OrganizationPurpose(BaseModel):
    """Class representing organization purpose"""

    class Meta:
        """Organization purpose metadata."""

        schema_class = "OrganizationPurposeSchema"

    def __init__(
        self,
        purpose_id: str = None,
        purpose_name: str = None,
        purpose_description: str = None,
        lawful_usage: bool = False,
        policy_url: str = None,
        **kwargs
    ):
        """
        Initialize organization purpose object.

        Args:
            purpose_id: "purpose ID"
            purpose_name: "purpose name"
            purpose_description: "purpose description"
            lawful_usage: "lawful usage"
            policy_url: "policy URL"
        """
        super().__init__(**kwargs)
        self.purpose_id = purpose_id
        self.purpose_name = purpose_name
        self.purpose_description = purpose_description
        self.lawful_usage = lawful_usage
        self.policy_url = policy_url


class OrganizationPurposeSchema(BaseModelSchema):
    """Organization purpose schema."""

    class Meta:
        """Organization purpose schema metadata."""

        model_class = OrganizationPurpose

    purpose_id = fields.Str()
    purpose_name = fields.Str()
    purpose_description = fields.Str()
    lawful_usage = fields.Boolean()
    policy_url = fields.Str()
