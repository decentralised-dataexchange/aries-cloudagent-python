"""Organization privacy dashboard inner object."""

from typing import Sequence
from marshmallow import fields

from ......messaging.models.base import BaseModel, BaseModelSchema


class OrganizationPrivacyDashboard(BaseModel):
    """Class representing organization privacy dashboard"""

    class Meta:
        """Organization privacy dashboard metadata."""

        schema_class = "OrganizationPrivacyDashboardSchema"

    def __init__(
        self,
        host_name: str = None,
        version: str = None,
        status: int = None,
        delete: bool = False,
        **kwargs
    ):
        """
        Initialize organization purpose object.

        Args:
            host_name: "Privacy dashboard host name"
            version: "Privacy dashboard version"
            status: "Privacy dashboard deployment status"
            delete: "Privacy dashboard deployment delete status"
        """
        super().__init__(**kwargs)
        self.host_name = host_name
        self.version = version
        self.status = status
        self.delete = delete


class OrganizationPrivacyDashboardSchema(BaseModelSchema):
    """Organization privacy dashboard schema."""

    class Meta:
        """Organization privacy dashboard schema metadata."""

        model_class = OrganizationPrivacyDashboard

    host_name = fields.Str()
    version = fields.Str()
    status = fields.Integer()
    delete = fields.Boolean()
