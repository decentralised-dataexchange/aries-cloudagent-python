"""Organization type inner object."""

from marshmallow import fields

from ......messaging.models.base import BaseModel, BaseModelSchema


class OrganizationType(BaseModel):
    """Class representing organization type"""

    class Meta:
        """Organization type metadata."""

        schema_class = "OrganizationTypeSchema"

    def __init__(
        self,
        org_type: str,
        **kwargs
    ):
        """
        Initialize organization type object.

        Args:
            org_type: "Organization type for e.g. Travel, Health care e.t.c"
        """
        super().__init__(**kwargs)
        self.org_type = org_type


class OrganizationTypeSchema(BaseModelSchema):
    """Organization type schema."""

    class Meta:
        """Organization type schema metadata."""

        model_class = OrganizationType

    org_type = fields.Str()