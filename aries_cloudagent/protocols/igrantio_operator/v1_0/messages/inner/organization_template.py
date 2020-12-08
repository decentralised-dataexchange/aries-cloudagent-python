"""Organization template inner object."""

from typing import Sequence
from marshmallow import fields

from ......messaging.models.base import BaseModel, BaseModelSchema


class OrganizationTemplate(BaseModel):
    """Class representing organization template"""

    class Meta:
        """Organization template metadata."""

        schema_class = "OrganizationTemplateSchema"

    def __init__(
        self,
        template_id: str = None,
        template_name: str = None,
        template_purpose_ids: Sequence[str] = None,
        **kwargs
    ):
        """
        Initialize organization template object.

        Args:
            template_id: "Template ID"
            template_name: "Template name"
            template_purpose_ids: "Template purpose IDs"
        """
        super().__init__(**kwargs)
        self.template_id = template_id
        self.template_name = template_name
        self.template_purpose_ids = list(
            template_purpose_ids) if template_purpose_ids else []


class OrganizationTemplateSchema(BaseModelSchema):
    """Organization template schema."""

    class Meta:
        """Organization template schema metadata."""

        model_class = OrganizationTemplate

    template_id = fields.Str()
    template_name = fields.Str()
    template_purpose_ids = fields.List(fields.Str())
