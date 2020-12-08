"""A list data certificate types inner object."""

from marshmallow import fields

from ......messaging.models.base import BaseModel, BaseModelSchema


class ListDataCertificateTypesResult(BaseModel):
    """Class representing a list data certificate types result."""

    class Meta:
        """list data certificate types result metadata."""

        schema_class = "ListDataCertificateTypesResultSchema"

    def __init__(
        self,
        schema_version: str,
        schema_name: str,
        epoch: str,
        schema_id: str,
        cred_def_id: str,
        schema_issuer_did: str,
        issuer_did: str,
        **kwargs
    ):
        """
        Initialize list data certificate types result object.
        Args:
            schema_version: "Version of schema"
            schema_name: "Name of schema"
            epoch: "Epoch time for credential definition creation"
            schema_id: "Schema ID"
            cred_def_id: "Credential definition ID"
            schema_issuer_did: "Schema issuer DID"
            issuer_did: "Issuer DID"
        """
        super().__init__(**kwargs)
        self.schema_version = schema_version
        self.schema_name = schema_name
        self.epoch = epoch
        self.schema_id = schema_id
        self.cred_def_id = cred_def_id
        self.schema_issuer_did = schema_issuer_did
        self.issuer_did = issuer_did


class ListDataCertificateTypesResultSchema(BaseModelSchema):
    """List data certificate types result specification schema."""

    class Meta:
        """List data certificate types result schema metadata."""

        model_class = ListDataCertificateTypesResult

    schema_version = fields.Str()
    schema_name = fields.Str()
    epoch = fields.Str()
    schema_id = fields.Str()
    cred_def_id = fields.Str()
    schema_issuer_did = fields.Str()
    issuer_did = fields.Str()
