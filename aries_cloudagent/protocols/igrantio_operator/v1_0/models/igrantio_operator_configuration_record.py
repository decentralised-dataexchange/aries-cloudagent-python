"""An object for containing iGrant.io operator configuration."""

from marshmallow import EXCLUDE, fields

from .....messaging.models.base_record import BaseRecord, BaseRecordSchema


class IGrantIOOperatorConfigurationRecord(BaseRecord):
    """Class representing stored iGrant.io operator configuration."""

    RECORD_TYPE = "igrantio_operator_configuration"

    class Meta:
        """IGrantIOOperatorConfigurationRecord metadata."""

        schema_class = "IGrantIOOperatorConfigurationRecordSchema"

    def __init__(
        self,
        *,
        api_key: str = None,
        org_id: str = None,
        operator_endpoint: str = None,
        **kwargs
    ):
        """
        Initialize a IGrantIOOperatorConfigurationRecord instance.

        Args:
            api_key: iGrant.io operator API key

        """
        super().__init__(**kwargs)
        self.api_key = api_key
        self.org_id = org_id
        self.operator_endpoint = operator_endpoint

    @property
    def record_value(self) -> dict:
        """Accessor to for the JSON record value properties for this connection."""
        return {
            prop: getattr(self, prop)
            for prop in (
                "api_key",
                "org_id",
                "operator_endpoint"
            )
        }

    @property
    def operator_configuration_record_id(self) -> str:
        """Accessor for the ID associated with this operator configuration."""
        return self._id


class IGrantIOOperatorConfigurationRecordSchema(BaseRecordSchema):
    """IGrantIOOperatorConfigurationRecord schema."""

    class Meta:
        """IGrantIOOperatorConfigurationRecordSchema metadata."""

        model_class = IGrantIOOperatorConfigurationRecord
        unknown = EXCLUDE

    api_key = fields.Str(required=False)
    org_id = fields.Str(required=False)
    operator_endpoint = fields.Str(required=False)
