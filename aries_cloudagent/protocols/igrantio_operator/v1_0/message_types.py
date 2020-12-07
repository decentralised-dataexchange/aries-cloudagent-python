"""Message type identifiers for Routing."""

from ...didcomm_prefix import DIDCommPrefix


# Message types
ORGANIZATION_INFO = "igrantio-operator/1.0/organization-info"

PROTOCOL_PACKAGE = "aries_cloudagent.protocols.igrantio_operator.v1_0"

MESSAGE_TYPES = DIDCommPrefix.qualify_all(
    {
        ORGANIZATION_INFO: f"{PROTOCOL_PACKAGE}.messages.organization_info.OrganizationInfo",
    }
)
