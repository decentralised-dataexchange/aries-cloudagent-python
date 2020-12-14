"""Message type identifiers for Routing."""

from ...didcomm_prefix import DIDCommPrefix


# Message types
LIST_DATA_CERTIFICATE_TYPES = "igrantio-operator/1.0/list-data-certificate-types"
LIST_DATA_CERTIFICATE_TYPES_RESPONSE = "igrantio-operator/1.0/list-data-certificate-types-response"

PROBLEM_REPORT = "igrantio-operator/1.0/problem_report"

ORGANIZATION_INFO = "igrantio-operator/1.0/organization-info"
ORGANIZATION_INFO_RESPONSE = "igrantio-operator/1.0/organization-info-response"

PROTOCOL_PACKAGE = "aries_cloudagent.protocols.igrantio_operator.v1_0"

MESSAGE_TYPES = DIDCommPrefix.qualify_all(
    {
        LIST_DATA_CERTIFICATE_TYPES: (
            f"{PROTOCOL_PACKAGE}.messages.list_data_certificate_types.ListDataCertificateTypesMessage"
        ),
        LIST_DATA_CERTIFICATE_TYPES_RESPONSE: (
            f"{PROTOCOL_PACKAGE}.messages.list_data_certificate_types_response.ListDataCertificateTypesResponseMessage"
        ),
        ORGANIZATION_INFO: (
            f"{PROTOCOL_PACKAGE}.messages.organization_info.OrganizationInfoMessage"
        ),
        ORGANIZATION_INFO_RESPONSE: (
            f"{PROTOCOL_PACKAGE}.messages.organization_info_response.OrganizationInfoResponseMessage"
        ),
        PROBLEM_REPORT: (
            f"{PROTOCOL_PACKAGE}.messages.problem_report.ProblemReport"
        ),
    }
)
