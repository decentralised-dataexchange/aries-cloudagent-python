"""iGrant.io operator manager classes for tracking and inspecting operator records."""
import logging
import aiohttp

from .messages.inner.list_data_certificate_types_result import ListDataCertificateTypesResult
from .messages.list_data_certificate_types_response import ListDataCertificateTypesResponseMessage
from .messages.list_data_certificate_types import ListDataCertificateTypesMessage
from .messages.organization_info import OrganizationInfoMessage
from .messages.organization_info_response import OrganizationInfoResponseMessage
from .messages.inner.organization_privacy_dashboard import OrganizationPrivacyDashboard
from .models.igrantio_operator_configuration_record import IGrantIOOperatorConfigurationRecord
from ....storage.base import BaseStorage
from ....storage.error import StorageNotFoundError
from ....core.error import BaseError
from ....config.injection_context import InjectionContext
from ....messaging.responder import BaseResponder
from ....messaging.credential_definitions.util import CRED_DEF_SENT_RECORD_TYPE


class IGrantIOOperatorManagerError(BaseError):
    """Generic iGrant.io operator manager error."""


class IGrantIOOperatorManager:
    """Class for handling iGrant.io operator messages"""

    OPERATOR_CONFIG_RECORD_TYPE = "igrantio_operator_configuration"

    def __init__(self,  context: InjectionContext):
        """
        Initialize a IGrantIOOperatorManager.

        Args:
            context: The context for this manager
        """
        self._context = context
        self._logger = logging.getLogger(__name__)

    @property
    def context(self) -> InjectionContext:
        """
        Accessor for the current injection context.

        Returns:
            The injection context for this iGrant.io operator manager

        """
        return self._context

    async def fetch_operator_configuration(self) -> IGrantIOOperatorConfigurationRecord:
        """
        Fetches operator configuration record

        Returns:
            An `IGrantIOOperatorConfigurationRecord` instance

        """
        igrantio_operator_configurations = None
        igrantio_operator_configuration = None

        try:
            igrantio_operator_configurations = await IGrantIOOperatorConfigurationRecord.query(
                self.context, {}, {}, {})

        except StorageNotFoundError:
            pass

        if not igrantio_operator_configurations:
            return None

        igrantio_operator_configuration = igrantio_operator_configurations[0]
        return igrantio_operator_configuration

    async def update_operator_configuration(self, api_key: str = None, org_id: str = None, operator_endpoint: str = None) -> IGrantIOOperatorConfigurationRecord:
        """
        Updates operator configuration record

        Organization admin configures iGrant.io operator for integrating MyData operator
        functionalities within the cloud agent.

        Args:
            api_key: iGrant.io operator API key

        Returns:
            An `IGrantIOOperatorConfigurationRecord` instance

        """
        igrantio_operator_configurations = None
        igrantio_operator_configuration = None

        if not api_key:
            raise IGrantIOOperatorManagerError("API key not provided")

        if not org_id:
            raise IGrantIOOperatorManagerError("Organization ID not provided")

        if not operator_endpoint:
            raise IGrantIOOperatorManagerError("Operator endpoint not provided")

        try:
            igrantio_operator_configurations = await IGrantIOOperatorConfigurationRecord.query(
                self.context, {}, {}, {})

        except StorageNotFoundError:
            pass

        if not igrantio_operator_configurations:
            igrantio_operator_configuration = IGrantIOOperatorConfigurationRecord(
                api_key=api_key, org_id=org_id, operator_endpoint=operator_endpoint
            )
        else:
            igrantio_operator_configuration = igrantio_operator_configurations[0]
            igrantio_operator_configuration.api_key = api_key
            igrantio_operator_configuration.org_id = org_id
            igrantio_operator_configuration.operator_endpoint = operator_endpoint

        await igrantio_operator_configuration.save(self.context, reason="Updated iGrant.io operator configuration")

        return igrantio_operator_configuration

    async def list_data_certificate_types_request(self, connection_id: str):

        request = ListDataCertificateTypesMessage()

        responder: BaseResponder = await self._context.inject(
            BaseResponder, required=False
        )
        if responder:
            await responder.send(request, connection_id=connection_id)

    async def get_list_data_certificate_types_response_message(self):
        storage = await self.context.inject(BaseStorage)
        found = await storage.search_records(
            type_filter=CRED_DEF_SENT_RECORD_TYPE,
            tag_query={}
        ).fetch_all()

        data_certificate_types = []

        if found:
            for record in found:
                temp_data_certificate_type = ListDataCertificateTypesResult(
                    schema_version=record.tags.get("schema_version"),
                    schema_name=record.tags.get("schema_name"),
                    epoch=record.tags.get("epoch"),
                    schema_id=record.tags.get("schema_id"),
                    cred_def_id=record.tags.get("cred_def_id"),
                    schema_issuer_did=record.tags.get("schema_issuer_did"),
                    issuer_did=record.tags.get("issuer_did")
                )
                data_certificate_types.append(temp_data_certificate_type)

        response = ListDataCertificateTypesResponseMessage(
            data_certificate_types=data_certificate_types)

        return response

    async def send_organization_info_request(self, connection_id: str):

        request = OrganizationInfoMessage()

        responder: BaseResponder = await self._context.inject(
            BaseResponder, required=False
        )
        if responder:
            await responder.send(request, connection_id=connection_id)

    async def get_organization_info_message(self):

        igrantio_operator_mgr = IGrantIOOperatorManager(context=self.context)
        operator_configuration = await igrantio_operator_mgr.fetch_operator_configuration()

        if operator_configuration:

            org_info_route = "/v1/organizations/{org_id}".format(
                org_id=operator_configuration.org_id)

            headers = {'Authorization': 'ApiKey {}'.format(
                operator_configuration.api_key)}

            async with aiohttp.ClientSession(headers=headers) as session:
                async with session.get(operator_configuration.operator_endpoint + org_info_route) as resp:
                    if resp.status == 200:
                        resp_json = await resp.json()

                        if "Organization" in resp_json:
                            organization_details = resp_json["Organization"]

                            exclude_keys = [
                                "BillingInfo",
                                "Admins",
                                "HlcSupport",
                                "DataRetention",
                                "Enabled",
                                "Subs"
                            ]

                            for exclude_key in exclude_keys:
                                organization_details.pop(exclude_key, None)

                            response = OrganizationInfoResponseMessage(
                                org_id=organization_details["ID"],
                                name=organization_details["Name"],
                                cover_image_url=organization_details["CoverImageURL"] + "/web",
                                logo_image_url=organization_details["LogoImageURL"] + "/web",
                                location=organization_details["Location"],
                                org_type=organization_details["Type"]["Type"],
                                description=organization_details["Description"],
                                policy_url=organization_details["PolicyURL"],
                                eula_url=organization_details["EulaURL"],
                                privacy_dashboard=OrganizationPrivacyDashboard(
                                    host_name=organization_details["PrivacyDashboard"]["HostName"],
                                    version=organization_details["PrivacyDashboard"]["Version"],
                                    status=organization_details["PrivacyDashboard"]["Status"],
                                    delete=organization_details["PrivacyDashboard"]["Delete"]
                                )
                            )

                            return response
                    else:
                        return None
        return None
