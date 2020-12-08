"""iGrant.io operator handling admin routes."""

import json
import aiohttp
from marshmallow import fields
from aiohttp import web
from aiohttp_apispec import (
    docs,
    match_info_schema,
    querystring_schema,
    response_schema,
)

from .valid import IGrantIOAPIKeyJWS
from .manager import IGrantIOOperatorManager
from ....messaging.models.openapi import OpenAPISchema
from ....messaging.valid import UUIDFour
from ....storage.error import StorageNotFoundError
from ....connections.models.connection_record import ConnectionRecord


class ConnIdMatchInfoSchema(OpenAPISchema):
    """Path parameters and validators for request taking connection id."""

    conn_id = fields.Str(
        description="Connection identifier", required=True, example=UUIDFour.EXAMPLE
    )


class OperatorConfigurationQueryStringSchema(OpenAPISchema):
    """Parameters and validators for operator configuration query string."""

    operator_endpoint = fields.Str(
        description="Operator endpoint",
        required=True,
        example="https://api.igrant.io"
    )

    api_key = fields.Str(
        description="iGrant.io operator API key",
        required=True,
        example=IGrantIOAPIKeyJWS.EXAMPLE
    )

    org_id = fields.Str(
        description="Organization ID",
        required=True,
        example=UUIDFour.EXAMPLE
    )


class OperatorConfigurationResultSchema(OpenAPISchema):
    """Result schema for operator configuration."""

    api_key = fields.Str(
        description="iGrant.io operator API key", example=IGrantIOAPIKeyJWS.EXAMPLE
    )

    org_id = fields.Str(
        description="Organization ID",
        required=True,
        example=UUIDFour.EXAMPLE
    )


@docs(
    tags=["igrantio_operator"],
    summary="Fetch iGrant.io operator configuration",
)
@response_schema(OperatorConfigurationResultSchema(), 200)
async def igrantio_operator_configuration(request: web.BaseRequest):
    """
    Request handler for fetching iGrant.io operator configuration.

    Args:
        request: aiohttp request object

    Returns:
        iGrant.io operator configuration response

    """

    context = request.app["request_context"]

    igrantio_operator_mgr = IGrantIOOperatorManager(context=context)
    operator_configuration = await igrantio_operator_mgr.fetch_operator_configuration()

    result = {"api_key": "", "org_id": "", "operator_endpoint": ""}
    if operator_configuration:
        result = {
            "api_key": operator_configuration.api_key,
            "org_id": operator_configuration.org_id,
            "operator_endpoint": operator_configuration.operator_endpoint
        }

    return web.json_response(result)


@docs(
    tags=["igrantio_operator"],
    summary="Configure iGrant.io operator",
)
@querystring_schema(OperatorConfigurationQueryStringSchema())
@response_schema(OperatorConfigurationResultSchema(), 200)
async def configure_igrantio_operator(request: web.BaseRequest):
    """
    Request handler for configuring igrantio operator

    Args:
        request: aiohttp request object

    Returns:
        The igrantio operator configuration details

    """
    context = request.app["request_context"]

    api_key = request.query.get("api_key")
    org_id = request.query.get("org_id")
    operator_endpoint = request.query.get("operator_endpoint")

    igrantio_operator_mgr = IGrantIOOperatorManager(context=context)
    operator_configuration = await igrantio_operator_mgr.update_operator_configuration(
        api_key=api_key,
        org_id=org_id,
        operator_endpoint=operator_endpoint
    )

    result = {
        "api_key": operator_configuration.api_key,
        "org_id": operator_configuration.org_id,
        "operator_endpoint": operator_configuration.operator_endpoint
    }

    return web.json_response(result)


@docs(
    tags=["igrantio_operator"],
    summary="Fetch organization details from the operator",
)
async def fetch_organization_details(request: web.BaseRequest):
    """
    Request handler to fetch organization details

    Args:
        request: aiohttp request object

    Returns:
        Organization details

    """
    context = request.app["request_context"]

    igrantio_operator_mgr = IGrantIOOperatorManager(context=context)
    operator_configuration = await igrantio_operator_mgr.fetch_operator_configuration()

    result = {}
    if operator_configuration:

        org_info_route = "/v1/organizations/{org_id}".format(
            org_id=operator_configuration.org_id)

        headers = {'Authorization': 'ApiKey {}'.format(operator_configuration.api_key)}

        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(operator_configuration.operator_endpoint + org_info_route) as resp:
                if resp.status == 200:
                    resp_json = await resp.json()

                    exclude_keys = [
                        "BillingInfo",
                        "Admins",
                        "HlcSupport",
                        "DataRetention",
                        "Enabled",
                        "ID",
                        "Subs"
                    ]

                    for exclude_key in exclude_keys:
                        resp_json["Organization"].pop(exclude_key, None)

                    print(json.dumps(resp_json, indent=4))

    return web.json_response(result)


@docs(
    tags=["igrantio_operator"],
    summary="Sending message for listing available data certificate types that can be issued"
)
@match_info_schema(ConnIdMatchInfoSchema())
async def list_available_data_certificates_types(request: web.BaseRequest):
    """
    Request handle for listing available data certificate types that can be issued

    Args:
        request: aiohttp request object
    """
    context = request.app["request_context"]
    connection_id = request.match_info["conn_id"]

    try:
        record: ConnectionRecord = await ConnectionRecord.retrieve_by_id(context, connection_id)
        igrantio_operator_mgr = IGrantIOOperatorManager(context=context)
        await igrantio_operator_mgr.list_data_certificate_types_request(connection_id=record.connection_id)
    except StorageNotFoundError as err:
        raise web.HTTPNotFound(reason=err.roll_up) from err

    return web.json_response({})


@docs(
    tags=["igrantio_operator"],
    summary="Sending message for fetching organization info"
)
@match_info_schema(ConnIdMatchInfoSchema())
async def send_organization_info_request(request: web.BaseRequest):
    """
    Request handle for fetching organization info

    Args:
        request: aiohttp request object
    """
    context = request.app["request_context"]
    connection_id = request.match_info["conn_id"]

    try:
        record: ConnectionRecord = await ConnectionRecord.retrieve_by_id(context, connection_id)
        igrantio_operator_mgr = IGrantIOOperatorManager(context=context)
        await igrantio_operator_mgr.send_organization_info_request(connection_id=record.connection_id)
    except StorageNotFoundError as err:
        raise web.HTTPNotFound(reason=err.roll_up) from err

    return web.json_response({})


async def register(app: web.Application):
    """Register routes."""

    app.add_routes(
        [
            web.get("/igrantio-operator/operator-configuration",
                    igrantio_operator_configuration, allow_head=False),
            web.post("/igrantio-operator/operator-configuration",
                     configure_igrantio_operator),
            web.get("/igrantio-operator/organization-info",
                    fetch_organization_details, allow_head=False),
            web.post("/igrantio-operator/connections/{conn_id}/list-data-certificate-types",
                     list_available_data_certificates_types),
            web.post("/igrantio-operator/connections/{conn_id}/organization-info",
                     send_organization_info_request),
        ]
    )


def post_process_routes(app: web.Application):
    """Amend swagger API."""

    # Add top-level tags description
    if "tags" not in app._state["swagger_dict"]:
        app._state["swagger_dict"]["tags"] = []
    app._state["swagger_dict"]["tags"].append(
        {
            "name": "igrantio_operator",
            "description": "iGrant.io operator management",
        }
    )
