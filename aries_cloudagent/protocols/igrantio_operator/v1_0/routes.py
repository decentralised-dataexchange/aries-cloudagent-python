"""iGrant.io operator handling admin routes."""


from aiohttp import web
from aiohttp_apispec import docs


@docs(
    tags=["igrantio_operator"],
    summary="Query iGrant.io operator configuration",
)
async def operator_configuration(request: web.BaseRequest):
    """
    Request handler for fetching iGrant.io operator configuration.

    Args:
        request: aiohttp request object

    Returns:
        iGrant.io operator configuration response

    """

    return web.json_response({"ApiKey": "qwerty123"})


async def register(app: web.Application):
    """Register routes."""

    app.add_routes(
        [
            web.get("/igrantio-operator/operator-configuration",
                    operator_configuration, allow_head=False),
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
