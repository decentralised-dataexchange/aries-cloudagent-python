"""Organization info response handler."""

from .....messaging.base_handler import (
    BaseHandler,
    BaseResponder,
    RequestContext,
)

from ..messages.organization_info_response import OrganizationInfoResponseMessage

class OrganizationInfoResponseHandler(BaseHandler):
    """Message handler class for organization info response messages."""

    async def handle(self, context: RequestContext, responder: BaseResponder):
        """
        Message handler logic for organization info response messages.

        Args:
            context: request context
            responder: responder callback
        """
        self._logger.debug(
            "OrganizationInfoResponseHandler called with context %s", context)
        assert isinstance(context.message, OrganizationInfoResponseMessage)

        self._logger.info(
            "Received organization info response message: %s",
            context.message.serialize(as_string=True)
        )
