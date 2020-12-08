"""List data certificate types response handler."""

from .....messaging.base_handler import (
    BaseHandler,
    BaseResponder,
    RequestContext,
)

from ..messages.list_data_certificate_types_response import ListDataCertificateTypesResponseMessage

class ListDataCertificateTypesResponseHandler(BaseHandler):
    """Message handler class for list data certificate types response messages."""

    async def handle(self, context: RequestContext, responder: BaseResponder):
        """
        Message handler logic for list data certificate types response messages.

        Args:
            context: request context
            responder: responder callback
        """
        self._logger.debug(
            "ListDataCertificateTypesResponseHandler called with context %s", context)
        assert isinstance(context.message, ListDataCertificateTypesResponseMessage)

        self._logger.info(
            "Received list data certificate types response message: %s",
            context.message.serialize(as_string=True)
        )
