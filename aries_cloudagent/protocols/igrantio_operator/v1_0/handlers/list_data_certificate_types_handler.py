"""List data certificate types handler."""

from .....messaging.base_handler import (
    BaseHandler,
    BaseResponder,
    RequestContext,
)

from ..messages.list_data_certificate_types import ListDataCertificateTypesMessage
from ..manager import IGrantIOOperatorManager


class ListDataCertificateTypesHandler(BaseHandler):
    """Message handler class for list data certificate types messages."""

    async def handle(self, context: RequestContext, responder: BaseResponder):
        """
        Message handler logic for list data certificate types messages.

        Args:
            context: request context
            responder: responder callback
        """
        self._logger.debug(
            "ListDataCertificateTypesHandler called with context %s", context)
        assert isinstance(context.message, ListDataCertificateTypesMessage)

        self._logger.info(
            "Received list data certificate types message: %s",
            context.message.serialize(as_string=True)
        )

        igrantio_operator_mgr = IGrantIOOperatorManager(context=context)
        await igrantio_operator_mgr.list_data_certificate_types_response()
