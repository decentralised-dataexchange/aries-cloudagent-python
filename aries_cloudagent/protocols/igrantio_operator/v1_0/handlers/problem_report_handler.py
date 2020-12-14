"""Problem report handler."""

from .....messaging.base_handler import (
    BaseHandler,
    BaseResponder,
    RequestContext,
)

from ..messages.problem_report import ProblemReport


class ProblemReportHandler(BaseHandler):
    """Message handler class for problem report messages."""

    async def handle(self, context: RequestContext, responder: BaseResponder):
        """
        Message handler logic for problem report messages.

        Args:
            context: request context
            responder: responder callback
        """
        self._logger.debug(
            "ProblemReportHandler called with context %s", context)
        assert isinstance(context.message, ProblemReport)

        self._logger.info(
            "Received problem report message: %s",
            context.message.serialize(as_string=True)
        )
