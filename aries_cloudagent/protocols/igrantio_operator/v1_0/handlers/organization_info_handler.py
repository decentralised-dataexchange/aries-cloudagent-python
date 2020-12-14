"""Organization info handler."""


from .....messaging.base_handler import (
    BaseHandler,
    BaseResponder,
    RequestContext,
)
from ..messages.problem_report import ProblemReport, ProblemReportReason
from ..messages.organization_info import OrganizationInfoMessage
from ..manager import IGrantIOOperatorManager


class OrganizationInfoHandler(BaseHandler):
    """Message handler class for organization info messages."""

    async def handle(self, context: RequestContext, responder: BaseResponder):
        """
        Message handler logic for organization info messages.

        Args:
            context: request context
            responder: responder callback
        """
        self._logger.debug(
            "OrganizationInfoHandler called with context %s", context)
        assert isinstance(context.message, OrganizationInfoMessage)

        self._logger.info(
            "Received list data certificate types message: %s",
            context.message.serialize(as_string=True)
        )

        igrantio_operator_mgr = IGrantIOOperatorManager(context=context)
        reply = await igrantio_operator_mgr.get_organization_info_message()

        if reply:
            await responder.send_reply(reply)
        else:
            problem_report = ProblemReport(
                problem_code=ProblemReportReason.OPERATOR_NOT_CONFIGURED.value,
                explain="Operator is not configured or invalid configuration provided."
            )
            await responder.send_reply(problem_report)
