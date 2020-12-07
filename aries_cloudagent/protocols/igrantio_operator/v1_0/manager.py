"""iGrant.io operator manager classes for tracking and inspecting operator records."""
import logging

from ....core.error import BaseError
from ....core.profile import ProfileSession


class IGrantIOOperatorManagerError(BaseError):
    """Generic iGrant.io operator manager error."""


class IGrantIOOperatorManager:
    """Class for handling iGrant.io operator messages"""

    ORGANIZATION_INFO_RECORD_TYPE = "igrantio_operator_organization_info"

    def __init__(self, session: ProfileSession):
        """
        Initialize a IGrantIOOperatorManager.

        Args:
            context: The context for this manager
        """
        self._session = session
        self._logger = logging.getLogger(__name__)
        if not session:
            raise IGrantIOOperatorManagerError("Missing profile session")

    @property
    def session(self) -> ProfileSession:
        """
        Accessor for the current profile session.

        Returns:
            The profile session for this connection

        """
        return self._session
