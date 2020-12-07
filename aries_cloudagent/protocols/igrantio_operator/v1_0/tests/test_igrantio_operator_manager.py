from asynctest import TestCase as AsyncTestCase

from .....messaging.request_context import RequestContext

from ..manager import IGrantIOOperatorManager, IGrantIOOperatorManagerError

class TestIGrantIOOperatorManager(AsyncTestCase):
    async def setUp(self):
        self.context = RequestContext.test_context()
        self.session = await self.context.session()
        self.manager = IGrantIOOperatorManager(self.session)
        assert self.manager.session

    async def test_create_manager_no_context(self):
        with self.assertRaises(IGrantIOOperatorManagerError):
            IGrantIOOperatorManager(None)
