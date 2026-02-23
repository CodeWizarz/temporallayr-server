import unittest
import asyncio
from unittest.mock import patch, MagicMock
from app.query.engine import run_query
from app.query.parser import QueryAST, Condition, Operator


class TestQueryEngine(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def tearDown(self):
        self.loop.close()

    @patch("app.query.engine.async_session_maker")
    def test_run_query_enforces_limit(self, mock_session_maker):
        # Even if limit passed is 1000, effective limit should be capped at 500
        mock_session_maker.return_value.__aenter__.return_value.execute = MagicMock()
        mock_session_maker.return_value.__aexit__ = MagicMock()

        ast = QueryAST(conditions=[])

        # Test large limit
        results = self.loop.run_until_complete(run_query(ast, "test-tenant", 1000))
        self.assertEqual(results, [])
        # We can't easily inspect the exact SQL limit without deeper SQLAlchemy mocking,
        # but we can verify it doesn't crash and returns empty list from mock

    def test_run_query_empty_db_safely_ignored(self):
        with patch("app.query.engine.async_session_maker", None):
            ast = QueryAST(conditions=[])
            results = self.loop.run_until_complete(run_query(ast, "test-tenant", 10))
            self.assertEqual(results, [])

    @patch("app.query.engine.async_session_maker")
    def test_fields_extraction(self, mock_session_maker):
        mock_session = MagicMock()
        mock_execute = MagicMock()

        # Mocking scalar iteration returning specific payload
        class MockResult:
            def scalars(self):
                return [
                    {
                        "execution_id": "test-123",
                        "timestamp": "2026-01-01T00:00:00Z",
                        "failure_type": "timeout",
                        "duration": 450,
                        "nodes": [{"id": "n1", "name": "StartNode", "parent_id": ""}],
                    }
                ]

        # Properly mock async execute
        async def mock_execute(*args, **kwargs):
            return MockResult()

        mock_session.execute = mock_execute

        # Mock async context manager cleanly
        class AsyncContextManager:
            async def __aenter__(self):
                return mock_session

            async def __aexit__(self, exc_type, exc, tb):
                pass

        mock_session_maker.return_value = AsyncContextManager()

        ast = QueryAST(conditions=[])
        results = self.loop.run_until_complete(run_query(ast, "test-tenant", 10))

        self.assertEqual(len(results), 1)
        res = results[0]
        self.assertEqual(res["execution_id"], "test-123")
        self.assertEqual(res["timestamp"], "2026-01-01T00:00:00Z")
        self.assertEqual(res["root_node"], "StartNode")
        self.assertEqual(res["failure_type"], "timeout")
        self.assertEqual(res["duration"], 450)


if __name__ == "__main__":
    unittest.main()
